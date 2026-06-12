package meta

import (
	"errors"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// fakePersister is the test stand-in for the BadgerDB adapter: a map of
// persisted rows, with an injectable commit failure.
type fakePersister struct {
	rows    map[string][]byte
	commits int
	fail    error
}

func newFakePersister() *fakePersister {
	return &fakePersister{rows: make(map[string][]byte)}
}

func (f *fakePersister) Commit(rows []Row) error {
	if f.fail != nil {
		return f.fail
	}
	f.commits++
	for _, r := range rows {
		if r.Value == nil {
			delete(f.rows, r.Key)
		} else {
			f.rows[r.Key] = r.Value
		}
	}
	return nil
}

// dumpRows renders a store's entire keyspace in encoded form — the
// canonical comparison between a live store and one rebuilt from disk.
func dumpRows(s *Store) map[string]string {
	out := make(map[string]string)
	s.kv.scan("", func(k string, v any) bool {
		out[k] = string(marshalRecord(v))
		return true
	})
	return out
}

// restoreFrom rebuilds a store from a persister's rows.
func restoreFrom(t *testing.T, f *fakePersister) *Store {
	t.Helper()
	s := NewStore()
	for k, v := range f.rows {
		if err := s.Restore(k, v); err != nil {
			t.Fatalf("restore: %v", err)
		}
	}
	return s
}

func TestPersistRestartEquivalence(t *testing.T) {
	f := newFakePersister()
	s := NewStore()
	s.SetPersister(f)

	rng := rand.New(rand.NewPCG(5, 0))
	at := int64(1_750_000_000_000)
	mint := func() VersionID { return mintAt(at, rng) }

	// A representative slice of state: bucket, plain object with metadata,
	// an overwritten object, a multipart in flight, a completed multipart.
	mustOK(t, s.ApplyCreateBucket(CreateBucket{ProposedAtUnixMS: at, Bucket: "bkt"}))
	_, err := s.ApplyPutObject(PutObject{ProposedAtUnixMS: at, Bucket: "bkt", Key: "plain",
		VersionID: mint(), Size: 9, ETag: []byte{1}, ContentType: "text/plain",
		UserMetadata: map[string]string{"x-amz-meta-a": "1"}, ObjectChecksum: []byte{2}})
	mustOK(t, err)
	at++
	_, err = s.ApplyPutObject(PutObject{ProposedAtUnixMS: at, Bucket: "bkt", Key: "plain",
		VersionID: mint(), Size: 11, ETag: []byte{3}})
	mustOK(t, err)

	at++
	open := mint()
	mustOK(t, s.ApplyCreateMultipartUpload(CreateMultipartUpload{ProposedAtUnixMS: at, Bucket: "bkt", Key: "mp-open", UploadID: open}))
	_, err = s.ApplyUploadPart(UploadPart{ProposedAtUnixMS: at, Bucket: "bkt", Key: "mp-open", UploadID: open, PartNumber: 1, DataID: mint(), Size: MinPartSize, ETag: []byte{4}, Checksum: []byte{5}})
	mustOK(t, err)

	at++
	done := mint()
	mustOK(t, s.ApplyCreateMultipartUpload(CreateMultipartUpload{ProposedAtUnixMS: at, Bucket: "bkt", Key: "mp-done", UploadID: done}))
	_, err = s.ApplyUploadPart(UploadPart{ProposedAtUnixMS: at, Bucket: "bkt", Key: "mp-done", UploadID: done, PartNumber: 1, DataID: mint(), Size: MinPartSize, ETag: []byte{6}, Checksum: []byte{7}})
	mustOK(t, err)
	_, err = s.ApplyCompleteMultipartUpload(CompleteMultipartUpload{ProposedAtUnixMS: at, Bucket: "bkt", Key: "mp-done", UploadID: done,
		VersionID: mint(), ETag: []byte{8}, Parts: []CompletedPart{{PartNumber: 1, ETag: []byte{6}}}})
	mustOK(t, err)

	restored := restoreFrom(t, f)
	if !maps.Equal(dumpRows(s), dumpRows(restored)) {
		t.Fatalf("restored store differs from the one that wrote the rows:\nlive:     %v\nrestored: %v",
			slices.Sorted(maps.Keys(dumpRows(s))), slices.Sorted(maps.Keys(dumpRows(restored))))
	}

	// The restored store keeps working — and keeps persisting.
	restored.SetPersister(f)
	res, err := restored.ApplyDeleteObject(DeleteObject{ProposedAtUnixMS: at + 1, Bucket: "bkt", Key: "plain", VersionID: mint()})
	mustOK(t, err)
	if !res.Removed {
		t.Fatal("delete after restore did not remove")
	}
	if _, ok := restoreFrom(t, f).Current("bkt", "plain"); ok {
		t.Fatal("second restore still sees the deleted object")
	}
}

// A failed persist must leave no trace in memory: the apply reports the
// error and the store is bit-identical to before — disk and memory never
// diverge in either direction.
func TestPersistFailureRollsBack(t *testing.T) {
	f := newFakePersister()
	s := NewStore()
	s.SetPersister(f)
	mustOK(t, s.ApplyCreateBucket(CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"}))
	before := dumpRows(s)

	f.fail = errors.New("disk is on fire")
	_, err := s.ApplyPutObject(PutObject{ProposedAtUnixMS: 2, Bucket: "bkt", Key: "k",
		VersionID: VersionID{1}, Size: 1, ETag: []byte{1}})
	if err == nil || !strings.Contains(err.Error(), "disk is on fire") {
		t.Fatalf("persist failure not surfaced: %v", err)
	}
	if !maps.Equal(before, dumpRows(s)) {
		t.Fatal("in-memory state changed although the persist failed")
	}

	// Recovery: the same apply succeeds once the persister does.
	f.fail = nil
	if _, err := s.ApplyPutObject(PutObject{ProposedAtUnixMS: 3, Bucket: "bkt", Key: "k",
		VersionID: VersionID{1}, Size: 1, ETag: []byte{1}}); err != nil {
		t.Fatalf("apply after persister recovery: %v", err)
	}
	if _, ok := restoreFrom(t, f).Current("bkt", "k"); !ok {
		t.Fatal("recovered put was not persisted")
	}
}

// Applies that change nothing — validation refusals, idempotent misses —
// must not reach the persister at all.
func TestPersistSkipsNoOps(t *testing.T) {
	f := newFakePersister()
	s := NewStore()
	s.SetPersister(f)
	mustOK(t, s.ApplyCreateBucket(CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"}))
	base := f.commits

	if err := s.ApplyCreateBucket(CreateBucket{ProposedAtUnixMS: 1, Bucket: "no"}); err == nil {
		t.Fatal("bad bucket name accepted")
	}
	if _, err := s.ApplyDeleteObject(DeleteObject{ProposedAtUnixMS: 1, Bucket: "bkt", Key: "ghost", VersionID: VersionID{1}}); err != nil {
		t.Fatal(err)
	}
	if f.commits != base {
		t.Fatalf("no-op applies reached the persister: %d commits, want %d", f.commits, base)
	}
}

func TestRestoreRejectsGarbage(t *testing.T) {
	s := NewStore()
	if err := s.Restore("v/bkt\x00k\x00aaaaaaaaaaaaaaaa", []byte{0xFF, 0xFF}); err == nil {
		t.Fatal("garbage row restored without error")
	}
	if err := s.Restore("zz/unknown", nil); err == nil {
		t.Fatal("unknown prefix restored without error")
	}
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
