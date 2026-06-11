package meta

import (
	"bytes"
	"slices"
	"testing"
)

// model is the trivial reference implementation of what the metadata layer
// should do (docs/SIMULATION.md, "checking correctness"): plain maps and
// slices, S3 semantics reimplemented independently of the keyspace, the
// derived index, and the apply code. Each operation takes the proposal and
// what the store did, verifies they agree, and advances the model.
type model struct {
	buckets map[string]*modelBucket
}

type modelBucket struct {
	lockEnabled bool
	versioning  VersioningState
	objects     map[string][]modelVersion // oldest → newest, in commit order
}

type modelVersion struct {
	id       VersionID
	dataID   VersionID // the proposal's minted ID; zero for markers
	kind     Kind
	size     int64
	etag     []byte
	created  int64
	null     bool
	retMode  RetentionMode
	retUntil int64
	hold     bool
}

func newModel() *model {
	return &model{buckets: make(map[string]*modelBucket)}
}

// locked is the model's independent restatement of the lock rule.
func (v modelVersion) locked(at int64, bypass bool) bool {
	if v.hold {
		return true
	}
	if v.retUntil <= at {
		return false
	}
	return v.retMode == RetentionCompliance || (v.retMode == RetentionGovernance && !bypass)
}

func (b *modelBucket) find(key string, vid VersionID) int {
	for i, v := range b.objects[key] {
		if v.id == vid {
			return i
		}
	}
	return -1
}

func (b *modelBucket) findNull(key string) int {
	for i, v := range b.objects[key] {
		if v.null {
			return i
		}
	}
	return -1
}

func (b *modelBucket) removeAt(key string, i int) {
	b.objects[key] = slices.Delete(b.objects[key], i, i+1)
	if len(b.objects[key]) == 0 {
		delete(b.objects, key)
	}
}

// confirm asserts the store's outcome matches the model's expectation.
func confirm(t *testing.T, op string, want, got error) bool {
	t.Helper()
	if want != got {
		t.Fatalf("%s: store returned %v, model expects %v", op, got, want)
	}
	return want == nil
}

func (m *model) createBucket(t *testing.T, p CreateBucket, got error) {
	t.Helper()
	var want error
	switch {
	case validateBucketName(p.Bucket) != nil:
		want = ErrInvalidBucketName
	case m.buckets[p.Bucket] != nil:
		want = ErrBucketExists
	}
	if !confirm(t, "CreateBucket", want, got) {
		return
	}
	versioning := Unversioned
	if p.ObjectLockEnabled {
		versioning = VersioningEnabled
	}
	m.buckets[p.Bucket] = &modelBucket{
		lockEnabled: p.ObjectLockEnabled,
		versioning:  versioning,
		objects:     make(map[string][]modelVersion),
	}
}

func (m *model) deleteBucket(t *testing.T, p DeleteBucket, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case len(b.objects) > 0:
		want = ErrBucketNotEmpty
	}
	if !confirm(t, "DeleteBucket", want, got) {
		return
	}
	delete(m.buckets, p.Bucket)
}

func (m *model) setVersioning(t *testing.T, p SetBucketVersioning, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case p.State != VersioningEnabled && p.State != VersioningSuspended:
		want = ErrInvalidVersioningState
	case p.State == VersioningSuspended && b.lockEnabled:
		want = ErrInvalidVersioningState
	}
	if !confirm(t, "SetBucketVersioning", want, got) {
		return
	}
	b.versioning = p.State
}

func (m *model) put(t *testing.T, p PutObject, res PutResult, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case validateObjectKey(p.Key) != nil:
		want = ErrInvalidObjectKey
	case (p.RetentionMode != RetentionNone || p.LegalHold) && !b.lockEnabled:
		want = ErrInvalidRetention
	case p.RetentionMode != RetentionNone && p.RetainUntilUnixMS <= p.ProposedAtUnixMS:
		want = ErrInvalidRetention
	}
	if !confirm(t, "PutObject", want, got) {
		return
	}
	if vs := b.objects[p.Key]; len(vs) > 0 && res.VersionID.Compare(vs[len(vs)-1].id) <= 0 {
		t.Fatalf("PutObject: committed ID %v does not sort after newest %v", res.VersionID, vs[len(vs)-1].id)
	}
	null := b.versioning != VersioningEnabled
	if null {
		if i := b.findNull(p.Key); i >= 0 {
			b.removeAt(p.Key, i)
		}
	}
	b.objects[p.Key] = append(b.objects[p.Key], modelVersion{
		id: res.VersionID, dataID: p.VersionID, kind: KindObject,
		size: p.Size, etag: slices.Clone(p.ETag), created: p.ProposedAtUnixMS, null: null,
		retMode: p.RetentionMode, retUntil: p.RetainUntilUnixMS, hold: p.LegalHold,
	})
}

func (m *model) deleteObject(t *testing.T, p DeleteObject, res DeleteObjectResult, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case validateObjectKey(p.Key) != nil:
		want = ErrInvalidObjectKey
	}
	if !confirm(t, "DeleteObject", want, got) {
		return
	}
	if b.versioning == Unversioned {
		vs := b.objects[p.Key]
		if res.Removed != (len(vs) > 0) || res.MarkerCreated {
			t.Fatalf("DeleteObject(unversioned): result %+v with %d versions", res, len(vs))
		}
		if len(vs) > 0 {
			b.removeAt(p.Key, len(vs)-1)
		}
		return
	}
	if !res.MarkerCreated || res.Removed {
		t.Fatalf("DeleteObject(versioned): result %+v, want a marker", res)
	}
	if vs := b.objects[p.Key]; len(vs) > 0 && res.MarkerID.Compare(vs[len(vs)-1].id) <= 0 {
		t.Fatalf("DeleteObject: marker ID %v does not sort after newest", res.MarkerID)
	}
	null := b.versioning == VersioningSuspended
	if null {
		if i := b.findNull(p.Key); i >= 0 {
			b.removeAt(p.Key, i)
		}
	}
	b.objects[p.Key] = append(b.objects[p.Key], modelVersion{
		id: res.MarkerID, kind: KindDeleteMarker, created: p.ProposedAtUnixMS, null: null,
	})
}

func (m *model) deleteVersion(t *testing.T, p DeleteVersion, res DeleteVersionResult, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	i := -1
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case validateObjectKey(p.Key) != nil:
		want = ErrInvalidObjectKey
	default:
		if i = b.find(p.Key, p.VersionID); i >= 0 && b.objects[p.Key][i].locked(p.ProposedAtUnixMS, p.BypassGovernance) {
			want = ErrObjectLocked
		}
	}
	if !confirm(t, "DeleteVersion", want, got) {
		return
	}
	if res.Removed != (i >= 0) {
		t.Fatalf("DeleteVersion: Removed=%v but model index %d", res.Removed, i)
	}
	if i >= 0 {
		b.removeAt(p.Key, i)
	}
}

func (m *model) updateRetention(t *testing.T, p UpdateRetention, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	i := -1
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case validateObjectKey(p.Key) != nil:
		want = ErrInvalidObjectKey
	case !b.lockEnabled:
		want = ErrInvalidRetention
	default:
		i = b.find(p.Key, p.VersionID)
		v := modelVersion{}
		switch {
		case i < 0:
			want = ErrNoSuchVersion
		case b.objects[p.Key][i].kind != KindObject:
			want = ErrInvalidRetention
		case p.Mode != RetentionNone && p.RetainUntilUnixMS <= p.ProposedAtUnixMS:
			want = ErrInvalidRetention
		default:
			v = b.objects[p.Key][i]
			curMode := v.retMode
			if curMode != RetentionNone && v.retUntil <= p.ProposedAtUnixMS {
				curMode = RetentionNone
			}
			switch curMode {
			case RetentionCompliance:
				if p.Mode != RetentionCompliance || p.RetainUntilUnixMS < v.retUntil {
					want = ErrObjectLocked
				}
			case RetentionGovernance:
				if !p.BypassGovernance && (p.Mode == RetentionNone || p.RetainUntilUnixMS < v.retUntil) {
					want = ErrObjectLocked
				}
			}
		}
	}
	if !confirm(t, "UpdateRetention", want, got) {
		return
	}
	v := &b.objects[p.Key][i]
	v.retMode = p.Mode
	v.retUntil = p.RetainUntilUnixMS
	if p.Mode == RetentionNone {
		v.retUntil = 0
	}
}

func (m *model) updateLegalHold(t *testing.T, p UpdateLegalHold, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	i := -1
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case validateObjectKey(p.Key) != nil:
		want = ErrInvalidObjectKey
	case !b.lockEnabled:
		want = ErrInvalidRetention
	default:
		if i = b.find(p.Key, p.VersionID); i < 0 {
			want = ErrNoSuchVersion
		} else if b.objects[p.Key][i].kind != KindObject {
			want = ErrInvalidRetention
		}
	}
	if !confirm(t, "UpdateLegalHold", want, got) {
		return
	}
	b.objects[p.Key][i].hold = p.Hold
}

// check compares every observable of the store against the model: bucket
// configs, full version lists (content and newest-first order), current
// resolution, listings, and the v/→c/ derived-index equivalence the design
// promises (METADATA.md, "the current-version index").
func (m *model) check(t *testing.T, s *Store) {
	t.Helper()

	storeBuckets := s.ListBuckets()
	if len(storeBuckets) != len(m.buckets) {
		t.Fatalf("store has %d buckets, model has %d", len(storeBuckets), len(m.buckets))
	}
	for _, cfg := range storeBuckets {
		mb := m.buckets[cfg.Name]
		if mb == nil || cfg.Versioning != mb.versioning || cfg.ObjectLockEnabled != mb.lockEnabled {
			t.Fatalf("bucket %q config mismatch: %+v", cfg.Name, cfg)
		}

		var liveKeys []string
		totalVersions := 0
		for key, mvs := range mb.objects {
			totalVersions += len(mvs)
			svs := s.ListVersions(cfg.Name, key)
			if len(svs) != len(mvs) {
				t.Fatalf("%s/%s: store has %d versions, model has %d", cfg.Name, key, len(svs), len(mvs))
			}
			for j, sv := range svs {
				mv := mvs[len(mvs)-1-j] // store is newest-first, model oldest-first
				if sv.VersionID != mv.id || sv.DataID != mv.dataID || sv.Kind != mv.kind || sv.Size != mv.size ||
					!bytes.Equal(sv.ETag, mv.etag) || sv.CreatedUnixMS != mv.created ||
					sv.NullVersion != mv.null || sv.RetentionMode != mv.retMode ||
					sv.RetainUntilUnixMS != mv.retUntil || sv.LegalHold != mv.hold {
					t.Fatalf("%s/%s version %d mismatch:\nstore %+v\nmodel %+v", cfg.Name, key, j, sv, mv)
				}
				if j > 0 && svs[j-1].VersionID.Compare(sv.VersionID) <= 0 {
					t.Fatalf("%s/%s: version list is not strictly newest-first", cfg.Name, key)
				}
			}

			newest := mvs[len(mvs)-1]
			cur, ok := s.Current(cfg.Name, key)
			if wantCur := newest.kind == KindObject; ok != wantCur {
				t.Fatalf("%s/%s: current present=%v, want %v", cfg.Name, key, ok, wantCur)
			} else if wantCur {
				if cur.VersionID != newest.id || cur.Size != newest.size || !bytes.Equal(cur.ETag, newest.etag) {
					t.Fatalf("%s/%s: current record mismatch: %+v", cfg.Name, key, cur)
				}
				liveKeys = append(liveKeys, key)
			}
		}

		slices.Sort(liveKeys)
		listed := s.ListObjects(cfg.Name, "", "", 0)
		if got := keysOf(listed); !slices.Equal(got, liveKeys) {
			t.Fatalf("bucket %q listing mismatch:\nstore %v\nmodel %v", cfg.Name, got, liveKeys)
		}

		// No phantom rows: the store's version count matches the model's.
		storeVersions := 0
		prefix := bucketVersionsScanPrefix(cfg.Name)
		s.kv.scan(prefix, func(k string, _ any) bool {
			if !hasPrefix(k, prefix) {
				return false
			}
			storeVersions++
			return true
		})
		if storeVersions != totalVersions {
			t.Fatalf("bucket %q: store holds %d version rows, model %d", cfg.Name, storeVersions, totalVersions)
		}
	}

	m.checkDerivedIndex(t, s)
}

// checkDerivedIndex rebuilds what c/ should be from a raw scan of v/ and
// compares it row for row with the actual index — "derived" means exactly
// this equivalence.
func (m *model) checkDerivedIndex(t *testing.T, s *Store) {
	t.Helper()
	for name := range m.buckets {
		expect := make(map[string]VersionID)
		prefix := bucketVersionsScanPrefix(name)
		s.kv.scan(prefix, func(k string, v any) bool {
			if !hasPrefix(k, prefix) {
				return false
			}
			key, _ := keyAndVersionFromVersionRow(k, name)
			if _, seen := expect[key]; !seen {
				// First row per key is its newest version.
				if e := v.(VersionEntry); e.Kind == KindObject {
					expect[key] = e.VersionID
				} else {
					expect[key] = VersionID{} // marker: no current row
				}
			}
			return true
		})
		got := make(map[string]VersionID)
		cPrefix := currentScanPrefix(name)
		s.kv.scan(cPrefix, func(k string, v any) bool {
			if !hasPrefix(k, cPrefix) {
				return false
			}
			got[objectKeyFromCurrentRow(k, name)] = v.(CurrentRecord).VersionID
			return true
		})
		for key, vid := range expect {
			if vid.IsZero() {
				if _, ok := got[key]; ok {
					t.Fatalf("bucket %q key %q: c/ row exists but newest version is a delete marker", name, key)
				}
				continue
			}
			if got[key] != vid {
				t.Fatalf("bucket %q key %q: c/ row %v, rebuild says %v", name, key, got[key], vid)
			}
			delete(got, key)
		}
		for key := range got {
			t.Fatalf("bucket %q: orphan c/ row for key %q with no versions", name, key)
		}
	}
}
