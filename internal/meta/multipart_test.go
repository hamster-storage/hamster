package meta

import (
	"errors"
	"testing"
)

// Multipart fixtures on env (apply_test.go).

func (e *env) createUpload(bucket, key string) VersionID {
	e.t.Helper()
	at := e.tick()
	uid := mintAt(at, e.rng)
	if err := e.s.ApplyCreateMultipartUpload(CreateMultipartUpload{
		ProposedAtUnixMS: at, Bucket: bucket, Key: key, UploadID: uid,
	}); err != nil {
		e.t.Fatal(err)
	}
	return uid
}

func (e *env) uploadPart(bucket, key string, uid VersionID, n uint32, size int64) (VersionID, []byte) {
	e.t.Helper()
	at := e.tick()
	dataID := mintAt(at, e.rng)
	etag := randETag(e.rng)
	if _, err := e.s.ApplyUploadPart(UploadPart{
		ProposedAtUnixMS: at, Bucket: bucket, Key: key, UploadID: uid,
		PartNumber: n, DataID: dataID, Size: size, ETag: etag,
	}); err != nil {
		e.t.Fatal(err)
	}
	return dataID, etag
}

func TestMultipartLifecycle(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	uid := e.createUpload("docs", "video.mp4")

	d1, t1 := e.uploadPart("docs", "video.mp4", uid, 1, MinPartSize)
	d2, t2 := e.uploadPart("docs", "video.mp4", uid, 2, MinPartSize+7)
	d3, t3 := e.uploadPart("docs", "video.mp4", uid, 3, 100) // last part: any size

	at := e.tick()
	res, err := e.s.ApplyCompleteMultipartUpload(CompleteMultipartUpload{
		ProposedAtUnixMS: at, Bucket: "docs", Key: "video.mp4", UploadID: uid,
		VersionID: mintAt(at, e.rng), ETag: []byte{0xAA},
		Parts: []CompletedPart{{1, t1}, {2, t2}, {3, t3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DiscardedDataIDs) != 0 {
		t.Fatalf("complete discarded %v, used every part", res.DiscardedDataIDs)
	}

	entry, ok := e.s.GetVersion("docs", "video.mp4", res.VersionID)
	if !ok {
		t.Fatal("completed version not found")
	}
	if entry.Size != 2*MinPartSize+107 {
		t.Fatalf("size %d, want sum of parts", entry.Size)
	}
	if !entry.DataID.IsZero() {
		t.Fatal("multipart entry must not carry a single DataID")
	}
	want := []VersionID{d1, d2, d3}
	got := entry.DataIDs()
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("DataIDs %v, want the part addresses %v in order", got, want)
	}
	cur, ok := e.s.Current("docs", "video.mp4")
	if !ok || cur.PartCount != 3 || cur.Size != entry.Size {
		t.Fatalf("current record %+v, want part count 3", cur)
	}

	// The upload state is gone: completing again must miss.
	if _, err := e.s.ApplyCompleteMultipartUpload(CompleteMultipartUpload{
		ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "video.mp4", UploadID: uid,
		VersionID: mintAt(e.now, e.rng), Parts: []CompletedPart{{1, t1}},
	}); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("second complete: %v, want ErrNoSuchUpload", err)
	}
	if _, exists := e.s.GetUpload("docs", "video.mp4", uid); exists {
		t.Fatal("upload record survived completion")
	}
}

func TestCompleteValidation(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	uid := e.createUpload("docs", "k")
	_, t1 := e.uploadPart("docs", "k", uid, 1, MinPartSize)
	_, t2 := e.uploadPart("docs", "k", uid, 2, 50) // undersized: only valid as the last part

	complete := func(parts []CompletedPart) error {
		at := e.tick()
		_, err := e.s.ApplyCompleteMultipartUpload(CompleteMultipartUpload{
			ProposedAtUnixMS: at, Bucket: "docs", Key: "k", UploadID: uid,
			VersionID: mintAt(at, e.rng), Parts: parts,
		})
		return err
	}

	if err := complete(nil); !errors.Is(err, ErrInvalidPart) {
		t.Fatalf("empty part list: %v", err)
	}
	if err := complete([]CompletedPart{{1, randETag(e.rng)}}); !errors.Is(err, ErrInvalidPart) {
		t.Fatalf("wrong ETag: %v", err)
	}
	if err := complete([]CompletedPart{{1, t1}, {7, t2}}); !errors.Is(err, ErrInvalidPart) {
		t.Fatalf("missing part: %v", err)
	}
	if err := complete([]CompletedPart{{2, t2}, {1, t1}}); !errors.Is(err, ErrInvalidPartOrder) {
		t.Fatalf("descending order: %v", err)
	}
	if err := complete([]CompletedPart{{1, t1}, {1, t1}}); !errors.Is(err, ErrInvalidPartOrder) {
		t.Fatalf("duplicate part number: %v", err)
	}
	if err := complete([]CompletedPart{{2, t2}, {1, t1}}); !errors.Is(err, ErrInvalidPartOrder) {
		t.Fatalf("descending: %v", err)
	}
	// Part 2 is undersized, so it cannot appear before the end of the list.
	uid2 := e.createUpload("docs", "k")
	_, u1 := e.uploadPart("docs", "k", uid2, 1, 50)
	_, u2 := e.uploadPart("docs", "k", uid2, 2, MinPartSize)
	at := e.tick()
	if _, err := e.s.ApplyCompleteMultipartUpload(CompleteMultipartUpload{
		ProposedAtUnixMS: at, Bucket: "docs", Key: "k", UploadID: uid2,
		VersionID: mintAt(at, e.rng), Parts: []CompletedPart{{1, u1}, {2, u2}},
	}); !errors.Is(err, ErrPartTooSmall) {
		t.Fatalf("undersized non-last part: %v", err)
	}
	// A failed complete keeps the upload alive: the undersized part alone,
	// as the only (last) part, completes fine.
	if _, err := e.s.ApplyCompleteMultipartUpload(CompleteMultipartUpload{
		ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "k", UploadID: uid2,
		VersionID: mintAt(e.now, e.rng), Parts: []CompletedPart{{1, u1}},
	}); err != nil {
		t.Fatalf("single undersized part: %v", err)
	}

	// Unknown upload ID.
	at = e.tick()
	if _, err := e.s.ApplyCompleteMultipartUpload(CompleteMultipartUpload{
		ProposedAtUnixMS: at, Bucket: "docs", Key: "k", UploadID: mintAt(at, e.rng),
		VersionID: mintAt(at, e.rng), Parts: []CompletedPart{{1, t1}},
	}); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("unknown upload: %v", err)
	}
}

func TestCompleteDiscardsUnusedParts(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	uid := e.createUpload("docs", "k")
	_, t1 := e.uploadPart("docs", "k", uid, 1, MinPartSize)
	d2, _ := e.uploadPart("docs", "k", uid, 2, MinPartSize)
	_, t3 := e.uploadPart("docs", "k", uid, 3, 10)

	at := e.tick()
	res, err := e.s.ApplyCompleteMultipartUpload(CompleteMultipartUpload{
		ProposedAtUnixMS: at, Bucket: "docs", Key: "k", UploadID: uid,
		VersionID: mintAt(at, e.rng), Parts: []CompletedPart{{1, t1}, {3, t3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.DiscardedDataIDs) != 1 || res.DiscardedDataIDs[0] != d2 {
		t.Fatalf("discarded %v, want exactly the skipped part %v", res.DiscardedDataIDs, d2)
	}
	entry, _ := e.s.GetVersion("docs", "k", res.VersionID)
	if len(entry.Parts) != 2 || entry.Size != MinPartSize+10 {
		t.Fatalf("entry %+v, want the two used parts", entry)
	}
}

func TestUploadPartRules(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	uid := e.createUpload("docs", "k")

	// Re-uploading a part number replaces it and surfaces the old address.
	first, _ := e.uploadPart("docs", "k", uid, 1, 10)
	at := e.tick()
	res, err := e.s.ApplyUploadPart(UploadPart{
		ProposedAtUnixMS: at, Bucket: "docs", Key: "k", UploadID: uid,
		PartNumber: 1, DataID: mintAt(at, e.rng), Size: 20, ETag: randETag(e.rng),
	})
	if err != nil || res.ReplacedDataID != first {
		t.Fatalf("re-upload: %+v, %v — want the displaced address %v", res, err, first)
	}

	for _, bad := range []uint32{0, MaxPartNumber + 1} {
		at := e.tick()
		if _, err := e.s.ApplyUploadPart(UploadPart{
			ProposedAtUnixMS: at, Bucket: "docs", Key: "k", UploadID: uid,
			PartNumber: bad, DataID: mintAt(at, e.rng),
		}); !errors.Is(err, ErrInvalidPartNumber) {
			t.Fatalf("part number %d: %v", bad, err)
		}
	}
	at = e.tick()
	if _, err := e.s.ApplyUploadPart(UploadPart{
		ProposedAtUnixMS: at, Bucket: "docs", Key: "k", UploadID: mintAt(at, e.rng),
		PartNumber: 1, DataID: mintAt(at, e.rng),
	}); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("unknown upload: %v", err)
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	uid := e.createUpload("docs", "k")
	d1, _ := e.uploadPart("docs", "k", uid, 1, 10)
	d2, _ := e.uploadPart("docs", "k", uid, 5, 10)

	res, err := e.s.ApplyAbortMultipartUpload(AbortMultipartUpload{
		ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "k", UploadID: uid,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.PartDataIDs) != 2 || res.PartDataIDs[0] != d1 || res.PartDataIDs[1] != d2 {
		t.Fatalf("abort returned %v, want both part addresses in order", res.PartDataIDs)
	}
	if _, err := e.s.ApplyAbortMultipartUpload(AbortMultipartUpload{
		ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "k", UploadID: uid,
	}); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("second abort: %v, want ErrNoSuchUpload", err)
	}
}

func TestDeleteBucketBlockedByUpload(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	uid := e.createUpload("docs", "k")

	if err := e.s.ApplyDeleteBucket(DeleteBucket{ProposedAtUnixMS: e.tick(), Bucket: "docs"}); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("delete with in-progress upload: %v, want ErrBucketNotEmpty", err)
	}
	if _, err := e.s.ApplyAbortMultipartUpload(AbortMultipartUpload{
		ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "k", UploadID: uid,
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.s.ApplyDeleteBucket(DeleteBucket{ProposedAtUnixMS: e.tick(), Bucket: "docs"}); err != nil {
		t.Fatalf("delete after abort: %v", err)
	}
}

func TestCompleteBumpsLikePut(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)

	fast := e.now + 60_000
	first, err := e.s.ApplyPutObject(PutObject{ProposedAtUnixMS: fast, Bucket: "docs", Key: "k", VersionID: mintAt(fast, e.rng)})
	if err != nil {
		t.Fatal(err)
	}

	uid := e.createUpload("docs", "k")
	d1, t1 := e.uploadPart("docs", "k", uid, 1, 10)
	slow := e.now
	res, err := e.s.ApplyCompleteMultipartUpload(CompleteMultipartUpload{
		ProposedAtUnixMS: slow, Bucket: "docs", Key: "k", UploadID: uid,
		VersionID: mintAt(slow, e.rng), Parts: []CompletedPart{{1, t1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.VersionID != first.VersionID.Next() {
		t.Fatalf("complete did not bump past the fast-clock write: %v", res.VersionID)
	}
	// The bump moves the identity; the part data addresses never move.
	entry, _ := e.s.GetVersion("docs", "k", res.VersionID)
	if len(entry.Parts) != 1 || entry.Parts[0].DataID != d1 {
		t.Fatalf("part address moved: %+v", entry.Parts)
	}
}

func TestListUploadsAndParts(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	uidA1 := e.createUpload("docs", "a")
	uidA2 := e.createUpload("docs", "a")
	uidB := e.createUpload("docs", "logs/b")
	e.uploadPart("docs", "a", uidA1, 2, 10)
	e.uploadPart("docs", "a", uidA1, 4, 10)
	e.uploadPart("docs", "a", uidA1, 9, 10)

	// Key order, then initiation order within a key.
	all := e.s.ListUploads("docs", "", "", VersionID{}, 100)
	if len(all) != 3 || all[0].Upload.UploadID != uidA1 || all[1].Upload.UploadID != uidA2 || all[2].Upload.UploadID != uidB {
		t.Fatalf("ListUploads order: %+v", all)
	}
	// Prefix narrows by key.
	if got := e.s.ListUploads("docs", "logs/", "", VersionID{}, 100); len(got) != 1 || got[0].Key != "logs/b" {
		t.Fatalf("prefix listing: %+v", got)
	}
	// key-marker without an upload marker skips the whole key.
	if got := e.s.ListUploads("docs", "", "a", VersionID{}, 100); len(got) != 1 || got[0].Key != "logs/b" {
		t.Fatalf("key-marker listing: %+v", got)
	}
	// The marker pair resumes inside a key's uploads.
	if got := e.s.ListUploads("docs", "", "a", uidA1, 100); len(got) != 2 || got[0].Upload.UploadID != uidA2 {
		t.Fatalf("marker-pair listing: %+v", got)
	}

	parts, ok := e.s.ListUploadParts("docs", "a", uidA1, 0, 100)
	if !ok || len(parts) != 3 || parts[0].PartNumber != 2 || parts[2].PartNumber != 9 {
		t.Fatalf("ListUploadParts: ok=%v %+v", ok, parts)
	}
	// Resume after part 2, capped at one result.
	parts, _ = e.s.ListUploadParts("docs", "a", uidA1, 2, 1)
	if len(parts) != 1 || parts[0].PartNumber != 4 {
		t.Fatalf("resumed parts listing: %+v", parts)
	}
	if _, ok := e.s.ListUploadParts("docs", "a", mintAt(e.tick(), e.rng), 0, 100); ok {
		t.Fatal("parts listing for unknown upload reported existence")
	}
}
