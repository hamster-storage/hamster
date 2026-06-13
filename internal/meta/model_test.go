package meta

import (
	"bytes"
	"slices"
	"strings"
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
	objects     map[string][]modelVersion  // oldest → newest, in commit order
	uploads     map[VersionID]*modelUpload // in-progress multipart uploads
}

type modelVersion struct {
	id       VersionID
	dataID   VersionID // the proposal's minted ID; zero for markers and multipart
	kind     Kind
	size     int64
	etag     []byte
	created  int64
	null     bool
	retMode  RetentionMode
	retUntil int64
	hold     bool
	parts    []modelPartRef // multipart assembly; nil for whole PUTs
}

type modelPartRef struct {
	dataID VersionID
	size   int64
}

type modelUpload struct {
	key     string
	created int64
	parts   map[uint32]modelPart
}

type modelPart struct {
	dataID VersionID
	size   int64
	etag   []byte
}

// partNumbers returns an upload's part numbers in ascending order — the
// deterministic iteration the map cannot give.
func (u *modelUpload) partNumbers() []uint32 {
	nums := make([]uint32, 0, len(u.parts))
	for n := range u.parts {
		nums = append(nums, n)
	}
	slices.Sort(nums)
	return nums
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
		uploads:     make(map[VersionID]*modelUpload),
	}
}

func (m *model) deleteBucket(t *testing.T, p DeleteBucket, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case len(b.objects) > 0 || len(b.uploads) > 0:
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

// modelDataIDs mirrors VersionEntry.DataIDs for a model version.
func modelDataIDs(v modelVersion) []VersionID {
	if len(v.parts) > 0 {
		ids := make([]VersionID, len(v.parts))
		for i, p := range v.parts {
			ids[i] = p.dataID
		}
		return ids
	}
	if v.kind != KindObject || v.dataID.IsZero() {
		return nil
	}
	return []VersionID{v.dataID}
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
	var wantReplaced []VersionID
	if null {
		if i := b.findNull(p.Key); i >= 0 {
			wantReplaced = modelDataIDs(b.objects[p.Key][i])
			b.removeAt(p.Key, i)
		}
	}
	if !slices.Equal(res.ReplacedDataIDs, wantReplaced) {
		t.Fatalf("PutObject: replaced %v, model expects %v", res.ReplacedDataIDs, wantReplaced)
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
		var wantRemoved []VersionID
		if len(vs) > 0 {
			wantRemoved = modelDataIDs(vs[len(vs)-1])
			b.removeAt(p.Key, len(vs)-1)
		}
		if !slices.Equal(res.RemovedDataIDs, wantRemoved) {
			t.Fatalf("DeleteObject: removed %v, model expects %v", res.RemovedDataIDs, wantRemoved)
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
	var wantRemoved []VersionID
	if null {
		if i := b.findNull(p.Key); i >= 0 {
			wantRemoved = modelDataIDs(b.objects[p.Key][i])
			b.removeAt(p.Key, i)
		}
	}
	if !slices.Equal(res.RemovedDataIDs, wantRemoved) {
		t.Fatalf("DeleteObject(marker): removed %v, model expects %v", res.RemovedDataIDs, wantRemoved)
	}
	b.objects[p.Key] = append(b.objects[p.Key], modelVersion{
		id: res.MarkerID, kind: KindDeleteMarker, created: p.ProposedAtUnixMS, null: null,
	})
}

func (m *model) createUpload(t *testing.T, p CreateMultipartUpload, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case validateObjectKey(p.Key) != nil:
		want = ErrInvalidObjectKey
	case b.uploads[p.UploadID] != nil:
		want = ErrUploadExists
	}
	if !confirm(t, "CreateMultipartUpload", want, got) {
		return
	}
	b.uploads[p.UploadID] = &modelUpload{key: p.Key, created: p.ProposedAtUnixMS, parts: make(map[uint32]modelPart)}
}

func (m *model) uploadPart(t *testing.T, p UploadPart, res UploadPartResult, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	var up *modelUpload
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case validateObjectKey(p.Key) != nil:
		want = ErrInvalidObjectKey
	case p.PartNumber < 1 || p.PartNumber > MaxPartNumber:
		want = ErrInvalidPartNumber
	default:
		if up = b.uploads[p.UploadID]; up == nil || up.key != p.Key {
			up = nil
			want = ErrNoSuchUpload
		}
	}
	if !confirm(t, "UploadPart", want, got) {
		return
	}
	if prior, ok := up.parts[p.PartNumber]; ok != !res.ReplacedDataID.IsZero() || (ok && prior.dataID != res.ReplacedDataID) {
		t.Fatalf("UploadPart: replaced %v, model has %+v", res.ReplacedDataID, up.parts[p.PartNumber])
	}
	up.parts[p.PartNumber] = modelPart{dataID: p.DataID, size: p.Size, etag: slices.Clone(p.ETag)}
}

// completeUpload restates the completion contract independently: list
// ordering first, then per-part existence, ETag, and size; on success the
// upload becomes a committed version exactly like a PUT, and the unused
// parts come back for reclaim in part-number order.
func (m *model) completeUpload(t *testing.T, p CompleteMultipartUpload, res CompleteResult, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	var up *modelUpload
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case validateObjectKey(p.Key) != nil:
		want = ErrInvalidObjectKey
	default:
		if up = b.uploads[p.UploadID]; up == nil || up.key != p.Key {
			up = nil
			want = ErrNoSuchUpload
		} else if len(p.Parts) == 0 {
			want = ErrInvalidPart
		} else {
			for i := 1; i < len(p.Parts) && want == nil; i++ {
				if p.Parts[i].PartNumber <= p.Parts[i-1].PartNumber {
					want = ErrInvalidPartOrder
				}
			}
			for i, cp := range p.Parts {
				if want != nil {
					break
				}
				mp, ok := up.parts[cp.PartNumber]
				switch {
				case !ok || !bytes.Equal(mp.etag, cp.ETag):
					want = ErrInvalidPart
				case i < len(p.Parts)-1 && mp.size < MinPartSize:
					want = ErrPartTooSmall
				}
			}
		}
	}
	if !confirm(t, "CompleteMultipartUpload", want, got) {
		return
	}

	used := make(map[uint32]bool, len(p.Parts))
	var size int64
	var parts []modelPartRef
	for _, cp := range p.Parts {
		mp := up.parts[cp.PartNumber]
		size += mp.size
		parts = append(parts, modelPartRef{dataID: mp.dataID, size: mp.size})
		used[cp.PartNumber] = true
	}
	if vs := b.objects[p.Key]; len(vs) > 0 && res.VersionID.Compare(vs[len(vs)-1].id) <= 0 {
		t.Fatalf("CompleteMultipartUpload: committed ID %v does not sort after newest", res.VersionID)
	}
	null := b.versioning != VersioningEnabled
	var wantDiscard []VersionID
	if null {
		if i := b.findNull(p.Key); i >= 0 {
			wantDiscard = modelDataIDs(b.objects[p.Key][i])
			b.removeAt(p.Key, i)
		}
	}
	for _, n := range up.partNumbers() {
		if !used[n] {
			wantDiscard = append(wantDiscard, up.parts[n].dataID)
		}
	}
	if !slices.Equal(res.DiscardedDataIDs, wantDiscard) {
		t.Fatalf("CompleteMultipartUpload: discarded %v, model expects %v", res.DiscardedDataIDs, wantDiscard)
	}
	b.objects[p.Key] = append(b.objects[p.Key], modelVersion{
		id: res.VersionID, kind: KindObject, size: size,
		etag: slices.Clone(p.ETag), created: p.ProposedAtUnixMS, null: null, parts: parts,
	})
	delete(b.uploads, p.UploadID)
}

func (m *model) abortUpload(t *testing.T, p AbortMultipartUpload, res AbortResult, got error) {
	t.Helper()
	var want error
	b := m.buckets[p.Bucket]
	var up *modelUpload
	switch {
	case b == nil:
		want = ErrNoSuchBucket
	case validateObjectKey(p.Key) != nil:
		want = ErrInvalidObjectKey
	default:
		if up = b.uploads[p.UploadID]; up == nil || up.key != p.Key {
			up = nil
			want = ErrNoSuchUpload
		}
	}
	if !confirm(t, "AbortMultipartUpload", want, got) {
		return
	}
	var wantIDs []VersionID
	for _, n := range up.partNumbers() {
		wantIDs = append(wantIDs, up.parts[n].dataID)
	}
	if !slices.Equal(res.PartDataIDs, wantIDs) {
		t.Fatalf("AbortMultipartUpload: reclaim list %v, model expects %v", res.PartDataIDs, wantIDs)
	}
	delete(b.uploads, p.UploadID)
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
				if len(sv.Parts) != len(mv.parts) {
					t.Fatalf("%s/%s version %d: store has %d parts, model %d", cfg.Name, key, j, len(sv.Parts), len(mv.parts))
				}
				for pi, sp := range sv.Parts {
					if sp.DataID != mv.parts[pi].dataID || sp.Size != mv.parts[pi].size {
						t.Fatalf("%s/%s version %d part %d mismatch: %+v vs %+v", cfg.Name, key, j, pi, sp, mv.parts[pi])
					}
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
				if cur.VersionID != newest.id || cur.Size != newest.size || !bytes.Equal(cur.ETag, newest.etag) ||
					cur.PartCount != uint32(len(newest.parts)) {
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

		m.checkUploads(t, s, cfg.Name, mb)
	}

	m.checkDerivedIndex(t, s)
}

// checkUploads compares a bucket's u/ state against the model: the upload
// listing (content and key-then-initiation order), every upload's part
// rows, and the row count (no phantoms).
func (m *model) checkUploads(t *testing.T, s *Store, bucket string, mb *modelBucket) {
	t.Helper()

	type uploadID struct {
		key string
		id  VersionID
	}
	var want []uploadID
	for id, up := range mb.uploads {
		want = append(want, uploadID{up.key, id})
	}
	slices.SortFunc(want, func(a, b uploadID) int {
		if a.key != b.key {
			return strings.Compare(a.key, b.key)
		}
		return a.id.Compare(b.id)
	})
	got := s.ListUploads(bucket, "", "", VersionID{}, len(want)+1)
	if len(got) != len(want) {
		t.Fatalf("bucket %q: store lists %d uploads, model has %d", bucket, len(got), len(want))
	}
	for i, g := range got {
		if g.Key != want[i].key || g.Upload.UploadID != want[i].id {
			t.Fatalf("bucket %q upload %d: store (%q, %v), model (%q, %v)",
				bucket, i, g.Key, g.Upload.UploadID, want[i].key, want[i].id)
		}
	}

	totalParts := 0
	for id, up := range mb.uploads {
		totalParts += len(up.parts)
		parts, ok := s.ListUploadParts(bucket, up.key, id, 0, len(up.parts)+1)
		if !ok || len(parts) != len(up.parts) {
			t.Fatalf("bucket %q upload %v: store has %d parts (exists=%v), model %d", bucket, id, len(parts), ok, len(up.parts))
		}
		for _, sp := range parts {
			mp, ok := up.parts[sp.PartNumber]
			if !ok || sp.DataID != mp.dataID || sp.Size != mp.size || !bytes.Equal(sp.ETag, mp.etag) {
				t.Fatalf("bucket %q upload %v part %d mismatch: %+v vs %+v", bucket, id, sp.PartNumber, sp, mp)
			}
		}
	}

	rows := 0
	prefix := uploadsScanPrefix(bucket)
	s.kv.scan(prefix, func(k string, _ any) bool {
		if !hasPrefix(k, prefix) {
			return false
		}
		rows++
		return true
	})
	if rows != len(mb.uploads)+totalParts {
		t.Fatalf("bucket %q: store holds %d u/ rows, model %d", bucket, rows, len(mb.uploads)+totalParts)
	}
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
