package meta

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
)

// The randomized workload: seeded, reproducible runs driving every
// proposal type — including the hostile ones the design demands we try
// (NUL keys, COMPLIANCE deletes, retention weakening, clock skew) —
// against the store and the reference model in lockstep. This is the
// metadata slice of the SIMULATION.md checker; the cluster harness will
// drive the same store through Raft later.

const (
	workloadSeeds = 30
	workloadOps   = 1500
)

func TestRandomWorkloadAgainstModel(t *testing.T) {
	var stats workloadStats
	for seed := range uint64(workloadSeeds) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			st := runWorkload(t, seed, workloadOps)
			stats.bumps += st.bumps
			stats.lockRefusals += st.lockRefusals
			stats.completes += st.completes
		})
	}
	// The workload must actually exercise the paths that matter, not just
	// pass vacuously.
	if stats.bumps == 0 {
		t.Error("no clock-skew version-ID bump ever occurred; the skew model is broken")
	}
	if stats.lockRefusals == 0 {
		t.Error("no COMPLIANCE delete was ever refused; the harness is not trying")
	}
	if stats.completes == 0 {
		t.Error("no multipart upload ever completed; the multipart workload is broken")
	}
}

func TestWorkloadDeterminism(t *testing.T) {
	a := traceWorkload(t, 7)
	b := traceWorkload(t, 7)
	if !slices.Equal(a, b) {
		t.Fatal("two runs with the same seed diverged")
	}
	c := traceWorkload(t, 8)
	if slices.Equal(a, c) {
		t.Fatal("different seeds produced identical traces")
	}
}

type workloadStats struct {
	bumps        int // proposals whose version ID apply had to bump
	lockRefusals int // destruction attempts refused by ErrObjectLocked
	completes    int // multipart completes that committed a version
	trace        []string
}

func traceWorkload(t *testing.T, seed uint64) []string {
	t.Helper()
	return runWorkload(t, seed, 600).trace
}

func runWorkload(t *testing.T, seed uint64, ops int) *workloadStats {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 0))
	s := NewStore()
	persisted := newFakePersister()
	s.SetPersister(persisted)
	m := newModel()
	stats := &workloadStats{}

	bucketPool := []string{"alpha", "beta", "vault"} // "vault" is created lock-enabled
	keyPool := []string{
		"a", "b", "a/b", "a/b/c", "photos/2026/cat.jpg", "photos/2026/dog.jpg",
		"日本語のキー", "zz", "report.pdf", " leading-space", "trailing/",
	}
	bucket := func() string { return bucketPool[rng.IntN(len(bucketPool))] }
	key := func() string { return keyPool[rng.IntN(len(keyPool))] }

	// knownVersion picks a real version from the model when one exists, so
	// version-targeting ops usually hit something.
	knownVersion := func() (string, string, VersionID, bool) {
		var candidates []struct {
			b, k string
			id   VersionID
		}
		for _, bn := range bucketPool {
			mb := m.buckets[bn]
			if mb == nil {
				continue
			}
			for _, kn := range keyPool {
				for _, v := range mb.objects[kn] {
					candidates = append(candidates, struct {
						b, k string
						id   VersionID
					}{bn, kn, v.id})
				}
			}
		}
		if len(candidates) == 0 {
			return "", "", VersionID{}, false
		}
		c := candidates[rng.IntN(len(candidates))]
		return c.b, c.k, c.id, true
	}

	// knownUpload picks a live multipart upload from the model. Candidates
	// are gathered in deterministic order (pool order, then key, then ID) —
	// map iteration would silently break trace determinism.
	knownUpload := func() (string, string, VersionID, bool) {
		type cand struct {
			b, k string
			id   VersionID
		}
		var candidates []cand
		for _, bn := range bucketPool {
			mb := m.buckets[bn]
			if mb == nil {
				continue
			}
			var local []cand
			for id, up := range mb.uploads {
				local = append(local, cand{bn, up.key, id})
			}
			slices.SortFunc(local, func(a, b cand) int {
				if a.k != b.k {
					return strings.Compare(a.k, b.k)
				}
				return a.id.Compare(b.id)
			})
			candidates = append(candidates, local...)
		}
		if len(candidates) == 0 {
			return "", "", VersionID{}, false
		}
		c := candidates[rng.IntN(len(candidates))]
		return c.b, c.k, c.id, true
	}

	// partList builds a completion list from an upload's stored parts —
	// honest by default, deliberately corrupted some of the time.
	partList := func(b string, id VersionID) []CompletedPart {
		up := m.buckets[b].uploads[id]
		var parts []CompletedPart
		for _, n := range up.partNumbers() {
			parts = append(parts, CompletedPart{PartNumber: n, ETag: slices.Clone(up.parts[n].etag)})
		}
		if len(parts) == 0 {
			return nil // empty list: must be refused
		}
		switch rng.IntN(8) {
		case 0: // ordering violation
			if len(parts) > 1 {
				parts[0], parts[1] = parts[1], parts[0]
			}
		case 1: // ETag mismatch
			parts[rng.IntN(len(parts))].ETag = randETag(rng)
		case 2: // a part that was never uploaded
			parts = append(parts, CompletedPart{PartNumber: MaxPartNumber, ETag: randETag(rng)})
		case 3: // subset: drop the tail
			parts = parts[:1+rng.IntN(len(parts))]
		}
		return parts
	}

	now := int64(1_750_000_000_000)
	record := func(format string, args ...any) {
		stats.trace = append(stats.trace, fmt.Sprintf(format, args...))
	}

	for op := 0; op < ops; op++ {
		now += rng.Int64N(2_000)
		// Per-proposal clock skew, ±10s: the proposing node's clock is
		// not to be trusted, and apply must keep order anyway.
		at := now + rng.Int64N(20_001) - 10_000
		mint := func() VersionID { return NewVersionID(time.UnixMilli(at), rng) }

		switch c := rng.IntN(100); {
		case c < 6:
			p := CreateBucket{ProposedAtUnixMS: at, Bucket: bucket()}
			p.ObjectLockEnabled = p.Bucket == "vault"
			err := s.ApplyCreateBucket(p)
			m.createBucket(t, p, err)
			record("create-bucket %s lock=%v: %v", p.Bucket, p.ObjectLockEnabled, err)

		case c < 9:
			p := DeleteBucket{ProposedAtUnixMS: at, Bucket: bucket()}
			err := s.ApplyDeleteBucket(p)
			m.deleteBucket(t, p, err)
			record("delete-bucket %s: %v", p.Bucket, err)

		case c < 14:
			states := []VersioningState{VersioningEnabled, VersioningSuspended, Unversioned}
			p := SetBucketVersioning{ProposedAtUnixMS: at, Bucket: bucket(), State: states[rng.IntN(len(states))]}
			err := s.ApplySetBucketVersioning(p)
			m.setVersioning(t, p, err)
			record("set-versioning %s %d: %v", p.Bucket, p.State, err)

		case c < 40:
			p := PutObject{
				ProposedAtUnixMS: at, Bucket: bucket(), Key: key(), VersionID: mint(),
				Size: rng.Int64N(1 << 20), ETag: randETag(rng), ContentType: "application/octet-stream",
				Partition: rng.Uint64() % 64, ECDataShards: 4, ECParityShards: 2,
			}
			if rng.IntN(4) == 0 { // sometimes locked, sometimes on lockless buckets (error path)
				p.RetentionMode = RetentionMode(1 + rng.IntN(2))
				p.RetainUntilUnixMS = at + rng.Int64N(120_000) - 20_000 // occasionally already past
				p.LegalHold = rng.IntN(8) == 0
			}
			minted := p.VersionID
			res, err := s.ApplyPutObject(p)
			m.put(t, p, res, err)
			if err == nil && res.VersionID != minted {
				stats.bumps++
			}
			record("put %s/%s ret=%d: %v %v", p.Bucket, p.Key, p.RetentionMode, res.VersionID, err)

		case c < 45:
			p := CreateMultipartUpload{ProposedAtUnixMS: at, Bucket: bucket(), Key: key(), UploadID: mint()}
			err := s.ApplyCreateMultipartUpload(p)
			m.createUpload(t, p, err)
			record("mp-create %s/%s %v: %v", p.Bucket, p.Key, p.UploadID, err)

		case c < 54:
			p := UploadPart{
				ProposedAtUnixMS: at, PartNumber: uint32(1 + rng.IntN(5)),
				DataID: mint(), ETag: randETag(rng),
				Size: MinPartSize + rng.Int64N(1<<20),
			}
			if rng.IntN(4) == 0 {
				p.Size = rng.Int64N(MinPartSize) // undersized: only valid as a last part
			}
			if rng.IntN(10) == 0 {
				p.PartNumber = uint32(rng.IntN(2)) * (MaxPartNumber + 1) // 0 or 10001: must be refused
			}
			if b, k, id, ok := knownUpload(); ok && rng.IntN(8) != 0 {
				p.Bucket, p.Key, p.UploadID = b, k, id
			} else {
				p.Bucket, p.Key, p.UploadID = bucket(), key(), mint() // miss: no such upload
			}
			res, err := s.ApplyUploadPart(p)
			m.uploadPart(t, p, res, err)
			record("mp-part %s/%s %v n=%d size=%d: %+v %v", p.Bucket, p.Key, p.UploadID, p.PartNumber, p.Size, res, err)

		case c < 60:
			p := CompleteMultipartUpload{ProposedAtUnixMS: at, VersionID: mint(), ETag: randETag(rng)}
			if b, k, id, ok := knownUpload(); ok && rng.IntN(8) != 0 {
				p.Bucket, p.Key, p.UploadID = b, k, id
				p.Parts = partList(b, id)
			} else {
				p.Bucket, p.Key, p.UploadID = bucket(), key(), mint()
				p.Parts = []CompletedPart{{PartNumber: 1, ETag: randETag(rng)}}
			}
			res, err := s.ApplyCompleteMultipartUpload(p)
			m.completeUpload(t, p, res, err)
			if err == nil {
				stats.completes++
			}
			record("mp-complete %s/%s %v parts=%d: %v %v", p.Bucket, p.Key, p.UploadID, len(p.Parts), res.VersionID, err)

		case c < 63:
			p := AbortMultipartUpload{ProposedAtUnixMS: at}
			if b, k, id, ok := knownUpload(); ok && rng.IntN(8) != 0 {
				p.Bucket, p.Key, p.UploadID = b, k, id
			} else {
				p.Bucket, p.Key, p.UploadID = bucket(), key(), mint()
			}
			res, err := s.ApplyAbortMultipartUpload(p)
			m.abortUpload(t, p, res, err)
			record("mp-abort %s/%s %v: %d %v", p.Bucket, p.Key, p.UploadID, len(res.PartDataIDs), err)

		case c < 71:
			p := DeleteObject{ProposedAtUnixMS: at, Bucket: bucket(), Key: key(), VersionID: mint()}
			res, err := s.ApplyDeleteObject(p)
			m.deleteObject(t, p, res, err)
			record("delete %s/%s: %+v %v", p.Bucket, p.Key, res, err)

		case c < 81:
			p := DeleteVersion{ProposedAtUnixMS: at, BypassGovernance: rng.IntN(2) == 0}
			if b, k, id, ok := knownVersion(); ok && rng.IntN(8) != 0 {
				p.Bucket, p.Key, p.VersionID = b, k, id
			} else {
				p.Bucket, p.Key, p.VersionID = bucket(), key(), mint() // miss: idempotent no-op
			}
			res, err := s.ApplyDeleteVersion(p)
			m.deleteVersion(t, p, res, err)
			if err == ErrObjectLocked {
				stats.lockRefusals++
			}
			record("delete-version %s/%s %v bypass=%v: %+v %v", p.Bucket, p.Key, p.VersionID, p.BypassGovernance, res, err)

		case c < 88:
			p := UpdateRetention{
				ProposedAtUnixMS: at,
				Mode:             RetentionMode(rng.IntN(3)),
				BypassGovernance: rng.IntN(2) == 0,
			}
			p.RetainUntilUnixMS = at + rng.Int64N(240_000) - 40_000
			if b, k, id, ok := knownVersion(); ok && rng.IntN(8) != 0 {
				p.Bucket, p.Key, p.VersionID = b, k, id
			} else {
				p.Bucket, p.Key, p.VersionID = bucket(), key(), mint()
			}
			err := s.ApplyUpdateRetention(p)
			m.updateRetention(t, p, err)
			if err == ErrObjectLocked {
				stats.lockRefusals++
			}
			record("retention %s/%s mode=%d until=%d: %v", p.Bucket, p.Key, p.Mode, p.RetainUntilUnixMS, err)

		case c < 93:
			p := UpdateLegalHold{ProposedAtUnixMS: at, Hold: rng.IntN(2) == 0}
			if b, k, id, ok := knownVersion(); ok && rng.IntN(8) != 0 {
				p.Bucket, p.Key, p.VersionID = b, k, id
			} else {
				p.Bucket, p.Key, p.VersionID = bucket(), key(), mint()
			}
			err := s.ApplyUpdateLegalHold(p)
			m.updateLegalHold(t, p, err)
			record("legal-hold %s/%s %v: %v", p.Bucket, p.Key, p.Hold, err)

		default:
			// Hostile keys, exactly as METADATA.md tells the workload
			// generator to try: the NUL byte must die in apply.
			hostile := []string{"a\x00b", "", "\x00"}
			p := PutObject{ProposedAtUnixMS: at, Bucket: bucket(), Key: hostile[rng.IntN(len(hostile))], VersionID: mint()}
			res, err := s.ApplyPutObject(p)
			m.put(t, p, res, err)
			record("hostile-put %s/%q: %v", p.Bucket, p.Key, err)
		}

		if op%25 == 24 {
			m.check(t, s)
		}
	}
	m.check(t, s)

	// Restart equivalence, against the whole randomized op space: a store
	// rebuilt from the persisted rows must match the model and be
	// byte-identical to the one that wrote them.
	restored := restoreFrom(t, persisted)
	m.check(t, restored)
	if !maps.Equal(dumpRows(s), dumpRows(restored)) {
		t.Fatal("restored store differs from the live store after the workload")
	}
	return stats
}

func randETag(rng *rand.Rand) []byte {
	etag := make([]byte, 16)
	for i := range etag {
		etag[i] = byte(rng.Uint64())
	}
	return etag
}
