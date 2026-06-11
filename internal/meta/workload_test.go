package meta

import (
	"fmt"
	"math/rand/v2"
	"slices"
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

		case c < 48:
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

		case c < 58:
			p := DeleteObject{ProposedAtUnixMS: at, Bucket: bucket(), Key: key(), VersionID: mint()}
			res, err := s.ApplyDeleteObject(p)
			m.deleteObject(t, p, res, err)
			record("delete %s/%s: %+v %v", p.Bucket, p.Key, res, err)

		case c < 72:
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

		case c < 82:
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

		case c < 88:
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
	return stats
}

func randETag(rng *rand.Rand) []byte {
	etag := make([]byte, 16)
	for i := range etag {
		etag[i] = byte(rng.Uint64())
	}
	return etag
}
