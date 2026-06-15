package meta

import (
	"errors"
	"math/rand/v2"
	"testing"
)

// env is a tiny test fixture: a store, a PRNG for minting IDs, and an
// advancing proposal clock.
type env struct {
	t   *testing.T
	s   *Store
	rng *rand.Rand
	now int64
}

func newEnv(t *testing.T) *env {
	return &env{t: t, s: NewStore(), rng: rand.New(rand.NewPCG(99, 0)), now: 1_750_000_000_000}
}

func (e *env) tick() int64 {
	e.now += 1_000
	return e.now
}

func (e *env) mustCreateBucket(name string, lock bool) {
	e.t.Helper()
	if err := e.s.ApplyCreateBucket(CreateBucket{ProposedAtUnixMS: e.tick(), Bucket: name, ObjectLockEnabled: lock}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) put(bucket, key string) PutResult {
	e.t.Helper()
	at := e.tick()
	res, err := e.s.ApplyPutObject(PutObject{
		ProposedAtUnixMS: at, Bucket: bucket, Key: key,
		VersionID: mintAt(at, e.rng), Size: 3, ETag: []byte{1, 2, 3},
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return res
}

func TestUnversionedPutReplaces(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	e.put("docs", "report")
	second := e.put("docs", "report")

	versions := e.s.ListVersions("docs", "report")
	if len(versions) != 1 {
		t.Fatalf("unversioned key holds %d versions, want 1", len(versions))
	}
	if !versions[0].NullVersion || versions[0].VersionID != second.VersionID {
		t.Fatal("surviving version is not the second write's null version")
	}
	cur, ok := e.s.Current("docs", "report")
	if !ok || cur.VersionID != second.VersionID {
		t.Fatal("current row does not point at the second write")
	}
}

func TestEnablingVersioningPreservesNullVersion(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	nullPut := e.put("docs", "report")
	if err := e.s.ApplySetBucketVersioning(SetBucketVersioning{ProposedAtUnixMS: e.tick(), Bucket: "docs", State: VersioningEnabled}); err != nil {
		t.Fatal(err)
	}
	newPut := e.put("docs", "report")

	versions := e.s.ListVersions("docs", "report")
	if len(versions) != 2 {
		t.Fatalf("got %d versions, want 2", len(versions))
	}
	if versions[0].VersionID != newPut.VersionID || versions[0].NullVersion {
		t.Fatal("newest version should be the post-enable write, not null")
	}
	if versions[1].VersionID != nullPut.VersionID || !versions[1].NullVersion {
		t.Fatal("the pre-enable null version was not preserved")
	}
}

func TestSuspendedPutReplacesOnlyNullVersion(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	if err := e.s.ApplySetBucketVersioning(SetBucketVersioning{ProposedAtUnixMS: e.tick(), Bucket: "docs", State: VersioningEnabled}); err != nil {
		t.Fatal(err)
	}
	e.put("docs", "report")
	e.put("docs", "report")
	if err := e.s.ApplySetBucketVersioning(SetBucketVersioning{ProposedAtUnixMS: e.tick(), Bucket: "docs", State: VersioningSuspended}); err != nil {
		t.Fatal(err)
	}
	e.put("docs", "report") // first null version
	final := e.put("docs", "report")

	versions := e.s.ListVersions("docs", "report")
	if len(versions) != 3 {
		t.Fatalf("got %d versions, want 3 (two enabled-era + one null)", len(versions))
	}
	if versions[0].VersionID != final.VersionID || !versions[0].NullVersion {
		t.Fatal("newest should be the final null version")
	}
}

func TestVersionedDeleteCreatesMarker(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", true) // lock-enabled: versioning on from creation
	put := e.put("docs", "report")

	at := e.tick()
	res, err := e.s.ApplyDeleteObject(DeleteObject{ProposedAtUnixMS: at, Bucket: "docs", Key: "report", VersionID: mintAt(at, e.rng)})
	if err != nil || !res.MarkerCreated {
		t.Fatalf("delete: %+v, %v", res, err)
	}
	if _, ok := e.s.Current("docs", "report"); ok {
		t.Fatal("current row survived a delete marker")
	}
	if got := e.s.ListObjects("docs", "", "", 0); len(got) != 0 {
		t.Fatalf("listing shows %d objects after delete", len(got))
	}

	// Destroying the marker resurrects the object as current.
	if _, err := e.s.ApplyDeleteVersion(DeleteVersion{ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "report", VersionID: res.MarkerID}); err != nil {
		t.Fatal(err)
	}
	cur, ok := e.s.Current("docs", "report")
	if !ok || cur.VersionID != put.VersionID {
		t.Fatal("removing the marker did not restore the object as current")
	}
}

func TestDeleteNewestVersionRecomputesCurrent(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", true)
	v1 := e.put("docs", "report")
	v2 := e.put("docs", "report")

	if _, err := e.s.ApplyDeleteVersion(DeleteVersion{ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "report", VersionID: v2.VersionID}); err != nil {
		t.Fatal(err)
	}
	cur, ok := e.s.Current("docs", "report")
	if !ok || cur.VersionID != v1.VersionID {
		t.Fatal("current did not fall back to the prior version")
	}
}

func TestClockSkewBump(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)

	// First write from a node with a fast clock.
	fast := e.now + 60_000
	first, err := e.s.ApplyPutObject(PutObject{ProposedAtUnixMS: fast, Bucket: "docs", Key: "k", VersionID: mintAt(fast, e.rng)})
	if err != nil {
		t.Fatal(err)
	}
	// Second write commits later but carries an earlier clock.
	slow := e.now
	minted := mintAt(slow, e.rng)
	second, err := e.s.ApplyPutObject(PutObject{ProposedAtUnixMS: slow, Bucket: "docs", Key: "k", VersionID: minted})
	if err != nil {
		t.Fatal(err)
	}
	if second.VersionID != first.VersionID.Next() {
		t.Fatalf("bump: got %v, want increment of %v", second.VersionID, first.VersionID)
	}
	cur, _ := e.s.Current("docs", "k")
	if cur.VersionID != second.VersionID {
		t.Fatal("commit order did not beat clock order: current is not the last write")
	}
	// The bump moves the version identity, never the data address: the
	// data was durably written under the minted ID before the commit.
	entry, ok := e.s.GetVersion("docs", "k", second.VersionID)
	if !ok || entry.DataID != minted {
		t.Fatalf("DataID %v, want the minted ID %v", entry.DataID, minted)
	}
}

func TestComplianceLockHasNoOverridePath(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("vault", true)
	at := e.tick()
	until := at + 1_000_000
	res, err := e.s.ApplyPutObject(PutObject{
		ProposedAtUnixMS: at, Bucket: "vault", Key: "audit.log", VersionID: mintAt(at, e.rng),
		RetentionMode: RetentionCompliance, RetainUntilUnixMS: until,
	})
	if err != nil {
		t.Fatal(err)
	}

	del := DeleteVersion{ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "audit.log", VersionID: res.VersionID}
	if _, err := e.s.ApplyDeleteVersion(del); !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("COMPLIANCE delete: %v, want ErrObjectLocked", err)
	}
	del.BypassGovernance = true // the governance bypass must mean nothing here
	if _, err := e.s.ApplyDeleteVersion(del); !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("COMPLIANCE delete with bypass: %v, want ErrObjectLocked", err)
	}

	shorten := UpdateRetention{
		ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "audit.log", VersionID: res.VersionID,
		Mode: RetentionCompliance, RetainUntilUnixMS: until - 1, BypassGovernance: true,
	}
	if err := e.s.ApplyUpdateRetention(shorten); !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("COMPLIANCE shorten: %v, want ErrObjectLocked", err)
	}
	weaken := shorten
	weaken.Mode = RetentionGovernance
	weaken.RetainUntilUnixMS = until + 1_000
	if err := e.s.ApplyUpdateRetention(weaken); !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("COMPLIANCE mode weaken: %v, want ErrObjectLocked", err)
	}

	extend := shorten
	extend.RetainUntilUnixMS = until + 1_000_000
	extend.BypassGovernance = false
	if err := e.s.ApplyUpdateRetention(extend); err != nil {
		t.Fatalf("COMPLIANCE extend should be allowed: %v", err)
	}

	// After expiry, the version is destroyable — retention ended, this is
	// not an override.
	expired := DeleteVersion{ProposedAtUnixMS: extend.RetainUntilUnixMS + 1, Bucket: "vault", Key: "audit.log", VersionID: res.VersionID}
	if _, err := e.s.ApplyDeleteVersion(expired); err != nil {
		t.Fatalf("post-expiry delete: %v", err)
	}
}

func TestGovernanceBypassAndLegalHold(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("vault", true)
	at := e.tick()
	res, err := e.s.ApplyPutObject(PutObject{
		ProposedAtUnixMS: at, Bucket: "vault", Key: "draft", VersionID: mintAt(at, e.rng),
		RetentionMode: RetentionGovernance, RetainUntilUnixMS: at + 1_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}

	del := DeleteVersion{ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "draft", VersionID: res.VersionID}
	if _, err := e.s.ApplyDeleteVersion(del); !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("GOVERNANCE delete without bypass: %v, want ErrObjectLocked", err)
	}

	if err := e.s.ApplyUpdateLegalHold(UpdateLegalHold{ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "draft", VersionID: res.VersionID, Hold: true}); err != nil {
		t.Fatal(err)
	}
	del.BypassGovernance = true
	if _, err := e.s.ApplyDeleteVersion(del); !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("legal hold must block even a governance bypass: %v", err)
	}
	if err := e.s.ApplyUpdateLegalHold(UpdateLegalHold{ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "draft", VersionID: res.VersionID, Hold: false}); err != nil {
		t.Fatal(err)
	}
	if res, err := e.s.ApplyDeleteVersion(del); err != nil || !res.Removed {
		t.Fatalf("governance bypass after hold release: %+v, %v", res, err)
	}
}

func TestBucketRules(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	if err := e.s.ApplyCreateBucket(CreateBucket{ProposedAtUnixMS: e.tick(), Bucket: "docs"}); !errors.Is(err, ErrBucketExists) {
		t.Fatalf("duplicate create: %v", err)
	}
	if err := e.s.ApplyCreateBucket(CreateBucket{ProposedAtUnixMS: e.tick(), Bucket: "x"}); !errors.Is(err, ErrInvalidBucketName) {
		t.Fatalf("short name: %v", err)
	}

	e.mustCreateBucket("vault", true)
	if cfg, _ := e.s.GetBucket("vault"); cfg.Versioning != VersioningEnabled {
		t.Fatal("lock-enabled bucket did not start with versioning enabled")
	}
	if err := e.s.ApplySetBucketVersioning(SetBucketVersioning{ProposedAtUnixMS: e.tick(), Bucket: "vault", State: VersioningSuspended}); !errors.Is(err, ErrInvalidVersioningState) {
		t.Fatalf("suspending a lock-enabled bucket: %v", err)
	}

	// A bare delete marker keeps a bucket non-empty.
	e.mustCreateBucket("hist", false)
	if err := e.s.ApplySetBucketVersioning(SetBucketVersioning{ProposedAtUnixMS: e.tick(), Bucket: "hist", State: VersioningEnabled}); err != nil {
		t.Fatal(err)
	}
	at := e.tick()
	res, err := e.s.ApplyDeleteObject(DeleteObject{ProposedAtUnixMS: at, Bucket: "hist", Key: "ghost", VersionID: mintAt(at, e.rng)})
	if err != nil || !res.MarkerCreated {
		t.Fatalf("marker on absent key: %+v, %v", res, err)
	}
	if err := e.s.ApplyDeleteBucket(DeleteBucket{ProposedAtUnixMS: e.tick(), Bucket: "hist"}); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("delete of marker-bearing bucket: %v", err)
	}
	if _, err := e.s.ApplyDeleteVersion(DeleteVersion{ProposedAtUnixMS: e.tick(), Bucket: "hist", Key: "ghost", VersionID: res.MarkerID}); err != nil {
		t.Fatal(err)
	}
	if err := e.s.ApplyDeleteBucket(DeleteBucket{ProposedAtUnixMS: e.tick(), Bucket: "hist"}); err != nil {
		t.Fatalf("delete of emptied bucket: %v", err)
	}
}

func TestApplyRejectsBadInputs(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	at := e.tick()

	if _, err := e.s.ApplyPutObject(PutObject{ProposedAtUnixMS: at, Bucket: "docs", Key: "a\x00b", VersionID: mintAt(at, e.rng)}); !errors.Is(err, ErrInvalidObjectKey) {
		t.Fatalf("NUL key: %v", err)
	}
	if _, err := e.s.ApplyPutObject(PutObject{ProposedAtUnixMS: at, Bucket: "nope", Key: "k", VersionID: mintAt(at, e.rng)}); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("missing bucket: %v", err)
	}
	// Retention on a bucket without object lock.
	if _, err := e.s.ApplyPutObject(PutObject{
		ProposedAtUnixMS: at, Bucket: "docs", Key: "k", VersionID: mintAt(at, e.rng),
		RetentionMode: RetentionCompliance, RetainUntilUnixMS: at + 1000,
	}); !errors.Is(err, ErrInvalidRetention) {
		t.Fatalf("retention on lockless bucket: %v", err)
	}
	// Retain-until in the past.
	e.mustCreateBucket("vault", true)
	if _, err := e.s.ApplyPutObject(PutObject{
		ProposedAtUnixMS: at, Bucket: "vault", Key: "k", VersionID: mintAt(at, e.rng),
		RetentionMode: RetentionCompliance, RetainUntilUnixMS: at - 1,
	}); !errors.Is(err, ErrInvalidRetention) {
		t.Fatalf("past retain-until: %v", err)
	}
}

func TestListObjects(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	for _, k := range []string{"a", "photos/1.jpg", "photos/2.jpg", "z"} {
		e.put("docs", k)
	}

	all := e.s.ListObjects("docs", "", "", 0)
	if got := keysOf(all); !equal(got, []string{"a", "photos/1.jpg", "photos/2.jpg", "z"}) {
		t.Fatalf("full listing: %v", got)
	}
	photos := e.s.ListObjects("docs", "photos/", "", 0)
	if got := keysOf(photos); !equal(got, []string{"photos/1.jpg", "photos/2.jpg"}) {
		t.Fatalf("prefix listing: %v", got)
	}
	after := e.s.ListObjects("docs", "", "photos/1.jpg", 0)
	if got := keysOf(after); !equal(got, []string{"photos/2.jpg", "z"}) {
		t.Fatalf("startAfter listing: %v", got)
	}
	capped := e.s.ListObjects("docs", "", "", 2)
	if got := keysOf(capped); !equal(got, []string{"a", "photos/1.jpg"}) {
		t.Fatalf("capped listing: %v", got)
	}
}

func keysOf(ls []ObjectListing) []string {
	out := make([]string, len(ls))
	for i, l := range ls {
		out[i] = l.Key
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestApplyReEncodeObject: re-encode rewrites only the EC layout — DataID, the
// shard counts, and ShardChecksums — leaving content (Size, ETag, checksum) and
// the object lock untouched, even on a COMPLIANCE-locked version. It is a
// physical re-representation, not a content edit (ADR-0015).
func TestApplyReEncodeObject(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", true) // lock-enabled → versioning enabled
	at := e.tick()
	res, err := e.s.ApplyPutObject(PutObject{
		ProposedAtUnixMS: at, Bucket: "docs", Key: "report", VersionID: mintAt(at, e.rng),
		Size: 12345, ETag: []byte{0xE1}, ObjectChecksum: []byte{0xC1},
		Partition: 7, ECDataShards: 4, ECParityShards: 2,
		ShardChecksums:    [][]byte{{1}, {2}, {3}, {4}, {5}, {6}},
		RetentionMode:     RetentionCompliance,
		RetainUntilUnixMS: at + 1_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	vid := res.VersionID

	newDID := mintAt(e.tick(), e.rng)
	if err := e.s.ApplyReEncodeObject(ReEncodeObject{
		ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "report", VersionID: vid,
		DataID: newDID, ECDataShards: 3, ECParityShards: 2,
		ShardChecksums: [][]byte{{7}, {8}, {9}, {10}, {11}},
	}); err != nil {
		t.Fatalf("re-encode a COMPLIANCE-locked object: %v", err)
	}

	got, ok := e.s.GetVersion("docs", "report", vid)
	if !ok {
		t.Fatal("version gone after re-encode")
	}
	// EC layout moved to the new profile.
	if got.ECDataShards != 3 || got.ECParityShards != 2 || got.DataID != newDID || len(got.ShardChecksums) != 5 {
		t.Fatalf("EC fields not re-encoded: %+v", got)
	}
	// Content and lock untouched.
	if got.Size != 12345 || string(got.ETag) != "\xE1" || string(got.ObjectChecksum) != "\xC1" {
		t.Fatalf("re-encode changed content fields: %+v", got)
	}
	if got.RetentionMode != RetentionCompliance || got.RetainUntilUnixMS != at+1_000_000 {
		t.Fatalf("re-encode disturbed the object lock: %+v", got)
	}

	// A missing version, and a checksum count that disagrees with k+m, are refused.
	if err := e.s.ApplyReEncodeObject(ReEncodeObject{Bucket: "docs", Key: "report",
		VersionID: VersionID{0xFF}, ECDataShards: 1, ShardChecksums: [][]byte{{1}}}); !errors.Is(err, ErrNoSuchVersion) {
		t.Fatalf("re-encode of a missing version: %v", err)
	}
	if err := e.s.ApplyReEncodeObject(ReEncodeObject{Bucket: "docs", Key: "report",
		VersionID: vid, ECDataShards: 3, ECParityShards: 2, ShardChecksums: [][]byte{{1}}}); !errors.Is(err, ErrInvalidReEncode) {
		t.Fatalf("re-encode with mismatched checksum count: %v", err)
	}
}
