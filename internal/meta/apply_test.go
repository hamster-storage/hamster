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

func TestObjectLockConfiguration(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("vault", true) // object lock enabled at creation
	if cfg, _ := e.s.GetBucket("vault"); !cfg.ObjectLockEnabled || cfg.Versioning != VersioningEnabled {
		t.Fatal("object-lock bucket should enable versioning")
	}

	// A default retention rule round-trips in days/years shape.
	if err := e.s.ApplySetObjectLockConfiguration(SetObjectLockConfiguration{
		ProposedAtUnixMS: e.tick(), Bucket: "vault", DefaultRetentionMode: RetentionCompliance, DefaultRetentionDays: 30,
	}); err != nil {
		t.Fatal(err)
	}
	cfg, _ := e.s.GetBucket("vault")
	if cfg.DefaultRetentionMode != RetentionCompliance || cfg.DefaultRetentionDays != 30 || cfg.DefaultRetentionYears != 0 {
		t.Fatalf("default retention = %v/%d/%d", cfg.DefaultRetentionMode, cfg.DefaultRetentionDays, cfg.DefaultRetentionYears)
	}

	// Exactly one of days/years; both (or neither, with a mode) is invalid.
	if err := e.s.ApplySetObjectLockConfiguration(SetObjectLockConfiguration{
		ProposedAtUnixMS: e.tick(), Bucket: "vault", DefaultRetentionMode: RetentionGovernance, DefaultRetentionDays: 1, DefaultRetentionYears: 1,
	}); !errors.Is(err, ErrInvalidRetention) {
		t.Fatalf("both days and years: %v", err)
	}

	// Clearing the default.
	if err := e.s.ApplySetObjectLockConfiguration(SetObjectLockConfiguration{
		ProposedAtUnixMS: e.tick(), Bucket: "vault", DefaultRetentionMode: RetentionNone,
	}); err != nil {
		t.Fatal(err)
	}
	if cfg, _ := e.s.GetBucket("vault"); cfg.DefaultRetentionMode != RetentionNone {
		t.Fatal("default retention not cleared")
	}

	// A bucket without object lock cannot take a lock configuration.
	e.mustCreateBucket("plain", false)
	if err := e.s.ApplySetObjectLockConfiguration(SetObjectLockConfiguration{
		ProposedAtUnixMS: e.tick(), Bucket: "plain", DefaultRetentionMode: RetentionGovernance, DefaultRetentionDays: 1,
	}); !errors.Is(err, ErrInvalidRetention) {
		t.Fatalf("lock config on non-lock bucket: %v", err)
	}
}

// TestEncryptionPostureEnableOnly: the cluster encryption posture turns on
// and stays on. EncNone is the default; enabling commits AES256GCM; setting
// it again is idempotent; a move back to EncNone is refused (ADR-0021), so a
// cluster never silently stops encrypting.
func TestEncryptionPostureEnableOnly(t *testing.T) {
	e := newEnv(t)

	if alg := e.s.EncryptionAlgorithm(); alg != EncNone {
		t.Fatalf("default posture %d, want EncNone", alg)
	}
	// An unknown algorithm is rejected.
	if err := e.s.ApplySetEncryptionPosture(SetEncryptionPosture{ProposedAtUnixMS: e.tick(), Algorithm: 99}); !errors.Is(err, ErrInvalidEncryption) {
		t.Fatalf("unknown algorithm: %v", err)
	}
	// Enable.
	if err := e.s.ApplySetEncryptionPosture(SetEncryptionPosture{ProposedAtUnixMS: e.tick(), Algorithm: EncAES256GCM}); err != nil {
		t.Fatal(err)
	}
	if alg := e.s.EncryptionAlgorithm(); alg != EncAES256GCM {
		t.Fatalf("after enable: %d", alg)
	}
	// Idempotent re-enable.
	if err := e.s.ApplySetEncryptionPosture(SetEncryptionPosture{ProposedAtUnixMS: e.tick(), Algorithm: EncAES256GCM}); err != nil {
		t.Fatalf("re-enable: %v", err)
	}
	// Disable is refused — the one-way ratchet.
	if err := e.s.ApplySetEncryptionPosture(SetEncryptionPosture{ProposedAtUnixMS: e.tick(), Algorithm: EncNone}); !errors.Is(err, ErrEncryptionDowngrade) {
		t.Fatalf("disable should be refused: %v", err)
	}
	if alg := e.s.EncryptionAlgorithm(); alg != EncAES256GCM {
		t.Fatal("posture changed after a refused disable")
	}
}

// putEncrypted commits an encrypted version with the given wrapped DEK,
// fingerprint, and optional COMPLIANCE lock — the shape master-key rotation
// rewraps.
func (e *env) putEncrypted(bucket, key string, wrapped []byte, fp uint64, until int64) VersionID {
	e.t.Helper()
	at := e.tick()
	res, err := e.s.ApplyPutObject(PutObject{
		ProposedAtUnixMS: at, Bucket: bucket, Key: key, VersionID: mintAt(at, e.rng),
		Size: 3, ETag: []byte{1, 2, 3}, EncAlgorithm: EncAES256GCM, WrappedDEK: wrapped, KEKFingerprint: fp,
		RetentionMode: func() RetentionMode {
			if until > 0 {
				return RetentionCompliance
			}
			return RetentionNone
		}(),
		RetainUntilUnixMS: until,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return res.VersionID
}

// TestKEKFingerprintEstablishAndGuard: enabling encryption establishes the
// cluster's current KEK fingerprint, and afterward a posture write carrying a
// different fingerprint — a node holding the wrong master key — is refused
// (ADR-0032), while the matching one stays idempotent.
func TestKEKFingerprintEstablishAndGuard(t *testing.T) {
	e := newEnv(t)
	const fpA, fpB = uint64(0xAAAA), uint64(0xBBBB)

	// Enable with fingerprint A: it becomes the established current.
	if err := e.s.ApplySetEncryptionPosture(SetEncryptionPosture{ProposedAtUnixMS: e.tick(), Algorithm: EncAES256GCM, KEKFingerprint: fpA}); err != nil {
		t.Fatal(err)
	}
	if got := e.s.EncryptionPosture().CurrentKEKFingerprint; got != fpA {
		t.Fatalf("current fingerprint %x, want %x", got, fpA)
	}
	// Re-affirm with A: idempotent.
	if err := e.s.ApplySetEncryptionPosture(SetEncryptionPosture{ProposedAtUnixMS: e.tick(), Algorithm: EncAES256GCM, KEKFingerprint: fpA}); err != nil {
		t.Fatalf("re-affirm A: %v", err)
	}
	// A different fingerprint is the split-key footgun: refused.
	if err := e.s.ApplySetEncryptionPosture(SetEncryptionPosture{ProposedAtUnixMS: e.tick(), Algorithm: EncAES256GCM, KEKFingerprint: fpB}); !errors.Is(err, ErrKEKMismatch) {
		t.Fatalf("mismatched key: got %v, want ErrKEKMismatch", err)
	}
	if got := e.s.EncryptionPosture().CurrentKEKFingerprint; got != fpA {
		t.Fatal("current fingerprint changed after a refused mismatch")
	}
}

// TestKEKRotationLifecycle walks a full rotation: begin, rewrap a version
// (COMPLIANCE-locked, to prove it stays lock- and byte-safe), complete, and
// the guards around each step (ADR-0032).
func TestKEKRotationLifecycle(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("vault", true) // object lock enabled
	const fpOld, fpNew, fpOther = uint64(0x1111), uint64(0x2222), uint64(0x3333)

	if err := e.s.ApplySetEncryptionPosture(SetEncryptionPosture{ProposedAtUnixMS: e.tick(), Algorithm: EncAES256GCM, KEKFingerprint: fpOld}); err != nil {
		t.Fatal(err)
	}
	until := e.now + 100*365*24*3600*1000 // far future
	vid := e.putEncrypted("vault", "k", []byte("OLD-WRAP"), fpOld, until)

	// Begin must name the real current key; a stale From is refused.
	if err := e.s.ApplyBeginKEKRotation(BeginKEKRotation{ProposedAtUnixMS: e.tick(), FromFingerprint: fpOther, ToFingerprint: fpNew}); !errors.Is(err, ErrKEKMismatch) {
		t.Fatalf("stale From: got %v, want ErrKEKMismatch", err)
	}
	// Open the rotation.
	if err := e.s.ApplyBeginKEKRotation(BeginKEKRotation{ProposedAtUnixMS: e.tick(), FromFingerprint: fpOld, ToFingerprint: fpNew}); err != nil {
		t.Fatal(err)
	}
	if got := e.s.EncryptionPosture().RotatingToKEKFingerprint; got != fpNew {
		t.Fatalf("rotating-to %x, want %x", got, fpNew)
	}
	// Re-begin to the same target is idempotent; a second, different target is refused.
	if err := e.s.ApplyBeginKEKRotation(BeginKEKRotation{ProposedAtUnixMS: e.tick(), FromFingerprint: fpOld, ToFingerprint: fpNew}); err != nil {
		t.Fatalf("idempotent re-begin: %v", err)
	}
	if err := e.s.ApplyBeginKEKRotation(BeginKEKRotation{ProposedAtUnixMS: e.tick(), FromFingerprint: fpOld, ToFingerprint: fpOther}); !errors.Is(err, ErrKEKMismatch) {
		t.Fatalf("second concurrent target: got %v, want ErrKEKMismatch", err)
	}

	// Rewrap the locked version: only WrappedDEK and the fingerprint change.
	before, _ := e.s.GetVersion("vault", "k", vid)
	if err := e.s.ApplyRewrapDEK(RewrapDEK{ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "k", VersionID: vid, WrappedDEK: []byte("NEW-WRAP"), KEKFingerprint: fpNew}); err != nil {
		t.Fatal(err)
	}
	after, _ := e.s.GetVersion("vault", "k", vid)
	if string(after.WrappedDEK) != "NEW-WRAP" || after.KEKFingerprint != fpNew {
		t.Fatalf("rewrap did not update the wrap: %q / %x", after.WrappedDEK, after.KEKFingerprint)
	}
	// COMPLIANCE-safe: lock, retention, bytes, and EC layout untouched.
	if after.RetentionMode != before.RetentionMode || after.RetainUntilUnixMS != before.RetainUntilUnixMS ||
		after.Size != before.Size || string(after.ETag) != string(before.ETag) || after.DataID != before.DataID {
		t.Fatal("rewrap changed a field other than the wrapped DEK")
	}
	// Idempotent: rewrapping to the same fingerprint is a no-op success.
	if err := e.s.ApplyRewrapDEK(RewrapDEK{ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "k", VersionID: vid, WrappedDEK: []byte("IGNORED"), KEKFingerprint: fpNew}); err != nil {
		t.Fatalf("idempotent rewrap: %v", err)
	}
	if again, _ := e.s.GetVersion("vault", "k", vid); string(again.WrappedDEK) != "NEW-WRAP" {
		t.Fatal("idempotent rewrap overwrote the wrap")
	}

	// Complete: a mismatched target is refused; the right one advances current.
	if err := e.s.ApplyCompleteKEKRotation(CompleteKEKRotation{ProposedAtUnixMS: e.tick(), ToFingerprint: fpOther}); !errors.Is(err, ErrKEKMismatch) {
		t.Fatalf("complete wrong target: got %v, want ErrKEKMismatch", err)
	}
	if err := e.s.ApplyCompleteKEKRotation(CompleteKEKRotation{ProposedAtUnixMS: e.tick(), ToFingerprint: fpNew}); err != nil {
		t.Fatal(err)
	}
	post := e.s.EncryptionPosture()
	if post.CurrentKEKFingerprint != fpNew || post.RotatingToKEKFingerprint != 0 {
		t.Fatalf("after complete: current %x rotating %x", post.CurrentKEKFingerprint, post.RotatingToKEKFingerprint)
	}
	// Completing again (already closed, on target) is idempotent.
	if err := e.s.ApplyCompleteKEKRotation(CompleteKEKRotation{ProposedAtUnixMS: e.tick(), ToFingerprint: fpNew}); err != nil {
		t.Fatalf("idempotent complete: %v", err)
	}
}

// TestKEKRotationRejections: rotation refuses a cluster that is not encrypting
// and a rewrap of a non-encrypted or missing version (ADR-0032).
func TestKEKRotationRejections(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)

	// Not encrypting: begin is refused.
	if err := e.s.ApplyBeginKEKRotation(BeginKEKRotation{ProposedAtUnixMS: e.tick(), FromFingerprint: 1, ToFingerprint: 2}); !errors.Is(err, ErrNotEncrypting) {
		t.Fatalf("begin without encryption: got %v, want ErrNotEncrypting", err)
	}

	if err := e.s.ApplySetEncryptionPosture(SetEncryptionPosture{ProposedAtUnixMS: e.tick(), Algorithm: EncAES256GCM, KEKFingerprint: 0x1111}); err != nil {
		t.Fatal(err)
	}
	if err := e.s.ApplyBeginKEKRotation(BeginKEKRotation{ProposedAtUnixMS: e.tick(), FromFingerprint: 0x1111, ToFingerprint: 0x2222}); err != nil {
		t.Fatal(err)
	}
	// Rewrap of a plaintext version is refused.
	plain := e.put("docs", "plain")
	if err := e.s.ApplyRewrapDEK(RewrapDEK{ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "plain", VersionID: plain.VersionID, WrappedDEK: []byte("x"), KEKFingerprint: 0x2222}); !errors.Is(err, ErrInvalidRewrap) {
		t.Fatalf("rewrap plaintext: got %v, want ErrInvalidRewrap", err)
	}
	// Rewrap of a missing version is refused.
	if err := e.s.ApplyRewrapDEK(RewrapDEK{ProposedAtUnixMS: e.tick(), Bucket: "docs", Key: "ghost", VersionID: plain.VersionID, WrappedDEK: []byte("x"), KEKFingerprint: 0x2222}); !errors.Is(err, ErrNoSuchVersion) {
		t.Fatalf("rewrap missing: got %v, want ErrNoSuchVersion", err)
	}
}

// TestTrustBundleCompareAndSet: the CA trust bundle installs generationally
// (ADR-0033) — first install is Version 1, each later one exactly +1, a stale
// or invalid generation refused — and a member's leaf-CA fingerprint updates in
// place.
func TestTrustBundleCompareAndSet(t *testing.T) {
	e := newEnv(t)
	const old, new = uint64(0xCA01), uint64(0xCA02)

	if _, ok := e.s.TrustBundle(); ok {
		t.Fatal("a fresh store should hold no trust bundle")
	}
	// A first install must be Version 1.
	if err := e.s.ApplySetTrustBundle(SetTrustBundle{ProposedAtUnixMS: e.tick(), Version: 2,
		CAs: []TrustedCA{{Fingerprint: old, CertPEM: []byte("-old-")}}, IssuerFingerprint: old}); !errors.Is(err, ErrStaleTrustBundle) {
		t.Fatalf("first install at v2: %v, want ErrStaleTrustBundle", err)
	}
	if err := e.s.ApplySetTrustBundle(SetTrustBundle{ProposedAtUnixMS: e.tick(), Version: 1,
		CAs: []TrustedCA{{Fingerprint: old, CertPEM: []byte("-old-")}}, IssuerFingerprint: old}); err != nil {
		t.Fatal(err)
	}
	// Issuer must be a trusted CA; an empty bundle is invalid.
	if err := e.s.ApplySetTrustBundle(SetTrustBundle{ProposedAtUnixMS: e.tick(), Version: 2,
		CAs: []TrustedCA{{Fingerprint: old, CertPEM: []byte("-old-")}}, IssuerFingerprint: new}); !errors.Is(err, ErrInvalidTrustBundle) {
		t.Fatalf("issuer not in bundle: %v, want ErrInvalidTrustBundle", err)
	}
	// Open the rotation: dual trust, issuer = new.
	if err := e.s.ApplySetTrustBundle(SetTrustBundle{ProposedAtUnixMS: e.tick(), Version: 2,
		CAs:               []TrustedCA{{Fingerprint: old, CertPEM: []byte("-old-")}, {Fingerprint: new, CertPEM: []byte("-new-")}},
		IssuerFingerprint: new}); err != nil {
		t.Fatal(err)
	}
	b, _ := e.s.TrustBundle()
	if b.Version != 2 || b.IssuerFingerprint != new || !b.HasCA(old) || !b.HasCA(new) {
		t.Fatalf("bundle after open: %+v", b)
	}

	// A member's leaf-CA fingerprint updates in place.
	if err := e.s.ApplyRegisterNode(RegisterNode{ProposedAtUnixMS: e.tick(), NodeID: "n1", LeafCAFingerprint: old}); err != nil {
		t.Fatal(err)
	}
	if rec, _ := e.s.Node("n1"); rec.LeafCAFingerprint != old {
		t.Fatalf("registered leaf CA %x, want %x", rec.LeafCAFingerprint, old)
	}
	if err := e.s.ApplySetNodeLeafCA(SetNodeLeafCA{ProposedAtUnixMS: e.tick(), NodeID: "n1", LeafCAFingerprint: new}); err != nil {
		t.Fatal(err)
	}
	if rec, _ := e.s.Node("n1"); rec.LeafCAFingerprint != new {
		t.Fatalf("after reissue leaf CA %x, want %x", rec.LeafCAFingerprint, new)
	}
	if err := e.s.ApplySetNodeLeafCA(SetNodeLeafCA{ProposedAtUnixMS: e.tick(), NodeID: "ghost", LeafCAFingerprint: new}); !errors.Is(err, ErrInvalidNode) {
		t.Fatalf("unknown node: %v, want ErrInvalidNode", err)
	}
}

// TestComplianceLockIsAbsolute is invariant 4 made executable: no path may
// destroy or weaken a COMPLIANCE-locked version, with or without a governance
// bypass, until its retention expires.
func TestComplianceLockIsAbsolute(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("vault", true) // object lock enabled
	at := e.tick()
	until := at + 100*365*24*3600*1000 // ~100 years out, far past every tick below
	res, err := e.s.ApplyPutObject(PutObject{
		ProposedAtUnixMS: at, Bucket: "vault", Key: "k", VersionID: mintAt(at, e.rng),
		Size: 3, ETag: []byte{1}, RetentionMode: RetentionCompliance, RetainUntilUnixMS: until,
	})
	if err != nil {
		t.Fatal(err)
	}
	vid := res.VersionID

	mustLock := func(name string, err error) {
		if !errors.Is(err, ErrObjectLocked) {
			t.Fatalf("%s: got %v, want ErrObjectLocked", name, err)
		}
	}
	_, dv := e.s.ApplyDeleteVersion(DeleteVersion{ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "k", VersionID: vid})
	mustLock("delete version", dv)
	_, dvb := e.s.ApplyDeleteVersion(DeleteVersion{ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "k", VersionID: vid, BypassGovernance: true})
	mustLock("delete version with bypass", dvb)
	mustLock("shorten", e.s.ApplyUpdateRetention(UpdateRetention{
		ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "k", VersionID: vid, Mode: RetentionCompliance, RetainUntilUnixMS: until - 1000}))
	mustLock("remove with bypass", e.s.ApplyUpdateRetention(UpdateRetention{
		ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "k", VersionID: vid, Mode: RetentionNone, BypassGovernance: true}))
	mustLock("downgrade to governance", e.s.ApplyUpdateRetention(UpdateRetention{
		ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "k", VersionID: vid, Mode: RetentionGovernance, RetainUntilUnixMS: until, BypassGovernance: true}))

	// The bucket cannot be deleted out from under the locked version either.
	if err := e.s.ApplyDeleteBucket(DeleteBucket{ProposedAtUnixMS: e.tick(), Bucket: "vault"}); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("delete bucket: got %v, want ErrBucketNotEmpty", err)
	}

	// After every assault, the version and its retention are untouched.
	got, ok := e.s.GetVersion("vault", "k", vid)
	if !ok || got.RetentionMode != RetentionCompliance || got.RetainUntilUnixMS != until {
		t.Fatalf("locked version changed: %+v ok=%v", got, ok)
	}

	// Strengthening (a later date) is the one mutation allowed — the lock forbids
	// only weakening.
	if err := e.s.ApplyUpdateRetention(UpdateRetention{
		ProposedAtUnixMS: e.tick(), Bucket: "vault", Key: "k", VersionID: vid, Mode: RetentionCompliance, RetainUntilUnixMS: until + 1000}); err != nil {
		t.Fatalf("strengthen compliance: %v", err)
	}
}

func TestListObjectVersions(t *testing.T) {
	e := newEnv(t)
	e.mustCreateBucket("docs", false)
	if err := e.s.ApplySetBucketVersioning(SetBucketVersioning{ProposedAtUnixMS: e.tick(), Bucket: "docs", State: VersioningEnabled}); err != nil {
		t.Fatal(err)
	}
	a1 := e.put("docs", "a")
	a2 := e.put("docs", "a")
	b1 := e.put("docs", "b")
	at := e.tick()
	del, err := e.s.ApplyDeleteObject(DeleteObject{ProposedAtUnixMS: at, Bucket: "docs", Key: "b", VersionID: mintAt(at, e.rng)})
	if err != nil {
		t.Fatal(err)
	}
	c1 := e.put("docs", "c")

	type want struct {
		key    string
		vid    VersionID
		latest bool
		marker bool
	}
	exp := []want{
		{"a", a2.VersionID, true, false},
		{"a", a1.VersionID, false, false},
		{"b", del.MarkerID, true, true},
		{"b", b1.VersionID, false, false},
		{"c", c1.VersionID, true, false},
	}
	all, trunc := e.s.ListObjectVersions("docs", "", "", VersionID{}, 100)
	if trunc || len(all) != len(exp) {
		t.Fatalf("got %d entries (trunc=%v), want %d", len(all), trunc, len(exp))
	}
	for i, w := range exp {
		g := all[i]
		if g.Key != w.key || g.Entry.VersionID != w.vid || g.IsLatest != w.latest || (g.Entry.Kind == KindDeleteMarker) != w.marker {
			t.Fatalf("entry %d = {%s latest=%v marker=%v}, want {%s latest=%v marker=%v}",
				i, g.Key, g.IsLatest, g.Entry.Kind == KindDeleteMarker, w.key, w.latest, w.marker)
		}
	}

	// Pagination two at a time, resuming after the last consumed entry.
	p1, t1 := e.s.ListObjectVersions("docs", "", "", VersionID{}, 2)
	if !t1 || len(p1) != 2 {
		t.Fatalf("page1 len=%d trunc=%v", len(p1), t1)
	}
	last1 := p1[1]
	p2, t2 := e.s.ListObjectVersions("docs", "", last1.Key, last1.Entry.VersionID, 2)
	if !t2 || len(p2) != 2 || !p2[0].IsLatest || p2[0].Entry.Kind != KindDeleteMarker {
		t.Fatalf("page2 len=%d trunc=%v latest=%v", len(p2), t2, p2[0].IsLatest)
	}
	last2 := p2[1]
	p3, t3 := e.s.ListObjectVersions("docs", "", last2.Key, last2.Entry.VersionID, 2)
	if t3 || len(p3) != 1 || p3[0].Key != "c" {
		t.Fatalf("page3 len=%d trunc=%v", len(p3), t3)
	}

	// Resuming into the middle of a key marks nothing latest there.
	pa, _ := e.s.ListObjectVersions("docs", "", "a", a2.VersionID, 1)
	if len(pa) != 1 || pa[0].Entry.VersionID != a1.VersionID || pa[0].IsLatest {
		t.Fatalf("mid-key resume = %+v", pa)
	}

	// Prefix filter.
	if pf, _ := e.s.ListObjectVersions("docs", "a", "", VersionID{}, 100); len(pf) != 2 {
		t.Fatalf("prefix a: %d entries, want 2", len(pf))
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
