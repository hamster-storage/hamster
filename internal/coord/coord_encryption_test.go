package coord_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/keys"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sim"
)

// The v0.7 pass-3 integration: the same simulated cluster as coord_test.go,
// with the encryption posture on. A PUT mints a per-object DEK, encrypts the
// body through the stream transform, wraps the DEK under the cluster KEK, and
// commits the wrapped DEK with the metadata; a GET unwraps and decrypts.
// Durability is still proven the hard way — by decoding the object out of the
// shard files on the surviving disks — but now the shards are ciphertext, so
// the decode also exercises unwrap.

// TestEncryptedPutGetRoundTrip: a healthy 6-node 4+2 cluster with encryption
// on. Every size commits an encrypted record, decodes back bit-identically off
// disk (with the key), and reads back over the network through the
// coordinator's decrypt path.
func TestEncryptedPutGetRoundTrip(t *testing.T) {
	c := newCluster(t, 1, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.encryptCluster()
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	for i, size := range []int{0, 1, 100 << 10, 1 << 20, 3<<20 + 777} {
		key := fmt.Sprintf("enc-%d", size)
		body := randomBody(uint64(i), size)
		if _, err := c.put(key, body); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}

		e, ok := c.entry(key)
		if !ok {
			t.Fatalf("%s: no entry", key)
		}
		if e.EncAlgorithm != meta.EncAES256GCM {
			t.Errorf("%s: EncAlgorithm %d, want AES256GCM", key, e.EncAlgorithm)
		}
		if len(e.WrappedDEK) != keys.WrappedLen {
			t.Errorf("%s: WrappedDEK len %d, want %d", key, len(e.WrappedDEK), keys.WrappedLen)
		}

		if got, err := c.readObject(key); err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s disk decode: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
		if got, err := c.get(key, 0, -1); err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s network get: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
		if size > 8192 {
			if got, err := c.get(key, 2500, 4000); err != nil || !bytes.Equal(got, body[2500:6500]) {
				t.Fatalf("%s range get: equal=%v err=%v", key, bytes.Equal(got, body[2500:6500]), err)
			}
		}
	}
}

// TestEncryptedConfidentialityOnDisk is the A/B proof that encryption reaches
// the disk: the same recognizable payload is written once before encryption is
// enabled and once after. The plaintext object's shard holds the marker
// verbatim; the encrypted object's shard does not — yet both read back.
func TestEncryptedConfidentialityOnDisk(t *testing.T) {
	c := newCluster(t, 2, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	marker := bytes.Repeat([]byte("TOPSECRET-HAMSTER"), 40000) // spans many stripes
	if _, err := c.put("plain", marker); err != nil {
		t.Fatalf("plaintext put: %v", err)
	}
	c.encryptCluster()
	if _, err := c.put("secret", marker); err != nil {
		t.Fatalf("encrypted put: %v", err)
	}

	needle := []byte("TOPSECRET-HAMSTER")
	if raw := c.rawShardBytes("plain", 0); !bytes.Contains(raw, needle) {
		t.Error("control failed: plaintext object's shard does not contain the marker")
	}
	if raw := c.rawShardBytes("secret", 0); bytes.Contains(raw, needle) {
		t.Error("encrypted object's shard contains the plaintext marker")
	}
	// Both still read back correctly — a mixed cluster of plaintext and
	// encrypted objects is normal.
	for _, key := range []string{"plain", "secret"} {
		if got, err := c.readObject(key); err != nil || !bytes.Equal(got, marker) {
			t.Fatalf("%s read back: equal=%v err=%v", key, bytes.Equal(got, marker), err)
		}
	}
	// The plaintext object carries no encryption fields; the encrypted one does.
	if e, _ := c.entry("plain"); e.EncAlgorithm != meta.EncNone || len(e.WrappedDEK) != 0 {
		t.Errorf("plaintext object recorded encryption: alg=%d dek=%d", e.EncAlgorithm, len(e.WrappedDEK))
	}
}

// TestEncryptedReadWithoutKEKRefused: an encrypted object cannot be read by a
// node that has lost its KEK. The read refuses loudly rather than serving
// ciphertext — the fail-closed rule (ADR-0021).
func TestEncryptedReadWithoutKEKRefused(t *testing.T) {
	c := newCluster(t, 3, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.encryptCluster()
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	body := randomBody(3, 1<<20)
	if _, err := c.put("locked", body); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Sanity: readable with the key.
	if got, err := c.get("locked", 0, -1); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("with key: equal=%v err=%v", bytes.Equal(got, body), err)
	}

	// The node loses its KEK (the key source became unavailable). The object
	// stays encrypted; the read must refuse.
	c.kek = keys.KEK{}
	if _, err := c.get("locked", 0, -1); err == nil {
		t.Fatal("read of an encrypted object with no KEK succeeded")
	}
}

// TestEncryptedDeterminism: the encrypted write path is deterministic under the
// simulator — the same seed replays the same DEK, wrapped DEK, version, and
// reconstructed bytes. (The DEK is the only random input; its entropy source is
// seeded.)
func TestEncryptedDeterminism(t *testing.T) {
	run := func() (meta.VersionEntry, []byte) {
		c := newCluster(t, 11, sim.NetConfig{
			MinLatency: time.Millisecond, MaxLatency: 25 * time.Millisecond,
			DropProb: 0.05, DuplicateProb: 0.05,
		}, 6, profile(t, "4+2"))
		c.encryptCluster()
		c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
		if _, err := c.put("replay", randomBody(11, 1<<20)); err != nil {
			t.Fatalf("put: %v", err)
		}
		e, _ := c.entry("replay")
		got, err := c.readObject("replay")
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return e, got
	}
	e1, got1 := run()
	e2, got2 := run()
	if e1.VersionID != e2.VersionID || !bytes.Equal(e1.WrappedDEK, e2.WrappedDEK) || !bytes.Equal(got1, got2) {
		t.Fatal("same seed, different encrypted run")
	}
	if len(e1.WrappedDEK) != keys.WrappedLen {
		t.Fatalf("WrappedDEK len %d", len(e1.WrappedDEK))
	}
}

// TestEncryptedRepairHealsLostShard: repair rebuilds a corrupted shard of an
// encrypted object from k surviving ciphertext shards — without ever touching
// the key. The object decrypts after the heal. This is the property that keeps
// storage-side work (repair, scrub, rebalance) key-free.
func TestEncryptedRepairHealsLostShard(t *testing.T) {
	c := newCluster(t, 34, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.encryptCluster()
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	body := randomBody(34, 2<<20)
	if _, err := c.put("rotting", body); err != nil {
		t.Fatalf("put: %v", err)
	}

	c.corruptShard("rotting", 1)
	c.corruptShard("rotting", 4)

	rep := c.sweep()
	if rep.RebuiltShards != 2 || len(rep.Unrepairable) != 0 || len(rep.Failed) != 0 {
		t.Fatalf("sweep: %+v", rep)
	}
	if rep = c.sweep(); rep.Healthy != rep.Objects {
		t.Fatalf("post-heal sweep not clean: %+v", rep)
	}
	if got, err := c.get("rotting", 0, -1); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("after heal: equal=%v err=%v", bytes.Equal(got, body), err)
	}
}

// TestKEKRotationRewrapsEveryObject: a master-key rotation rewraps every
// encrypted version's DEK from the old KEK to a new one (ADR-0032), metadata
// only — the shards never move. After the sweep every version names the new
// fingerprint, the rotation is closed, and the objects decrypt under the new
// key alone (the old one can be retired). A second sweep has nothing to do.
func TestKEKRotationRewrapsEveryObject(t *testing.T) {
	c := newCluster(t, 7, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.encryptCluster()
	oldFP := c.kek.Fingerprint().Uint64()
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	// Establish the cluster's current KEK fingerprint (what cluster encrypt does
	// on a real cluster), so the rotation has an old key to move from.
	c.propose(meta.SetEncryptionPosture{ProposedAtUnixMS: 2, Algorithm: meta.EncAES256GCM, KEKFingerprint: oldFP})

	bodies := map[string][]byte{}
	for i, size := range []int{0, 1, 100 << 10, 1 << 20, 3<<20 + 5} {
		key := fmt.Sprintf("obj-%d", i)
		body := randomBody(uint64(100+i), size)
		bodies[key] = body
		if _, err := c.put(key, body); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		if e, _ := c.entry(key); e.KEKFingerprint != oldFP {
			t.Fatalf("%s stamped %016x, want old %016x", key, e.KEKFingerprint, oldFP)
		}
	}

	// Load the new key, hand it to the keyring, and open the rotation.
	newKEK := c.mkKEK(0x80)
	newFP := newKEK.Fingerprint().Uint64()
	c.keyring[newFP] = newKEK
	c.propose(meta.BeginKEKRotation{ProposedAtUnixMS: 3, FromFingerprint: oldFP, ToFingerprint: newFP})

	rep, err := c.rewrap(c.leader())
	if err != nil {
		t.Fatalf("rewrap: %v", err)
	}
	if rep.Rewrapped != len(bodies) || rep.Remaining != 0 || !rep.Completed || len(rep.Failed) != 0 {
		t.Fatalf("rewrap report: %+v (want %d rewrapped, converged, completed)", rep, len(bodies))
	}

	// Every version now names the new key; the posture advanced and closed.
	for key := range bodies {
		if e, _ := c.entry(key); e.KEKFingerprint != newFP {
			t.Errorf("%s still on %016x, want new %016x", key, e.KEKFingerprint, newFP)
		}
	}

	// Retire the old key: the node now holds only the new one. Every object
	// still decrypts — the proof the rewrap preserved each DEK under the new KEK.
	c.kek = newKEK
	delete(c.keyring, oldFP)
	for key, body := range bodies {
		if got, err := c.readObject(key); err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s under new key: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
		if got, err := c.get(key, 0, -1); err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s network get under new key: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
	}

	// The rotation is closed: another sweep finds nothing to do.
	if _, err := c.rewrap(c.leader()); err != coord.ErrNoRotation {
		t.Fatalf("second sweep: %v, want ErrNoRotation", err)
	}
}

// TestKEKRotationResumes: a rotation that already rewrapped some versions (a
// crash, a prior partial sweep) resumes cleanly — the sweep skips the ones
// already on the new key, finishes the rest, and only then closes the rotation
// (ADR-0032). The convergence signal is the count of versions still on the old
// key reaching zero, not a single pass.
func TestKEKRotationResumes(t *testing.T) {
	c := newCluster(t, 8, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.encryptCluster()
	oldFP := c.kek.Fingerprint().Uint64()
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	c.propose(meta.SetEncryptionPosture{ProposedAtUnixMS: 2, Algorithm: meta.EncAES256GCM, KEKFingerprint: oldFP})

	for i := 0; i < 4; i++ {
		if _, err := c.put(fmt.Sprintf("k%d", i), randomBody(uint64(i), 64<<10)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	newKEK := c.mkKEK(0x80)
	newFP := newKEK.Fingerprint().Uint64()
	c.keyring[newFP] = newKEK
	c.propose(meta.BeginKEKRotation{ProposedAtUnixMS: 3, FromFingerprint: oldFP, ToFingerprint: newFP})

	// Simulate a partial rotation: hand-rewrap k0 under the new key, as a prior
	// interrupted sweep would have left it.
	e0, _ := c.entry("k0")
	dek, err := c.kek.Unwrap(e0.WrappedDEK)
	if err != nil {
		t.Fatal(err)
	}
	rewrapped, err := newKEK.Wrap(dek, e0.DataID[:keys.WrapNonceLen])
	if err != nil {
		t.Fatal(err)
	}
	c.propose(meta.RewrapDEK{ProposedAtUnixMS: 4, Bucket: bucket, Key: "k0", VersionID: e0.VersionID, WrappedDEK: rewrapped, KEKFingerprint: newFP})

	rep, err := c.rewrap(c.leader())
	if err != nil {
		t.Fatalf("rewrap: %v", err)
	}
	// One already on the new key, three rewrapped, converged and closed.
	if rep.AlreadyNew != 1 || rep.Rewrapped != 3 || rep.Remaining != 0 || !rep.Completed {
		t.Fatalf("resume report: %+v", rep)
	}
}

// TestEncryptedCrashMidPut: a coordinator dying mid-transfer of an encrypted
// object commits no metadata, leaving no half-written version; the retry
// encrypts, commits, and reads back.
func TestEncryptedCrashMidPut(t *testing.T) {
	c := newCluster(t, 6, sim.NetConfig{MinLatency: 5 * time.Millisecond, MaxLatency: 10 * time.Millisecond}, 6, profile(t, "4+2"))
	c.encryptCluster()
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	body := randomBody(6, 2<<20)
	lead := c.leader()
	c.worlds[lead].Loop.Post(func() {
		c.nodes[lead].co.Put(bucket, "casualty", body, coord.PutOptions{}, func(coord.PutResult, error) {
			t.Error("done fired for a coordinator that crashed mid-put")
		})
	})
	c.s.Run(time.Millisecond)
	c.crash(lead)
	c.s.Run(time.Second)
	c.s.Restart(lead)
	c.down[lead] = false

	if _, ok := c.entry("casualty"); ok {
		t.Fatal("a crashed coordinator's encrypted put committed metadata")
	}
	if _, err := c.put("casualty", body); err != nil {
		t.Fatalf("retry after crash: %v", err)
	}
	if got, err := c.get("casualty", 0, -1); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("retry read back: equal=%v err=%v", bytes.Equal(got, body), err)
	}
	e, _ := c.entry("casualty")
	if e.EncAlgorithm != meta.EncAES256GCM {
		t.Errorf("retry object not encrypted: alg=%d", e.EncAlgorithm)
	}
}
