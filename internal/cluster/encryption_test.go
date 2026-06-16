package cluster

import (
	"strings"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/keys"
)

// testKEK builds a fixed cluster KEK for the encryption tests.
func testKEK(t *testing.T) keys.KEK {
	t.Helper()
	m := make([]byte, keys.KEKLen)
	for i := range m {
		m[i] = byte(i + 1)
	}
	k, err := keys.LoadKEK(m)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// waitEncryption polls a node's status until its reported encryption posture
// equals want.
func waitEncryption(t *testing.T, dataDir, what, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		report, err := Status(dataDir, "")
		if err == nil && report.Encryption == want {
			return
		}
		if time.Now().After(deadline) {
			last := "?"
			if r, e := Status(dataDir, ""); e == nil {
				last = "'" + r.Encryption + "'"
			}
			t.Fatalf("waiting for %s: encryption %s, want %q", what, last, want)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestClusterEncryptionPosture: a cluster founded with master keys turns
// encryption on with `cluster encrypt`, every node reports it, the posture is a
// committed fact that survives a node restart (and the node reloads its KEK from
// its key source), and it cannot be disabled — the enable-only ratchet.
func TestClusterEncryptionPosture(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()
	kek := testKEK(t)

	if err := Init(d1, "enctest", "n1", freeAddr(t), "", 0, now); err != nil {
		t.Fatal(err)
	}
	n1, err := Run(d1, WithMasterKey(kek))
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Stop()
	waitStatus(t, d1, "", "n1 leading alone", func(ms []Member) bool {
		return len(ms) == 1 && ms[0].Leader
	})

	join := func(dir, id string) *Node {
		tok, err := MintToken(d1, time.Hour, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := Join(dir, id, freeAddr(t), tok, "", 0, ""); err != nil {
			t.Fatal(err)
		}
		n, err := Run(dir, WithMasterKey(kek))
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	n2 := join(d2, "n2")
	defer n2.Stop()
	n3 := join(d3, "n3")
	defer n3.Stop()
	waitStatus(t, d1, "", "three members", func(ms []Member) bool { return len(ms) == 3 })

	// Off until enabled.
	if r, err := Status(d1, ""); err != nil || r.Encryption != "" {
		t.Fatalf("initial posture %q (err %v), want off", r.Encryption, err)
	}

	// Enable from a non-leader: Encrypt follows the redirect to the leader.
	label, err := Encrypt(d2, "")
	if err != nil {
		t.Fatalf("encrypt via a non-leader: %v", err)
	}
	if label != "AES256GCM" {
		t.Fatalf("enable returned %q", label)
	}
	// Every node reports it — a replicated, committed fact.
	for _, d := range []string{d1, d2, d3} {
		waitEncryption(t, d, "posture replicated", "AES256GCM")
	}

	// Re-enabling is idempotent, not an error.
	if _, err := Encrypt(d1, ""); err != nil {
		t.Fatalf("re-enable: %v", err)
	}

	// Restart n3: the posture is durable (replicated store), and the node reloads
	// its KEK from its key source on boot.
	n3.Stop()
	n3b, err := Run(d3, WithMasterKey(kek))
	if err != nil {
		t.Fatalf("restart n3: %v", err)
	}
	defer n3b.Stop()
	waitEncryption(t, d3, "posture survives restart", "AES256GCM")
}

// TestClusterEncryptRefusedWithoutKey: enabling encryption on a leader that has
// no master key loaded is refused — the footgun guard, so a cluster never turns
// on encryption it then cannot perform.
func TestClusterEncryptRefusedWithoutKey(t *testing.T) {
	now := time.Now()
	d1 := t.TempDir()
	if err := Init(d1, "nokey", "n1", freeAddr(t), "", 0, now); err != nil {
		t.Fatal(err)
	}
	// No WithMasterKey: the node holds no KEK.
	n1, err := Run(d1)
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Stop()
	waitStatus(t, d1, "", "n1 leading alone", func(ms []Member) bool {
		return len(ms) == 1 && ms[0].Leader
	})

	_, err = Encrypt(d1, "")
	if err == nil || !strings.Contains(err.Error(), "master key") {
		t.Fatalf("encrypt without a key: %v, want a 'master key' refusal", err)
	}
	// And the posture stayed off.
	if r, _ := Status(d1, ""); r.Encryption != "" {
		t.Fatalf("posture %q after a refused enable, want off", r.Encryption)
	}
}
