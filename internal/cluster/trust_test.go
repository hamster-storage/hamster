package cluster

import (
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
)

// TestTrustBundleSeeded: once a leader exists, the founding CA is installed as
// the first trust-bundle generation (ADR-0033), and the node's live trust pool
// tracks it — the foundation a CA rotation extends. Every node builds trust
// from the replicated bundle, not only its boot ca.pem.
func TestTrustBundleSeeded(t *testing.T) {
	now := time.Now()
	d1 := t.TempDir()
	if err := Init(d1, "trust", "n1", freeAddr(t), "", 0, now); err != nil {
		t.Fatal(err)
	}
	n1, err := Run(d1)
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Stop()
	waitStatus(t, d1, "", "n1 leading alone", func(ms []Member) bool { return len(ms) == 1 && ms[0].Leader })

	deadline := time.Now().Add(20 * time.Second)
	for {
		got, _ := onLoop(n1, 5*time.Second, func() struct {
			b   meta.TrustBundle
			ok  bool
			ver uint64
		} {
			b, ok := n1.raft.Store().TrustBundle()
			return struct {
				b   meta.TrustBundle
				ok  bool
				ver uint64
			}{b, ok, n1.trustVersion}
		})
		if got.ok && len(got.b.CAs) == 1 && got.b.IssuerFingerprint != 0 &&
			got.b.IssuerFingerprint == got.b.CAs[0].Fingerprint && got.ver == got.b.Version {
			return // seeded, and the live trust pool tracks it
		}
		if time.Now().After(deadline) {
			t.Fatalf("trust bundle not seeded / not tracked: %+v", got)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
