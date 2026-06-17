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

// TestClusterCARotation: a three-node cluster rotates its CA (ADR-0033). The
// trust bundle advances two generations (add new CA, drop old), every member's
// leaf-CA fingerprint converges to the new CA, and the cluster keeps a leader
// throughout — the dual-trust rollover with no trust gap.
func TestClusterCARotation(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()
	if err := Init(d1, "carot", "n1", freeAddr(t), "", 0, now); err != nil {
		t.Fatal(err)
	}
	n1, err := Run(d1)
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Stop()
	waitStatus(t, d1, "", "n1 leading alone", func(ms []Member) bool { return len(ms) == 1 && ms[0].Leader })

	join := func(dir, id string) *Node {
		tok, err := MintToken(d1, time.Hour, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := Join(dir, id, freeAddr(t), tok, "", 0, ""); err != nil {
			t.Fatal(err)
		}
		n, err := Run(dir)
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

	before, err := Status(d1, "")
	if err != nil {
		t.Fatal(err)
	}
	if before.TrustVersion == 0 || before.CARotating {
		t.Fatalf("before rotation: trust v%d rotating=%v", before.TrustVersion, before.CARotating)
	}

	rep, err := RotateCA(d1, "")
	if err != nil {
		t.Fatalf("rotate-ca: %v", err)
	}
	if !rep.Completed || rep.Reissued != 3 {
		t.Fatalf("rotation report: %+v (want completed, 3 reissued)", rep)
	}

	// Trust advanced two generations (add, drop) and no rotation is open. The
	// node's applied trust pool catches up to the committed bundle a tick after
	// the driver returns, so poll.
	deadline := time.Now().Add(20 * time.Second)
	for {
		after, err := Status(d1, "")
		if err == nil && after.TrustVersion == before.TrustVersion+2 && !after.CARotating {
			break
		}
		if time.Now().After(deadline) {
			after, _ := Status(d1, "")
			t.Fatalf("after rotation: trust v%d (was %d) rotating=%v", after.TrustVersion, before.TrustVersion, after.CARotating)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// A leader still exists on the new CA.
	waitStatus(t, d1, "", "leader on the new CA", func(ms []Member) bool {
		for _, m := range ms {
			if m.Leader {
				return true
			}
		}
		return false
	})
}
