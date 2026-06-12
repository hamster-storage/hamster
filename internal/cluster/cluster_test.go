package cluster

import (
	"net"
	"strings"
	"testing"
	"time"
)

// The cluster package test: a real three-node metadata cluster on
// loopback — real TLS, real sockets, real disks under t.TempDir() — grown
// from one init node through token-authenticated joins, observed through
// the status protocol. The distributed logic is proven under the
// simulation harness (internal/raftnode); this proves the production
// composition around it.

func init() {
	// Simulation-like speeds: a whole cluster lives in this process.
	tickInterval = 5 * time.Millisecond
	electionTicks = 10
	peerSyncEvery = 50 * time.Millisecond
}

// freeAddr reserves a loopback port and releases it for the node to bind.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// waitStatus polls a node's status until pred holds.
func waitStatus(t *testing.T, dataDir, addr, what string, pred func([]Member) bool) []Member {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ms, err := Status(dataDir, addr)
		if err == nil && pred(ms) {
			return ms
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s: last status %v (err %v)", what, ms, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func voters(ms []Member) int {
	v := 0
	for _, m := range ms {
		if !m.Learner {
			v++
		}
	}
	return v
}

func TestClusterGrowsByTokenJoin(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()

	// Node 1: init and run. A fresh single-voter cluster.
	if err := Init(d1, "testcluster", "n1", freeAddr(t), freeAddr(t), now); err != nil {
		t.Fatal(err)
	}
	n1, err := Run(d1)
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Stop()
	waitStatus(t, d1, "", "n1 to lead its own cluster", func(ms []Member) bool {
		return len(ms) == 1 && ms[0].Leader && ms[0].NodeID == "n1"
	})

	// Node 2: token, join, run, and full membership: caught up, promoted.
	tok, err := MintToken(d1, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(tok, "hamster-join-") {
		t.Fatalf("token %q has no recognizable prefix", tok)
	}
	if err := Join(d2, "n2", freeAddr(t), freeAddr(t), tok); err != nil {
		t.Fatal(err)
	}
	n2, err := Run(d2)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Stop()
	waitStatus(t, d1, "", "n2 to join and be promoted", func(ms []Member) bool {
		return len(ms) == 2 && voters(ms) == 2
	})

	// The token burned on use: a second join with it is refused.
	if err := Join(t.TempDir(), "nX", freeAddr(t), freeAddr(t), tok); err == nil {
		t.Fatal("a used join token was accepted")
	} else if !strings.Contains(err.Error(), "already-used") {
		t.Fatalf("used token refused for the wrong reason: %v", err)
	}

	// A duplicate node ID is refused (fresh token, same name).
	tokDup, err := MintToken(d1, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := Join(t.TempDir(), "n2", freeAddr(t), freeAddr(t), tokDup); err == nil {
		t.Fatal("a duplicate node ID was accepted")
	}

	// An expired token is refused.
	tokOld, err := MintToken(d1, -time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := Join(t.TempDir(), "n4", freeAddr(t), freeAddr(t), tokOld); err == nil {
		t.Fatal("an expired join token was accepted")
	}

	// Node 3 joins; three voters under the cap.
	tok3, err := MintToken(d1, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := Join(d3, "n3", freeAddr(t), freeAddr(t), tok3); err != nil {
		t.Fatal(err)
	}
	n3, err := Run(d3)
	if err != nil {
		t.Fatal(err)
	}
	defer n3.Stop()
	waitStatus(t, d1, "", "a full three-voter cluster", func(ms []Member) bool {
		return len(ms) == 3 && voters(ms) == 3
	})

	// Status answers identically from a non-issuer node, with its own
	// certificate, against its own listener.
	waitStatus(t, d2, "", "status served by n2", func(ms []Member) bool {
		return len(ms) == 3
	})

	// A restart rejoins from disk: same identity, same membership.
	n2.Stop()
	n2, err = Run(d2)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Stop()
	waitStatus(t, d2, "", "n2 back after restart", func(ms []Member) bool {
		return len(ms) == 3 && voters(ms) == 3
	})

	// Tokens come only from the CA holder.
	if _, err := MintToken(d2, time.Hour, now); err == nil {
		t.Fatal("a node without the CA key minted a token")
	}
}

// TestRecoverRebuildsSingleVoterCluster: a two-node cluster loses n2
// forever; recover rewrites the stopped n1 into a sole-voter cluster that
// runs, leads, and grows again with a fresh token.
func TestRecoverRebuildsSingleVoterCluster(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()

	if err := Init(d1, "rec", "n1", freeAddr(t), freeAddr(t), now); err != nil {
		t.Fatal(err)
	}
	n1, err := Run(d1)
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Stop()
	tok, err := MintToken(d1, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := Join(d2, "n2", freeAddr(t), freeAddr(t), tok); err != nil {
		t.Fatal(err)
	}
	n2, err := Run(d2)
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Stop()
	waitStatus(t, d1, "", "two voters before the disaster", func(ms []Member) bool {
		return len(ms) == 2 && voters(ms) == 2
	})

	// Recovery refuses a running node.
	if _, err := Recover(d1); err == nil {
		t.Fatal("recover ran against a live node")
	}

	// The disaster: n2 is gone forever, n1 stopped for offline recovery.
	n2.Stop()
	n1.Stop()
	sum, err := Recover(d1)
	if err != nil {
		t.Fatal(err)
	}
	if len(sum.Removed) != 1 || string(sum.Removed[0].Addr) != "n2" {
		t.Fatalf("recovery removed %v, want n2", sum.Removed)
	}

	n1b, err := Run(d1)
	if err != nil {
		t.Fatal(err)
	}
	defer n1b.Stop()
	waitStatus(t, d1, "", "the survivor leading alone", func(ms []Member) bool {
		return len(ms) == 1 && ms[0].NodeID == "n1" && ms[0].Leader && !ms[0].Learner
	})

	// And it grows again.
	tok3, err := MintToken(d1, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := Join(d3, "n3", freeAddr(t), freeAddr(t), tok3); err != nil {
		t.Fatal(err)
	}
	n3, err := Run(d3)
	if err != nil {
		t.Fatal(err)
	}
	defer n3.Stop()
	ms := waitStatus(t, d1, "", "the recovered cluster growing to two voters", func(ms []Member) bool {
		return len(ms) == 2 && voters(ms) == 2
	})
	// The dead member's Raft ID is never reissued.
	for _, m := range ms {
		if m.NodeID == "n3" && m.RaftID <= sum.Removed[0].ID {
			t.Fatalf("n3 got raft id %d, not above the removed member's %d", m.RaftID, sum.Removed[0].ID)
		}
	}
}

func TestTokenRoundTrip(t *testing.T) {
	tok := token{JoinAddr: "10.0.0.1:7947", CAHash: [32]byte{1, 2, 3}, ID: "abcd", Secret: []byte{9, 9, 9}}
	got, err := decodeToken(encodeToken(tok))
	if err != nil {
		t.Fatal(err)
	}
	if got.JoinAddr != tok.JoinAddr || got.CAHash != tok.CAHash || got.ID != tok.ID || string(got.Secret) != string(tok.Secret) {
		t.Fatalf("token round trip diverged: %+v vs %+v", got, tok)
	}
	if _, err := decodeToken("not-a-token"); err == nil {
		t.Fatal("garbage decoded as a token")
	}
}

func TestNodeConfigRoundTrip(t *testing.T) {
	cfg := NodeConfig{
		Cluster: "c", NodeID: "n2", RaftID: 2,
		ClusterAddr: "127.0.0.1:1", JoinAddr: "127.0.0.1:2", Join: true,
		Members:    []Member{{RaftID: 1, NodeID: "n1", Dial: "127.0.0.1:3"}, {RaftID: 2, NodeID: "n2", Dial: "127.0.0.1:1"}},
		NextRaftID: 0,
	}
	got, err := decodeConfig(encodeConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if got.Cluster != cfg.Cluster || got.NodeID != cfg.NodeID || got.RaftID != cfg.RaftID ||
		got.Join != cfg.Join || len(got.Members) != 2 || got.Members[1] != cfg.Members[1] {
		t.Fatalf("config round trip diverged: %+v vs %+v", got, cfg)
	}
}
