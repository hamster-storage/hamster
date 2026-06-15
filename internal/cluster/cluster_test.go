package cluster

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
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

// waitLayout polls a node's committed cluster layout — read on its loop, so it
// reflects what this replica has applied — until pred holds. Same-package
// access; read the leader for the freshest view.
func waitLayout(t *testing.T, n *Node, what string, pred func(meta.ClusterLayout) bool) meta.ClusterLayout {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var cl meta.ClusterLayout
		var ok bool
		done := make(chan struct{})
		n.loop.Post(func() {
			cl, ok = n.raft.Store().ClusterLayout()
			close(done)
		})
		<-done
		if ok && pred(cl) {
			return cl
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s: layout %+v (ok %v)", what, cl, ok)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func nodeDraining(cl meta.ClusterLayout, id string) bool {
	for _, m := range cl.EffectiveNodes() {
		if m.ID == id {
			return m.Draining
		}
	}
	return false
}

func anyDraining(cl meta.ClusterLayout) bool {
	for _, m := range cl.EffectiveNodes() {
		if m.Draining {
			return true
		}
	}
	return false
}

func TestClusterGrowsByTokenJoin(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()

	// Node 1: init and run. A fresh single-voter cluster.
	if err := Init(d1, "testcluster", "n1", freeAddr(t), "", 0, now); err != nil {
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
	if err := Join(d2, "n2", freeAddr(t), tok, "", 0); err != nil {
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
	if err := Join(t.TempDir(), "nX", freeAddr(t), tok, "", 0); err == nil {
		t.Fatal("a used join token was accepted")
	} else if !strings.Contains(err.Error(), "already-used") {
		t.Fatalf("used token refused for the wrong reason: %v", err)
	}

	// A duplicate node ID is refused (fresh token, same name).
	tokDup, err := MintToken(d1, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := Join(t.TempDir(), "n2", freeAddr(t), tokDup, "", 0); err == nil {
		t.Fatal("a duplicate node ID was accepted")
	}

	// An expired token is refused.
	tokOld, err := MintToken(d1, -time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := Join(t.TempDir(), "n4", freeAddr(t), tokOld, "", 0); err == nil {
		t.Fatal("an expired join token was accepted")
	}

	// Node 3 joins; three voters under the cap.
	tok3, err := MintToken(d1, time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := Join(d3, "n3", freeAddr(t), tok3, "", 0); err != nil {
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

// nodeRecords snapshots a node's replicated member registry (ADR-0016) off
// its loop — the s/node/* rows reconcileLayout composes the layout from.
func nodeRecords(n *Node) map[string]meta.NodeRecord {
	done := make(chan map[string]meta.NodeRecord, 1)
	n.loop.Post(func() {
		out := map[string]meta.NodeRecord{}
		for _, r := range n.raft.Store().Nodes() {
			out[r.NodeID] = r
		}
		done <- out
	})
	select {
	case m := <-done:
		return m
	case <-time.After(5 * time.Second):
		return nil
	}
}

// TestClusterNodeRegistryReplicated proves the member registry (the
// failure-domain/capacity labels) is replicated, not issuer-local: every
// node's own committed store holds a NodeRecord for every member, with the
// right labels. Before this, the registry lived only on the issuer's disk, so
// only the issuer could compose a complete layout; now any leader can — which
// is what this replication buys. n1 (issuer) zone za cap 3, n2 zone zb cap 1,
// n3 zone zc cap 2.
func TestClusterNodeRegistryReplicated(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()

	if err := Init(d1, "regtest", "n1", freeAddr(t), "za", 3, now); err != nil {
		t.Fatal(err)
	}
	n1, err := Run(d1)
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Stop()
	waitStatus(t, d1, "", "n1 leading alone", func(ms []Member) bool {
		return len(ms) == 1 && ms[0].Leader
	})

	join := func(dir, id, zone string, capacity uint32) *Node {
		tok, err := MintToken(d1, time.Hour, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := Join(dir, id, freeAddr(t), tok, zone, capacity); err != nil {
			t.Fatal(err)
		}
		n, err := Run(dir)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	n2 := join(d2, "n2", "zb", 1)
	defer n2.Stop()
	n3 := join(d3, "n3", "zc", 2)
	defer n3.Stop()

	waitStatus(t, d1, "", "three members", func(ms []Member) bool { return len(ms) == 3 })

	wantZone := map[string]string{"n1": "za", "n2": "zb", "n3": "zc"}
	wantCap := map[string]uint32{"n1": 3, "n2": 1, "n3": 2}
	// Each node — issuer and non-issuers alike — must converge to a registry
	// holding all three members with the right labels.
	deadline := time.Now().Add(30 * time.Second)
	for _, n := range []*Node{n1, n2, n3} {
		for {
			recs := nodeRecords(n)
			ok := len(recs) == 3
			for id, z := range wantZone {
				r, present := recs[id]
				if !present || r.Zone != z || r.Capacity != wantCap[id] {
					ok = false
				}
			}
			if ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %s registry did not converge: %+v", n.cfg.NodeID, recs)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
}

// TestClusterNodeDraining proves the operator drain command flows end to end:
// `cluster drain` issued against a non-leader redirects to the leader, the
// SetNodeDraining proposal commits, the leader's reconcile folds it into the
// cluster layout, and every node's status reports the member as draining (a
// committed fact, unlike the local liveness view). Undrain reverses it.
// Placement avoidance itself is proven in internal/place; this proves the
// control RPC and the cluster wiring that feeds it.
func TestClusterNodeDraining(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()

	if err := Init(d1, "draintest", "n1", freeAddr(t), "", 0, now); err != nil {
		t.Fatal(err)
	}
	n1, err := Run(d1)
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
		if err := Join(dir, id, freeAddr(t), tok, "", 0); err != nil {
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

	// A settled layout before the drain: three active members, no transition.
	// n1 led from init, so its store holds the freshest committed layout.
	settled := waitLayout(t, n1, "settled layout, no transition", func(cl meta.ClusterLayout) bool {
		return len(cl.Previous) == 0 && len(cl.EffectiveNodes()) == 3 && !anyDraining(cl)
	})

	// Drain n3, issuing the command from n2 (which is not the leader): Drain
	// must follow the redirect to the leader and commit.
	if err := Drain(d2, "", "n3", true); err != nil {
		t.Fatalf("drain via a non-leader: %v", err)
	}

	// Every node reports n3 draining (and only n3) — a committed fact, so the
	// view is the same whichever node answers.
	for _, d := range []string{d1, d2, d3} {
		waitStatus(t, d, "", "n3 draining", func(ms []Member) bool {
			for _, m := range ms {
				if m.Draining != (m.NodeID == "n3") {
					return false
				}
			}
			return len(ms) == 3
		})
	}

	// Draining is a subtractive change, so the leader opens a layout transition
	// and — with no data to migrate here — closes it the moment a repair sweep
	// converges. Two layout installs (open carrying Previous, then close
	// dropping it), so the version advances by two and the end state carries no
	// transition: proof the open/close lifecycle ran, not a bare swap.
	closed := waitLayout(t, n1, "transition opened then closed", func(cl meta.ClusterLayout) bool {
		return len(cl.Previous) == 0 && nodeDraining(cl, "n3")
	})
	if closed.Version < settled.Version+2 {
		t.Fatalf("drain should open then close a transition (version +2); got %d from %d", closed.Version, settled.Version)
	}

	// One node drains at a time (the single-pair model): a second drain is
	// refused while n3 is draining, with a message that names the conflict.
	if err := Drain(d2, "", "n2", true); err == nil {
		t.Fatal("a second concurrent drain should be refused")
	} else if !strings.Contains(err.Error(), "n3") && !strings.Contains(err.Error(), "transition") {
		t.Fatalf("refusal should name the in-flight drain: %v", err)
	}

	// Undrain reverses it: no member draining.
	if err := Drain(d2, "", "n3", false); err != nil {
		t.Fatalf("undrain: %v", err)
	}
	waitStatus(t, d1, "", "no member draining", func(ms []Member) bool {
		for _, m := range ms {
			if m.Draining {
				return false
			}
		}
		return len(ms) == 3
	})

	// With the previous drain cleared and its transition closed, a fresh drain
	// is allowed again — the guard gates concurrent drains, not all drains.
	waitLayout(t, n1, "no transition after undrain", func(cl meta.ClusterLayout) bool {
		return len(cl.Previous) == 0 && !anyDraining(cl)
	})
	if err := Drain(d2, "", "n2", true); err != nil {
		t.Fatalf("drain after the previous one cleared: %v", err)
	}
}

// TestRecoverRebuildsSingleVoterCluster: a two-node cluster loses n2
// forever; recover rewrites the stopped n1 into a sole-voter cluster that
// runs, leads, and grows again with a fresh token.
func TestRecoverRebuildsSingleVoterCluster(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()

	if err := Init(d1, "rec", "n1", freeAddr(t), "", 0, now); err != nil {
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
	if err := Join(d2, "n2", freeAddr(t), tok, "", 0); err != nil {
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
	if err := Join(d3, "n3", freeAddr(t), tok3, "", 0); err != nil {
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

// TestUpdateListenAddr: an explicit listen address rewrites the persisted
// cluster/join addresses and the node's own dial entry, so an operator can move
// a node's port across restarts (e.g. correcting a first boot that failed to
// bind). An empty or unchanged address is a no-op.
func TestUpdateListenAddr(t *testing.T) {
	now := time.Now()
	d := t.TempDir()
	if err := Init(d, "movetest", "n1", "127.0.0.1:7946", "", 0, now); err != nil {
		t.Fatal(err)
	}
	if err := UpdateListenAddr(d, "127.0.0.1:8001"); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(Dir(d))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClusterAddr != "127.0.0.1:8001" || cfg.JoinAddr != "127.0.0.1:8001" {
		t.Fatalf("listen address not updated: cluster=%q join=%q", cfg.ClusterAddr, cfg.JoinAddr)
	}
	for _, m := range cfg.Members {
		if m.NodeID == "n1" && m.Dial != "127.0.0.1:8001" {
			t.Fatalf("the node's own dial entry did not follow: %q", m.Dial)
		}
	}
	// An empty address leaves the config untouched.
	if err := UpdateListenAddr(d, ""); err != nil {
		t.Fatal(err)
	}
	if cfg2, _ := loadConfig(Dir(d)); cfg2.ClusterAddr != "127.0.0.1:8001" {
		t.Fatalf("empty update changed the address to %q", cfg2.ClusterAddr)
	}
}

// TestMemberDownRoundTrip pins the additive STATE field (Member.Down) through
// the status-protocol member codec: a down member encodes and decodes back
// down, and a member without field 9 decodes to up — back-compat with any
// peer that predates the field.
func TestMemberDownRoundTrip(t *testing.T) {
	m := Member{RaftID: 7, NodeID: "n7", Dial: "127.0.0.1:9", Host: "h1", Zone: "za", Capacity: 3, Down: true}
	got, err := decodeMemberMsg(encodeMemberMsg(m))
	if err != nil {
		t.Fatal(err)
	}
	if got != m {
		t.Fatalf("member round trip diverged: %+v vs %+v", got, m)
	}
	// An "up" member omits field 9 entirely (zero values are not encoded);
	// it must still decode as up.
	up := Member{RaftID: 7, NodeID: "n7"}
	gotUp, err := decodeMemberMsg(encodeMemberMsg(up))
	if err != nil {
		t.Fatal(err)
	}
	if gotUp.Down {
		t.Fatalf("an up member decoded as down: %+v", gotUp)
	}
}

// TestClusterZoneLabels proves failure-domain labels (ADR-0016) flow end to
// end: -zone at join travels the join protocol, the issuer records it, the
// leader's reconcile composes a labeled layout, and it surfaces in status —
// the same labels placement spreads over. Three nodes on one machine (one
// host) in three distinct zones.
func TestClusterZoneLabels(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()

	if err := Init(d1, "zonetest", "n1", freeAddr(t), "za", 0, now); err != nil {
		t.Fatal(err)
	}
	n1, err := Run(d1)
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Stop()
	waitStatus(t, d1, "", "n1 leading alone", func(ms []Member) bool {
		return len(ms) == 1 && ms[0].Leader
	})

	join := func(dir, id, zone string) *Node {
		tok, err := MintToken(d1, time.Hour, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := Join(dir, id, freeAddr(t), tok, zone, 0); err != nil {
			t.Fatal(err)
		}
		n, err := Run(dir)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	n2 := join(d2, "n2", "zb")
	defer n2.Stop()
	n3 := join(d3, "n3", "zc")
	defer n3.Stop()

	ms := waitStatus(t, d1, "", "three zone-labeled members", func(ms []Member) bool {
		if len(ms) != 3 {
			return false
		}
		zones := map[string]bool{}
		for _, m := range ms {
			if m.Zone == "" {
				return false
			}
			zones[m.Zone] = true
		}
		return len(zones) == 3
	})
	want := map[string]string{"n1": "za", "n2": "zb", "n3": "zc"}
	for _, m := range ms {
		if m.Zone != want[m.NodeID] {
			t.Fatalf("node %s zone = %q, want %q", m.NodeID, m.Zone, want[m.NodeID])
		}
		if m.Host == "" {
			t.Fatalf("node %s has no host label", m.NodeID)
		}
	}
}

// TestClusterCapacityWeights proves capacity weights (ADR-0004) flow end to
// end: -capacity at init/join travels the join protocol, the issuer records
// it, the leader's reconcile writes it into the committed layout, and it
// surfaces in status — read back from that same layout, the weight placement
// biases by. One heavy node (3) and two equal ones (1).
func TestClusterCapacityWeights(t *testing.T) {
	now := time.Now()
	d1, d2, d3 := t.TempDir(), t.TempDir(), t.TempDir()

	if err := Init(d1, "captest", "n1", freeAddr(t), "", 3, now); err != nil {
		t.Fatal(err)
	}
	n1, err := Run(d1)
	if err != nil {
		t.Fatal(err)
	}
	defer n1.Stop()
	waitStatus(t, d1, "", "n1 leading alone", func(ms []Member) bool {
		return len(ms) == 1 && ms[0].Leader
	})

	join := func(dir, id string, capacity uint32) *Node {
		tok, err := MintToken(d1, time.Hour, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := Join(dir, id, freeAddr(t), tok, "", capacity); err != nil {
			t.Fatal(err)
		}
		n, err := Run(dir)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	n2 := join(d2, "n2", 1)
	defer n2.Stop()
	n3 := join(d3, "n3", 1)
	defer n3.Stop()

	want := map[string]uint32{"n1": 3, "n2": 1, "n3": 1}
	ms := waitStatus(t, d1, "", "three capacity-weighted members", func(ms []Member) bool {
		if len(ms) != 3 {
			return false
		}
		for _, m := range ms {
			if m.Capacity != want[m.NodeID] {
				return false
			}
		}
		return true
	})
	for _, m := range ms {
		if m.Capacity != want[m.NodeID] {
			t.Fatalf("node %s capacity = %d, want %d", m.NodeID, m.Capacity, want[m.NodeID])
		}
	}
}
