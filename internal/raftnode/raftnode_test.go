package raftnode_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/raftnode"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sim"
)

// The raftnode integration test: a three-node metadata cluster under the
// simulation harness, with the network reordering and dropping messages,
// leaders crashing and restarting, and partitions isolating the leader
// mid-stream. The invariants: every acknowledged proposal survives and
// every replica converges to the same state — checked through the public
// read API across all live nodes.

const (
	tick          = 10 * time.Millisecond
	electionTicks = 10
)

// cluster wires three raftnode.Nodes into a Sim and remembers the current
// node objects (and their worlds) across restarts.
type cluster struct {
	t           *testing.T
	s           *sim.Sim
	nodes       map[uint64]*raftnode.Node
	worlds      map[uint64]*sim.World
	ids         map[uint64]seam.NodeID
	down        map[uint64]bool
	rosters     map[uint64]string // each node's latest OnMembershipChange report
	snapEntries uint64            // 0: package default (snapshots effectively off in tests)
}

func newCluster(t *testing.T, seed uint64, net sim.NetConfig) *cluster {
	c := &cluster{
		t: t, s: sim.New(seed, net),
		nodes:   make(map[uint64]*raftnode.Node),
		worlds:  make(map[uint64]*sim.World),
		ids:     map[uint64]seam.NodeID{1: "n1", 2: "n2", 3: "n3"},
		down:    make(map[uint64]bool),
		rosters: make(map[uint64]string),
	}
	return c
}

// roster formats a membership report the way tests assert on it.
func roster(ms []raftnode.Member) string {
	var parts []string
	for _, m := range ms {
		role := "voter"
		if m.Learner {
			role = "learner"
		}
		parts = append(parts, fmt.Sprintf("%s=%s", m.Addr, role))
	}
	return strings.Join(parts, " ")
}

func (c *cluster) start() *cluster {
	for id := uint64(1); id <= 3; id++ {
		c.s.AddNode(c.ids[id], c.boot(id, false))
	}
	return c
}

func (c *cluster) boot(id uint64, join bool) sim.BootFunc {
	return func(w *sim.World) seam.MessageHandler {
		n, err := raftnode.New(raftnode.Config{
			ID: id, Peers: c.ids, Join: join,
			Clock: w.Clock, Transport: w.Transport, Disk: w.Disk, Rand: w.Rand,
			TickInterval: tick, ElectionTicks: electionTicks,
			SnapshotEntries:    c.snapEntries,
			OnMembershipChange: func(ms []raftnode.Member) { c.rosters[id] = roster(ms) },
		})
		if err != nil {
			c.t.Fatalf("boot node %d: %v", id, err)
		}
		c.nodes[id] = n
		c.worlds[id] = w
		return n
	}
}

// addNode boots a fresh node with an empty disk in join mode and admits it
// through the leader, retrying through dropped conf changes and leadership
// churn. It returns once the leader's applied membership includes the node.
func (c *cluster) addNode(id uint64) {
	c.t.Helper()
	addr := seam.NodeID(fmt.Sprintf("n%d", id))
	c.ids[id] = addr
	c.s.AddNode(addr, c.boot(id, true))
	for range 50 {
		lead := c.leader()
		if err := c.nodes[lead].AddNode(id, addr, ""); err != nil {
			continue
		}
		for range 400 {
			c.s.Run(tick)
			if hasMember(c.nodes[lead].Members(), id) {
				return
			}
		}
	}
	c.t.Fatalf("node %d was never admitted", id)
}

// removeNode removes a member through the leader, with the same retry
// contract as addNode, then crashes the removed process.
func (c *cluster) removeNode(id uint64) {
	c.t.Helper()
	for range 50 {
		lead := c.leader()
		if err := c.nodes[lead].RemoveNode(id); err != nil {
			continue
		}
		for range 400 {
			c.s.Run(tick)
			if !hasMember(c.nodes[lead].Members(), id) {
				c.crash(id)
				return
			}
		}
	}
	c.t.Fatalf("node %d was never removed", id)
}

// waitMembers runs until some live replica's applied membership has the
// wanted shape: wantTotal members, of which wantVoters vote.
func (c *cluster) waitMembers(wantVoters, wantTotal int) {
	c.t.Helper()
	for range 4000 {
		c.s.Run(tick)
		for id, n := range c.nodes {
			if c.down[id] {
				continue
			}
			ms := n.Members()
			voters := 0
			for _, m := range ms {
				if !m.Learner {
					voters++
				}
			}
			if len(ms) == wantTotal && voters == wantVoters {
				return
			}
		}
	}
	c.t.Fatalf("membership never reached %d voters of %d members", wantVoters, wantTotal)
}

func hasMember(ms []raftnode.Member, id uint64) bool {
	for _, m := range ms {
		if m.ID == id {
			return true
		}
	}
	return false
}

// logFiles lists a node's raft log files — how the tests observe rotation.
func (c *cluster) logFiles(id uint64) []string {
	c.t.Helper()
	names, err := c.worlds[id].Disk.List()
	if err != nil {
		c.t.Fatal(err)
	}
	var logs []string
	for _, name := range names {
		if strings.HasPrefix(name, "raft/log.") {
			logs = append(logs, name)
		}
	}
	return logs
}

// leader runs the sim until exactly one live node reports itself leader
// and a quorum agrees on it, then returns its ID.
func (c *cluster) leader() uint64 {
	for range 4000 {
		c.s.Run(tick)
		votes := make(map[uint64]int)
		for id, n := range c.nodes {
			if c.down[id] {
				continue
			}
			if lead, _ := n.Leader(); lead != 0 {
				votes[lead]++
			}
		}
		for lead, count := range votes {
			if count >= 2 && !c.down[lead] {
				if _, isLeader := c.nodes[lead].Leader(); isLeader {
					return lead
				}
			}
		}
	}
	c.t.Fatal("no leader emerged")
	return 0
}

// propose submits p on the current leader and runs the sim until the
// result callback fires, retrying through leadership changes. It returns
// apply's result.
func (c *cluster) propose(p any) any {
	c.t.Helper()
	for range 50 {
		lead := c.leader()
		var res any
		var applyErr error
		fired := false
		c.nodes[lead].Propose(p, func(r any, err error) {
			res, applyErr, fired = r, err, true
		})
		deadline := 0
		for !fired && deadline < 1000 {
			c.s.Run(tick)
			deadline++
		}
		if !fired {
			continue // leadership churned mid-proposal; try the new leader
		}
		if errors.Is(applyErr, raftnode.ErrNotLeader) {
			continue
		}
		if applyErr != nil {
			c.t.Fatalf("propose %T: %v", p, applyErr)
		}
		return res
	}
	c.t.Fatalf("propose %T: never committed", p)
	return nil
}

func (c *cluster) crash(id uint64) {
	c.s.Crash(c.ids[id])
	c.down[id] = true
	delete(c.nodes, id)
}

func (c *cluster) restart(id uint64) {
	c.down[id] = false
	c.s.Restart(c.ids[id]) // boot repopulates c.nodes[id]
}

// converged waits until every live replica reports the same buckets and
// objects, then compares them key by key through the public API.
func (c *cluster) converged(model map[string]map[string]int64) {
	c.t.Helper()
	for attempt := 0; ; attempt++ {
		c.s.Run(tick)
		if c.check(model, false) {
			c.check(model, true)
			return
		}
		if attempt > 4000 {
			c.check(model, true)
			return
		}
	}
}

// check compares every live store against the model: bucket set, object
// set, and sizes. With report set, mismatches are fatal test failures.
func (c *cluster) check(model map[string]map[string]int64, report bool) bool {
	c.t.Helper()
	for id, n := range c.nodes {
		if c.down[id] {
			continue
		}
		st := n.Store()
		var buckets []string
		for _, b := range st.ListBuckets() {
			buckets = append(buckets, b.Name)
		}
		if len(buckets) != len(model) {
			if report {
				c.t.Fatalf("node %d: %d buckets, want %d", id, len(buckets), len(model))
			}
			return false
		}
		for bucket, objects := range model {
			listed := st.ListObjects(bucket, "", "", 10_000)
			if len(listed) != len(objects) {
				if report {
					c.t.Fatalf("node %d: bucket %q has %d objects, want %d", id, bucket, len(listed), len(objects))
				}
				return false
			}
			for _, o := range listed {
				size, ok := objects[o.Key]
				if !ok || o.Current.Size != size {
					if report {
						c.t.Fatalf("node %d: object %s/%s size %d, model %d (known %v)", id, bucket, o.Key, o.Current.Size, size, ok)
					}
					return false
				}
			}
		}
	}
	return true
}

func mkPut(bucket, key string, size int64, seq byte) meta.PutObject {
	return meta.PutObject{
		ProposedAtUnixMS: 1_750_000_000_000 + int64(seq),
		Bucket:           bucket, Key: key,
		VersionID: meta.VersionID{seq, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Size:      size, ETag: []byte{seq}, ObjectChecksum: []byte{seq},
	}
}

// TestClusterReplicatesAndSurvivesLeaderCrash: the bread-and-butter
// schedule — elect, replicate, kill the leader mid-life, elect again,
// keep writing, bring the dead node back, require full convergence.
func TestClusterReplicatesAndSurvivesLeaderCrash(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3, 4, 5} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := newCluster(t, seed, sim.NetConfig{
				MinLatency: time.Millisecond, MaxLatency: 8 * time.Millisecond,
			}).start()
			model := map[string]map[string]int64{"bkt": {}}

			c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})
			for i := range 5 {
				key := fmt.Sprintf("pre/%d", i)
				c.propose(mkPut("bkt", key, int64(100+i), byte(i+1)))
				model["bkt"][key] = int64(100 + i)
			}

			lead := c.leader()
			c.crash(lead)

			for i := range 5 {
				key := fmt.Sprintf("post/%d", i)
				c.propose(mkPut("bkt", key, int64(200+i), byte(i+10)))
				model["bkt"][key] = int64(200 + i)
			}

			c.restart(lead)
			c.converged(model)
		})
	}
}

// TestClusterPartitionedLeaderCannotAck: isolate the leader, prove its
// proposals never commit (no quorum, no ack), let the majority elect and
// write, heal, and require the old leader to adopt the majority's history.
func TestClusterPartitionedLeaderCannotAck(t *testing.T) {
	c := newCluster(t, 42, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 5 * time.Millisecond,
	}).start()
	model := map[string]map[string]int64{"bkt": {}}
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})

	old := c.leader()
	for id := uint64(1); id <= 3; id++ {
		if id != old {
			c.s.Partition(c.ids[old], c.ids[id])
			c.s.Partition(c.ids[id], c.ids[old])
		}
	}

	// The isolated leader accepts a proposal but can never commit it.
	fired := false
	c.nodes[old].Propose(mkPut("bkt", "lost", 999, 99), func(any, error) { fired = true })
	for range 300 {
		c.s.Run(tick)
	}
	if fired {
		t.Fatal("a proposal committed without a quorum")
	}

	// The majority moves on. Track who can still see whom: the leader
	// helper only counts live nodes, and the old leader (isolated) will
	// keep claiming its stale term — exclude it by marking it down for
	// the election search, then restore it for the heal.
	c.down[old] = true
	for i := range 3 {
		key := fmt.Sprintf("majority/%d", i)
		c.propose(mkPut("bkt", key, int64(300+i), byte(i+20)))
		model["bkt"][key] = int64(300 + i)
	}
	c.down[old] = false

	for id := uint64(1); id <= 3; id++ {
		if id != old {
			c.s.Heal(c.ids[old], c.ids[id])
			c.s.Heal(c.ids[id], c.ids[old])
		}
	}
	c.converged(model)

	// The phantom write never surfaced anywhere.
	for id, n := range c.nodes {
		if _, ok := n.Store().Current("bkt", "lost"); ok {
			t.Fatalf("node %d: uncommitted write became visible", id)
		}
	}
}

// TestClusterSeedDeterminism: the ADR-0024 payoff — identical seeds give
// identical histories, elections included.
func TestClusterSeedDeterminism(t *testing.T) {
	run := func() (uint64, []string) {
		c := newCluster(t, 7, sim.NetConfig{
			MinLatency: time.Millisecond, MaxLatency: 12 * time.Millisecond,
			DropProb: 0.02, DuplicateProb: 0.02,
		}).start()
		c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})
		first := c.leader()
		c.crash(first)
		c.propose(mkPut("bkt", "k", 5, 1))
		c.restart(first)
		c.converged(map[string]map[string]int64{"bkt": {"k": 5}})

		var listings []string
		for _, o := range c.nodes[first].Store().ListObjects("bkt", "", "", 100) {
			listings = append(listings, fmt.Sprintf("%s/%d", o.Key, o.Current.Size))
		}
		lead := c.leader()
		return lead, listings
	}
	lead1, list1 := run()
	lead2, list2 := run()
	if lead1 != lead2 || fmt.Sprint(list1) != fmt.Sprint(list2) {
		t.Fatalf("same seed, different history: leader %d vs %d, %v vs %v", lead1, lead2, list1, list2)
	}
}

// TestSnapshotCompactsAndRecovers: with an aggressive snapshot threshold,
// enough writes must rotate every node's log (one file, opening with a
// snapshot), and a full-cluster cold restart must recover the complete
// state from snapshot plus tail.
func TestSnapshotCompactsAndRecovers(t *testing.T) {
	c := newCluster(t, 11, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 6 * time.Millisecond,
	})
	c.snapEntries = 8
	c.start()

	model := map[string]map[string]int64{"bkt": {}}
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})
	for i := range 30 {
		key := fmt.Sprintf("k/%02d", i)
		c.propose(mkPut("bkt", key, int64(1000+i), byte(i+1)))
		model["bkt"][key] = int64(1000 + i)
	}
	c.converged(model)

	for id := uint64(1); id <= 3; id++ {
		logs := c.logFiles(id)
		if len(logs) != 1 || logs[0] == "raft/log.1" {
			t.Fatalf("node %d: logs %v, want exactly one rotated file", id, logs)
		}
	}

	// Cold restart of the whole cluster: every replica reboots from its
	// snapshot and committed tail.
	for id := uint64(1); id <= 3; id++ {
		c.crash(id)
	}
	c.s.Run(time.Second)
	for id := uint64(1); id <= 3; id++ {
		c.restart(id)
	}
	c.converged(model)

	// And the recovered cluster still accepts writes.
	c.propose(mkPut("bkt", "after", 42, 99))
	model["bkt"]["after"] = 42
	c.converged(model)
}

// TestLaggingFollowerCatchesUpViaSnapshot: a follower sleeps through
// enough writes that the leader compacts past its log — the only road back
// is a streamed snapshot (MsgSnap), installed as a log rotation.
func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	c := newCluster(t, 23, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 6 * time.Millisecond,
	})
	c.snapEntries = 8
	c.start()

	model := map[string]map[string]int64{"bkt": {}}
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})

	lead := c.leader()
	var follower uint64
	for id := uint64(1); id <= 3; id++ {
		if id != lead {
			follower = id
			break
		}
	}
	c.crash(follower)

	for i := range 40 {
		key := fmt.Sprintf("k/%02d", i)
		c.propose(mkPut("bkt", key, int64(1000+i), byte(i+1)))
		model["bkt"][key] = int64(1000 + i)
	}

	c.restart(follower)
	c.converged(model)

	// The follower's recovery had to come through a streamed snapshot —
	// the leader compacted far past its log — installed as a rotation.
	if got := c.nodes[follower].SnapshotsReceived(); got == 0 {
		t.Fatal("follower converged without receiving a snapshot — the leader never compacted past it?")
	}
	logs := c.logFiles(follower)
	if len(logs) != 1 || logs[0] == "raft/log.1" {
		t.Fatalf("follower logs %v, want a single rotated file from the snapshot install", logs)
	}
}

// TestColdRestartBeforeSnapshot: a full-cluster cold restart while the
// cluster's only configuration record is the bootstrap conf-change entries
// — no snapshot has happened yet, so boot must re-feed those entries to
// raft or every node comes back memberless and no leader can ever emerge.
func TestColdRestartBeforeSnapshot(t *testing.T) {
	c := newCluster(t, 31, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 6 * time.Millisecond,
	}).start()
	model := map[string]map[string]int64{"bkt": {}}
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})
	c.propose(mkPut("bkt", "k", 7, 1))
	model["bkt"]["k"] = 7

	for id := uint64(1); id <= 3; id++ {
		c.crash(id)
	}
	c.s.Run(time.Second)
	for id := uint64(1); id <= 3; id++ {
		c.restart(id)
	}
	c.converged(model)

	c.propose(mkPut("bkt", "after", 9, 2))
	model["bkt"]["after"] = 9
	c.converged(model)
}

// TestJoinReplicatesAndPromotes: a fourth node joins a running cluster as
// a learner, replicates the existing history, is promoted to voter once
// caught up (four members — everyone votes under the ADR-0017 cap), and
// the grown cluster survives losing its leader.
func TestJoinReplicatesAndPromotes(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := newCluster(t, seed, sim.NetConfig{
				MinLatency: time.Millisecond, MaxLatency: 8 * time.Millisecond,
			}).start()
			model := map[string]map[string]int64{"bkt": {}}
			c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})
			for i := range 5 {
				key := fmt.Sprintf("pre/%d", i)
				c.propose(mkPut("bkt", key, int64(100+i), byte(i+1)))
				model["bkt"][key] = int64(100 + i)
			}

			c.addNode(4)
			c.waitMembers(4, 4) // caught up → promoted; everyone votes at four
			c.converged(model)

			lead := c.leader()
			c.crash(lead)
			c.propose(mkPut("bkt", "post", 7, 99))
			model["bkt"]["post"] = 7
			c.restart(lead)
			c.converged(model)
		})
	}
}

// TestJoinerRequestsOwnAdmission: nobody calls AddNode — the joining node
// asks the cluster itself (admit messages to every peer; the leader
// answers), which is how the CLI join flow works. The joiner must end up a
// member, caught up, and promoted.
func TestJoinerRequestsOwnAdmission(t *testing.T) {
	for _, seed := range []uint64{5, 6} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := newCluster(t, seed, sim.NetConfig{
				MinLatency: time.Millisecond, MaxLatency: 8 * time.Millisecond,
				DropProb: 0.02,
			}).start()
			model := map[string]map[string]int64{"bkt": {}}
			c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})
			c.propose(mkPut("bkt", "k", 5, 1))
			model["bkt"]["k"] = 5

			c.ids[4] = "n4"
			c.s.AddNode("n4", c.boot(4, true))
			c.waitMembers(4, 4)
			c.converged(model)

			// Every replica reported the change to its composition root
			// (the callback behind the membership log lines).
			want := "n1=voter n2=voter n3=voter n4=voter"
			for id := uint64(1); id <= 4; id++ {
				if c.rosters[id] != want {
					t.Fatalf("node %d's last membership report: %q, want %q", id, c.rosters[id], want)
				}
			}
		})
	}
}

// TestVoterCapAndLearners: grown to seven nodes, the cluster holds exactly
// five voters (ADR-0017) — the rest replicate as learners and serve reads
// identically, which converged() checks through the public API.
func TestVoterCapAndLearners(t *testing.T) {
	c := newCluster(t, 17, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 6 * time.Millisecond,
	}).start()
	model := map[string]map[string]int64{"bkt": {}}
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})
	for i := range 5 {
		key := fmt.Sprintf("k/%d", i)
		c.propose(mkPut("bkt", key, int64(100+i), byte(i+1)))
		model["bkt"][key] = int64(100 + i)
	}

	for id := uint64(4); id <= 7; id++ {
		c.addNode(id)
	}
	c.waitMembers(5, 7)
	c.converged(model)

	// Growth never overshoots: no replica ever applies a sixth voter.
	for id, n := range c.nodes {
		voters := 0
		for _, m := range n.Members() {
			if !m.Learner {
				voters++
			}
		}
		if voters > 5 {
			t.Fatalf("node %d applied %d voters; the cap is five", id, voters)
		}
	}
}

// TestRemoveVoterRefillsFromLearners: removing a voter opens a vacancy
// that promotion fills from the learners, and the removed node's address
// drops from the book.
func TestRemoveVoterRefillsFromLearners(t *testing.T) {
	c := newCluster(t, 19, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 6 * time.Millisecond,
	}).start()
	model := map[string]map[string]int64{"bkt": {}}
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})

	for id := uint64(4); id <= 6; id++ {
		c.addNode(id)
	}
	c.waitMembers(5, 6) // five voters, one learner

	// Remove a voter that is not the leader.
	lead := c.leader()
	var victim uint64
	for _, m := range c.nodes[lead].Members() {
		if !m.Learner && m.ID != lead {
			victim = m.ID
			break
		}
	}
	c.removeNode(victim)
	c.waitMembers(5, 5) // the learner was promoted into the vacancy

	c.propose(mkPut("bkt", "after", 11, 42))
	model["bkt"]["after"] = 11
	c.converged(model)
}

// TestJoinerCatchesUpViaSnapshotAndRestarts: a node joins a cluster whose
// leader has compacted its log — catch-up must arrive as a streamed
// snapshot carrying the rows and the address book — then crashes and
// restarts back into the cluster from its own disk.
func TestJoinerCatchesUpViaSnapshotAndRestarts(t *testing.T) {
	c := newCluster(t, 29, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 6 * time.Millisecond,
	})
	c.snapEntries = 8
	c.start()

	model := map[string]map[string]int64{"bkt": {}}
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"})
	for i := range 40 {
		key := fmt.Sprintf("k/%02d", i)
		c.propose(mkPut("bkt", key, int64(1000+i), byte(i+1)))
		model["bkt"][key] = int64(1000 + i)
	}

	c.addNode(4)
	c.converged(model)
	if got := c.nodes[4].SnapshotsReceived(); got == 0 {
		t.Fatal("joiner converged without a streamed snapshot — the leader never compacted?")
	}

	c.crash(4)
	for i := range 5 {
		key := fmt.Sprintf("late/%d", i)
		c.propose(mkPut("bkt", key, int64(2000+i), byte(i+50)))
		model["bkt"][key] = int64(2000 + i)
	}
	c.restart(4)
	c.converged(model)
}
