package raftnode_test

import (
	"errors"
	"fmt"
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
// node objects across restarts.
type cluster struct {
	t     *testing.T
	s     *sim.Sim
	nodes map[uint64]*raftnode.Node
	ids   map[uint64]seam.NodeID
	down  map[uint64]bool
}

func newCluster(t *testing.T, seed uint64, net sim.NetConfig) *cluster {
	c := &cluster{
		t: t, s: sim.New(seed, net),
		nodes: make(map[uint64]*raftnode.Node),
		ids:   map[uint64]seam.NodeID{1: "n1", 2: "n2", 3: "n3"},
		down:  make(map[uint64]bool),
	}
	for id := uint64(1); id <= 3; id++ {
		c.s.AddNode(c.ids[id], c.boot(id))
	}
	return c
}

func (c *cluster) boot(id uint64) sim.BootFunc {
	return func(w *sim.World) seam.MessageHandler {
		n, err := raftnode.New(raftnode.Config{
			ID: id, Peers: c.ids,
			Clock: w.Clock, Transport: w.Transport, Disk: w.Disk, Rand: w.Rand,
			TickInterval: tick, ElectionTicks: electionTicks,
		})
		if err != nil {
			c.t.Fatalf("boot node %d: %v", id, err)
		}
		c.nodes[id] = n
		return n
	}
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
			})
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
	})
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
		})
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
