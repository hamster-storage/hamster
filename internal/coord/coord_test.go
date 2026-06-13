package coord_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/place"
	"github.com/hamster-storage/hamster/internal/raftnode"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sim"
	"github.com/hamster-storage/hamster/internal/stream"
)

// The pass-3 integration: a simulated cluster where the first three nodes
// run the Raft metadata plane and every node runs the data plane, exactly
// the v0.3 shape (ADR-0027). PUTs are driven through the leader's
// coordinator; durability is then proven the hard way — by decoding the
// object back out of the shard files on the surviving simulated disks,
// never by trusting the coordinator's word.

const (
	tick          = 10 * time.Millisecond
	electionTicks = 10
	bucket        = "vault"
)

// simNode is one node's composition: the channel demux in front of the
// Raft plane (if this node has one) and the data plane — the same wiring
// the real binary will use in pass 5.
type simNode struct {
	raft *raftnode.Node // nil on data-only nodes
	data *datapath.Service
	co   *coord.Coordinator
}

func (n *simNode) HandleMessage(from seam.NodeID, msg []byte) {
	ch, payload, err := datapath.Unwrap(msg)
	if err != nil {
		return
	}
	switch ch {
	case datapath.ChannelRaft:
		if n.raft != nil {
			n.raft.HandleMessage(from, payload)
		}
	case datapath.ChannelData:
		_ = n.data.HandleData(from, payload)
	}
}

// raftTransport wraps Raft traffic in the channel envelope.
type raftTransport struct{ t seam.Transport }

func (rt raftTransport) Send(to seam.NodeID, msg []byte) {
	rt.t.Send(to, datapath.Wrap(datapath.ChannelRaft, msg))
}

type cluster struct {
	t       *testing.T
	s       *sim.Sim
	members []seam.NodeID
	raftIDs map[uint64]seam.NodeID
	nodes   map[seam.NodeID]*simNode
	worlds  map[seam.NodeID]*sim.World
	down    map[seam.NodeID]bool
	profile ec.Profile
}

// newCluster builds n nodes; the first min(n, 3) are Raft voters.
func newCluster(t *testing.T, seed uint64, net sim.NetConfig, n int, profile ec.Profile) *cluster {
	c := &cluster{
		t: t, s: sim.New(seed, net),
		raftIDs: make(map[uint64]seam.NodeID),
		nodes:   make(map[seam.NodeID]*simNode),
		worlds:  make(map[seam.NodeID]*sim.World),
		down:    make(map[seam.NodeID]bool),
		profile: profile,
	}
	for i := 1; i <= min(n, 3); i++ {
		c.raftIDs[uint64(i)] = seam.NodeID(fmt.Sprintf("n%d", i))
	}
	for i := 1; i <= n; i++ {
		c.members = append(c.members, seam.NodeID(fmt.Sprintf("n%d", i)))
	}
	for i := 1; i <= n; i++ {
		id := c.members[i-1]
		raftID := uint64(0)
		if i <= 3 {
			raftID = uint64(i)
		}
		c.s.AddNode(id, c.boot(id, raftID))
	}
	return c
}

func (c *cluster) boot(id seam.NodeID, raftID uint64) sim.BootFunc {
	return func(w *sim.World) seam.MessageHandler {
		n := &simNode{}
		n.data = datapath.New(datapath.Config{Clock: w.Clock, Transport: w.Transport, Disk: w.Disk})
		if raftID != 0 {
			rn, err := raftnode.New(raftnode.Config{
				ID: raftID, Peers: c.raftIDs,
				Clock: w.Clock, Transport: raftTransport{w.Transport}, Disk: w.Disk, Rand: w.Rand,
				TickInterval: tick, ElectionTicks: electionTicks,
			})
			if err != nil {
				c.t.Fatalf("boot %s: %v", id, err)
			}
			n.raft = rn
			n.co = coord.New(coord.Config{
				Clock: w.Clock, Rand: w.Rand, Data: n.data, Raft: rn,
				Layout: func() (place.Layout, bool) {
					// Distinct host/zone per node: spread collapses to the
					// bare rendezvous ranking readObject verifies against.
					nodes := make([]place.Node, len(c.members))
					for i, id := range c.members {
						nodes[i] = place.Node{ID: id, Host: string(id), Zone: string(id)}
					}
					return place.Layout{
						Version:        1,
						PartitionCount: place.DefaultPartitionCount,
						Members:        nodes,
					}, true
				},
			})
		}
		c.nodes[id] = n
		c.worlds[id] = w
		return n
	}
}

// leader runs the sim until a quorum of live Raft nodes agrees on one
// live leader, then returns its node ID.
func (c *cluster) leader() seam.NodeID {
	c.t.Helper()
	for range 4000 {
		c.s.Run(tick)
		votes := make(map[uint64]int)
		live := 0
		for _, id := range c.raftIDs {
			if c.down[id] {
				continue
			}
			live++
			if lead, _ := c.nodes[id].raft.Leader(); lead != 0 {
				votes[lead]++
			}
		}
		for lead, n := range votes {
			id := c.raftIDs[lead]
			if n >= min(2, live) && !c.down[id] {
				return id
			}
		}
	}
	c.t.Fatal("no leader emerged")
	return ""
}

// propose drives one metadata proposal through the leader, retrying
// through elections.
func (c *cluster) propose(p any) {
	c.t.Helper()
	for range 50 {
		id := c.leader()
		var perr error
		done := false
		c.worlds[id].Loop.Post(func() {
			c.nodes[id].raft.Propose(p, func(_ any, err error) { perr, done = err, true })
		})
		for range 400 {
			c.s.Run(tick)
			if done {
				break
			}
		}
		if done && perr == nil {
			return
		}
	}
	c.t.Fatalf("proposal %T never committed", p)
}

// put drives one PUT through the leader's coordinator to completion.
func (c *cluster) put(key string, body []byte) (coord.PutResult, error) {
	c.t.Helper()
	id := c.leader()
	var res coord.PutResult
	var perr error
	done := false
	c.worlds[id].Loop.Post(func() {
		c.nodes[id].co.Put(bucket, key, body, coord.PutOptions{}, func(r coord.PutResult, e error) {
			res, perr, done = r, e, true
		})
	})
	for range 5000 {
		c.s.Run(tick)
		if done {
			return res, perr
		}
	}
	c.t.Fatal("put never finished")
	return res, perr
}

// entry reads a key's current version entry from the leader's store.
func (c *cluster) entry(key string) (meta.VersionEntry, bool) {
	c.t.Helper()
	id := c.leader()
	store := c.nodes[id].raft.Store()
	var e meta.VersionEntry
	var ok bool
	c.worlds[id].Loop.Post(func() {
		cur, found := store.Current(bucket, key)
		if !found {
			return
		}
		e, ok = store.GetVersion(bucket, key, cur.VersionID)
	})
	c.s.Run(0) // the sim is single-threaded: dispatch the posted read now
	return e, ok
}

// readObject proves durability: it decodes the object from the shard
// files on the disks of nodes that are up, through the same pure readers
// production uses. Crashed nodes' shards are unreachable, exactly as a
// network GET would find them.
func (c *cluster) readObject(key string) ([]byte, error) {
	c.t.Helper()
	e, ok := c.entry(key)
	if !ok {
		return nil, errors.New("no such key")
	}
	width := int(e.ECDataShards + e.ECParityShards)
	nodes, err := place.Nodes(e.Partition, c.members, width)
	if err != nil {
		return nil, err
	}
	shards := make([]io.ReaderAt, width)
	for i, nid := range nodes {
		if c.down[nid] {
			continue
		}
		disk := c.worlds[nid].Disk
		name := datapath.ShardFileName(e.DataID, uint32(i))
		if _, err := disk.ReadFileAt(name+".ok", 0, 0); err != nil {
			continue // no commit marker: staging garbage, not a shard
		}
		data, err := disk.ReadFile(name)
		if err != nil {
			continue
		}
		shards[i] = bytes.NewReader(data)
	}
	er, err := ec.NewReader(shards)
	if err != nil {
		return nil, err
	}
	sr, err := stream.NewReader(er, er.FrameSize())
	if err != nil {
		return nil, err
	}
	got := make([]byte, e.Size)
	if e.Size > 0 {
		if _, err := sr.ReadAt(got, 0); err != nil {
			return nil, err
		}
	}
	return got, nil
}

func (c *cluster) crash(id seam.NodeID) {
	c.s.Crash(id)
	c.down[id] = true
}

func randomBody(seed uint64, n int) []byte {
	rng := rand.New(rand.NewPCG(seed, 0x90D))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.UintN(256))
	}
	return b
}

func profile(t *testing.T, name string) ec.Profile {
	p, err := ec.ProfileByName(name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestPutAcrossSizes: healthy 6-node cluster at 4+2. Every size lands all
// its shards, commits metadata with the right parameters, and decodes
// back bit-identically — including the small-object sizes that drop to
// k=1 and the multi-window size that exercises pacing.
func TestPutAcrossSizes(t *testing.T) {
	c := newCluster(t, 1, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	for i, size := range []int{0, 1, 100 << 10, 1 << 20, 3<<20 + 777, 8<<20 + 13} {
		key := fmt.Sprintf("obj-%d", size)
		body := randomBody(uint64(i), size)
		res, err := c.put(key, body)
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		wantK, wantM := c.profile.Params(int64(size))
		if res.Durable != wantK+wantM {
			t.Errorf("%s: durable %d, want all %d", key, res.Durable, wantK+wantM)
		}
		e, ok := c.entry(key)
		if !ok || int(e.ECDataShards) != wantK || int(e.ECParityShards) != wantM || e.Size != int64(size) {
			t.Fatalf("%s: entry k=%d m=%d size=%d ok=%v, want k=%d m=%d size=%d",
				key, e.ECDataShards, e.ECParityShards, e.Size, ok, wantK, wantM, size)
		}
		got, err := c.readObject(key)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s: read back: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
	}
}

// TestPutUnderFaultyNetwork: drops, duplicates, and jitter between every
// node — the transfer and the Raft commit both ride it out.
func TestPutUnderFaultyNetwork(t *testing.T) {
	c := newCluster(t, 7, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 15 * time.Millisecond,
		DropProb: 0.03, DuplicateProb: 0.03,
	}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	body := randomBody(7, 1<<20+99)
	res, err := c.put("stormy", body)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if res.Durable != 6 {
		t.Errorf("durable %d, want 6", res.Durable)
	}
	if got, err := c.readObject("stormy"); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("read back: err=%v", err)
	}
}

// TestDegradedReadWithinBudget: after a healthy ack, losing m nodes keeps
// the object readable; losing one more makes it cleanly unreadable —
// refused, never garbage.
func TestDegradedReadWithinBudget(t *testing.T) {
	c := newCluster(t, 2, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	body := randomBody(2, 2<<20)
	if _, err := c.put("survivor", body); err != nil {
		t.Fatalf("put: %v", err)
	}

	e, _ := c.entry("survivor")
	holders, err := place.Nodes(e.Partition, c.members, 6)
	if err != nil {
		t.Fatal(err)
	}
	// Never crash the metadata quorum out from under the test: kill the
	// two holders that leave at least two Raft nodes standing.
	var victims []seam.NodeID
	for _, h := range holders {
		if h != "n1" && h != "n2" && len(victims) < 2 {
			victims = append(victims, h)
		}
	}
	for _, v := range victims {
		c.crash(v)
	}
	if got, err := c.readObject("survivor"); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("read with m nodes down: equal=%v err=%v", bytes.Equal(got, body), err)
	}

	for _, h := range holders {
		if h != "n1" && h != "n2" && !c.down[h] {
			c.crash(h)
			break
		}
	}
	if _, err := c.readObject("survivor"); err == nil {
		t.Fatal("read with m+1 nodes down succeeded; it must refuse, not fabricate")
	}
}

// TestWriteFloorWithNodeDown: one node down out of six, 4+2. The write
// acks at five durable shards (the k+1 floor holds), the object reads
// back, and the budget is honest: one more loss is survivable.
func TestWriteFloorWithNodeDown(t *testing.T) {
	c := newCluster(t, 3, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	c.crash("n6") // data-only: quorum untouched
	body := randomBody(3, 1<<20)
	res, err := c.put("degraded-write", body)
	if err != nil {
		t.Fatalf("put with a node down: %v", err)
	}
	if res.Durable != 5 {
		t.Errorf("durable %d, want 5 (skipped shard on the down node)", res.Durable)
	}

	// Budget at ack is durable − k = 1: one more loss must be survivable.
	e, _ := c.entry("degraded-write")
	holders, _ := place.Nodes(e.Partition, c.members, 6)
	for _, h := range holders {
		if h != "n1" && h != "n2" && !c.down[h] {
			c.crash(h)
			break
		}
	}
	if got, err := c.readObject("degraded-write"); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("read inside the ack budget: equal=%v err=%v", bytes.Equal(got, body), err)
	}
}

// TestRefusedBelowFloor: two nodes down out of six leaves at most four
// durable shards — below the k+1 floor. The write must refuse (SlowDown)
// and commit nothing.
func TestRefusedBelowFloor(t *testing.T) {
	c := newCluster(t, 4, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	c.crash("n5")
	c.crash("n6")
	_, err := c.put("refused", randomBody(4, 1<<20))
	if !errors.Is(err, coord.ErrRefused) {
		t.Fatalf("put below the floor: err=%v, want ErrRefused", err)
	}
	if _, ok := c.entry("refused"); ok {
		t.Fatal("a refused write committed metadata")
	}
}

// TestSmallClusterNeedsEveryNode: 2+1 on three nodes — the floor equals
// k+m, so one node down refuses writes. The documented cost of small
// clusters, mechanically true.
func TestSmallClusterNeedsEveryNode(t *testing.T) {
	c := newCluster(t, 5, sim.NetConfig{}, 3, profile(t, "2+1"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	body := randomBody(5, 600<<10)
	if _, err := c.put("healthy", body); err != nil {
		t.Fatalf("healthy 2+1 put: %v", err)
	}

	// Crash a non-leader so the metadata plane keeps quorum.
	lead := c.leader()
	for _, id := range c.members {
		if id != lead {
			c.crash(id)
			break
		}
	}
	if _, err := c.put("blocked", randomBody(50, 600<<10)); !errors.Is(err, coord.ErrRefused) {
		t.Fatalf("2+1 put with a node down: err=%v, want ErrRefused", err)
	}
	// Reads of existing data keep working through reconstruction.
	if got, err := c.readObject("healthy"); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("degraded 2+1 read: equal=%v err=%v", bytes.Equal(got, body), err)
	}
}

// TestCoordinatorCrashMidPut: the coordinator dying mid-transfer commits
// nothing — the metadata commit is the linearization point, so a crash
// before it means the object never existed. A retry on the new leader
// succeeds over whatever staging garbage the first attempt left.
func TestCoordinatorCrashMidPut(t *testing.T) {
	c := newCluster(t, 6, sim.NetConfig{MinLatency: 5 * time.Millisecond, MaxLatency: 10 * time.Millisecond}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	body := randomBody(6, 2<<20)
	lead := c.leader()
	c.worlds[lead].Loop.Post(func() {
		c.nodes[lead].co.Put(bucket, "casualty", body, coord.PutOptions{}, func(coord.PutResult, error) {
			t.Error("done fired for a coordinator that crashed mid-put")
		})
	})
	c.s.Run(time.Millisecond) // the transfer has started; no ack has landed
	c.crash(lead)
	c.s.Run(time.Second)
	c.s.Restart(lead)
	c.down[lead] = false

	if _, ok := c.entry("casualty"); ok {
		t.Fatal("a crashed coordinator's put committed metadata")
	}
	res, err := c.put("casualty", body)
	if err != nil {
		t.Fatalf("retry after coordinator crash: %v", err)
	}
	if res.Durable != 6 {
		t.Errorf("retry durable %d, want 6", res.Durable)
	}
	if got, err := c.readObject("casualty"); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("retry read back: err=%v", err)
	}
}

// TestDeterminism: the same seed replays the same distributed PUT —
// outcome, version, and reconstructed bytes.
func TestDeterminism(t *testing.T) {
	run := func() (coord.PutResult, []byte) {
		c := newCluster(t, 11, sim.NetConfig{
			MinLatency: time.Millisecond, MaxLatency: 25 * time.Millisecond,
			DropProb: 0.05, DuplicateProb: 0.05,
		}, 6, profile(t, "4+2"))
		c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
		res, err := c.put("replay", randomBody(11, 1<<20))
		if err != nil {
			t.Fatalf("put: %v", err)
		}
		got, err := c.readObject("replay")
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return res, got
	}
	res1, got1 := run()
	res2, got2 := run()
	if res1.VersionID != res2.VersionID || res1.Durable != res2.Durable || !bytes.Equal(got1, got2) {
		t.Fatalf("same seed, different run: %v/%d vs %v/%d", res1.VersionID, res1.Durable, res2.VersionID, res2.Durable)
	}
}

// get drives one network GET through the leader's coordinator.
func (c *cluster) get(key string, off, length int64) ([]byte, error) {
	c.t.Helper()
	id := c.leader()
	var out []byte
	var gerr error
	done := false
	c.worlds[id].Loop.Post(func() {
		c.nodes[id].co.Get(bucket, key, off, length, func(b []byte, e error) {
			out, gerr, done = b, e, true
		})
	})
	for range 5000 {
		c.s.Run(tick)
		if done {
			return out, gerr
		}
	}
	c.t.Fatal("get never finished")
	return nil, nil
}

// TestGetOverNetwork: the full read path — header fetches, covering slice
// fetches, CRC-verified decode — across sizes, whole objects and ranges.
func TestGetOverNetwork(t *testing.T) {
	c := newCluster(t, 21, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	for i, size := range []int{0, 1, 100 << 10, 3<<20 + 777} {
		key := fmt.Sprintf("net-%d", size)
		body := randomBody(uint64(20+i), size)
		if _, err := c.put(key, body); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		got, err := c.get(key, 0, -1)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s whole: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
		if size > 4096 {
			got, err := c.get(key, 2500, 4000)
			if err != nil || !bytes.Equal(got, body[2500:6500]) {
				t.Fatalf("%s range: equal=%v err=%v", key, bytes.Equal(got, body[2500:6500]), err)
			}
			// A range crossing the final stripe boundary.
			tail := int64(size) - 1000
			got, err = c.get(key, tail, 5000) // clamped to the object end
			if err != nil || !bytes.Equal(got, body[tail:]) {
				t.Fatalf("%s tail: equal=%v err=%v", key, bytes.Equal(got, body[tail:]), err)
			}
		}
	}
	if _, err := c.get("never-put", 0, -1); err == nil {
		t.Fatal("get of a missing key succeeded")
	}
}

// TestGetDegradedOverNetwork: GETs reconstruct through m crashed holders
// and refuse cleanly past tolerance — over the real fetch path, where a
// down node is silence and timeouts, not a nil in a slice.
func TestGetDegradedOverNetwork(t *testing.T) {
	c := newCluster(t, 22, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	body := randomBody(22, 2<<20)
	if _, err := c.put("recon", body); err != nil {
		t.Fatalf("put: %v", err)
	}

	e, _ := c.entry("recon")
	holders, err := place.Nodes(e.Partition, c.members, 6)
	if err != nil {
		t.Fatal(err)
	}
	crashed := 0
	for _, h := range holders {
		if h != "n1" && h != "n2" && crashed < 2 {
			c.crash(h)
			crashed++
		}
	}
	got, err := c.get("recon", 0, -1)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("degraded whole get: equal=%v err=%v", bytes.Equal(got, body), err)
	}
	got, err = c.get("recon", 1<<20, 4096)
	if err != nil || !bytes.Equal(got, body[1<<20:1<<20+4096]) {
		t.Fatalf("degraded range get: err=%v", err)
	}

	for _, h := range holders {
		if h != "n1" && h != "n2" && !c.down[h] {
			c.crash(h)
			break
		}
	}
	if _, err := c.get("recon", 0, -1); !errors.Is(err, coord.ErrUnreadable) {
		t.Fatalf("get past tolerance: err=%v, want ErrUnreadable", err)
	}
}

// TestGetUnderFaultyNetwork: drops and duplicates between the coordinator
// and the shard holders; the fetch retries ride it out.
func TestGetUnderFaultyNetwork(t *testing.T) {
	c := newCluster(t, 23, sim.NetConfig{
		MinLatency: time.Millisecond, MaxLatency: 15 * time.Millisecond,
		DropProb: 0.03, DuplicateProb: 0.03,
	}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	body := randomBody(23, 1<<20+7)
	if _, err := c.put("gusty", body); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := c.get("gusty", 0, -1)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("faulty-net get: equal=%v err=%v", bytes.Equal(got, body), err)
	}
}

// sweep drives one repair sweep through the leader's coordinator.
func (c *cluster) sweep() coord.RepairReport {
	c.t.Helper()
	id := c.leader()
	var rep coord.RepairReport
	done := false
	c.worlds[id].Loop.Post(func() {
		c.nodes[id].co.RepairSweep(func(r coord.RepairReport, err error) {
			if err != nil {
				c.t.Errorf("sweep: %v", err)
			}
			rep, done = r, true
		})
	})
	for range 20000 {
		c.s.Run(tick)
		if done {
			return rep
		}
	}
	c.t.Fatal("sweep never finished")
	return rep
}

// corruptShard flips one payload byte of a committed shard file in place,
// leaving its commit marker — silent bitrot, as a disk would serve it.
func (c *cluster) corruptShard(key string, holderIdx int) seam.NodeID {
	c.t.Helper()
	e, ok := c.entry(key)
	if !ok {
		c.t.Fatalf("corruptShard: no entry for %s", key)
	}
	width := int(e.ECDataShards + e.ECParityShards)
	holders, err := place.Nodes(e.Partition, c.members, width)
	if err != nil {
		c.t.Fatal(err)
	}
	nid := holders[holderIdx]
	disk := c.worlds[nid].Disk
	name := datapath.ShardFileName(e.DataID, uint32(holderIdx))
	data, err := disk.ReadFile(name)
	if err != nil {
		c.t.Fatalf("corruptShard: %v", err)
	}
	data[len(data)-1] ^= 0xFF // a payload byte: headers stay plausible
	if err := disk.WriteFile(name, data); err != nil {
		c.t.Fatal(err)
	}
	if err := disk.Sync(name); err != nil {
		c.t.Fatal(err)
	}
	return nid
}

// emptyNodeShards removes every shard file (and marker) a node holds —
// the disk-swap scenario: same node, same identity, no data.
func (c *cluster) emptyNodeShards(nid seam.NodeID) int {
	c.t.Helper()
	disk := c.worlds[nid].Disk
	names, err := disk.List()
	if err != nil {
		c.t.Fatal(err)
	}
	removed := 0
	for _, n := range names {
		if !strings.HasPrefix(n, "shards/") {
			continue
		}
		if err := disk.Remove(n); err == nil {
			_ = disk.Sync(n)
		}
		if !strings.HasSuffix(n, ".ok") {
			removed++
		}
	}
	return removed
}

// TestRepairRebuildsEmptiedNode: a node comes back with no data (the
// disk-swap scenario) and a sweep restores every shard it should hold —
// then a second sweep finds nothing to do, and the bytes still decode.
func TestRepairRebuildsEmptiedNode(t *testing.T) {
	c := newCluster(t, 31, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	bodies := map[string][]byte{
		"big":   randomBody(31, 2<<20),
		"small": randomBody(32, 50<<10), // 1+2: full copies
		"other": randomBody(33, 700<<10),
	}
	for k, b := range bodies {
		if _, err := c.put(k, b); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	lost := c.emptyNodeShards("n4")
	rep := c.sweep()
	if rep.RebuiltShards != lost {
		t.Errorf("rebuilt %d shards, want %d (the emptied node's)", rep.RebuiltShards, lost)
	}
	if len(rep.Unrepairable) != 0 || len(rep.Failed) != 0 {
		t.Errorf("sweep report: %+v", rep)
	}

	rep = c.sweep()
	if rep.RebuiltShards != 0 || rep.Healthy != rep.Objects {
		t.Errorf("second sweep not clean: %+v", rep)
	}
	for k, b := range bodies {
		if got, err := c.get(k, 0, -1); err != nil || !bytes.Equal(got, b) {
			t.Fatalf("%s after repair: equal=%v err=%v", k, bytes.Equal(got, b), err)
		}
	}
}

// TestRepairHealsBitrot: silent rot on two different shards of one object
// on two different nodes — found by scrub with no read anywhere near it,
// rebuilt from the four clean shards, verified end to end.
func TestRepairHealsBitrot(t *testing.T) {
	c := newCluster(t, 34, sim.NetConfig{}, 6, profile(t, "4+2"))
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

// TestRepairReportsBeyondTolerance: three bad shards of six is past what
// k=4 survivors can rebuild. The sweep says so, touches nothing, and the
// next sweep says the same — no laundering, no destruction.
func TestRepairReportsBeyondTolerance(t *testing.T) {
	c := newCluster(t, 35, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	if _, err := c.put("doomed", randomBody(35, 1<<20)); err != nil {
		t.Fatalf("put: %v", err)
	}
	c.corruptShard("doomed", 0)
	c.corruptShard("doomed", 2)
	c.corruptShard("doomed", 5)

	rep := c.sweep()
	if rep.RebuiltShards != 0 || len(rep.Unrepairable) != 1 {
		t.Fatalf("sweep: %+v", rep)
	}
	if _, err := c.get("doomed", 0, -1); err == nil {
		t.Fatal("get of an unrepairable object returned data")
	}
}

// TestRepairCrashMidSweep: the repairing node dies mid-sweep; whatever it
// half-did is markerless garbage or already-committed truth, and a fresh
// sweep from the new leader converges.
func TestRepairCrashMidSweep(t *testing.T) {
	c := newCluster(t, 36, sim.NetConfig{MinLatency: 2 * time.Millisecond, MaxLatency: 5 * time.Millisecond}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	body := randomBody(36, 2<<20)
	if _, err := c.put("phoenix", body); err != nil {
		t.Fatalf("put: %v", err)
	}
	c.corruptShard("phoenix", 1)
	c.corruptShard("phoenix", 3)

	lead := c.leader()
	c.worlds[lead].Loop.Post(func() {
		c.nodes[lead].co.RepairSweep(func(coord.RepairReport, error) {
			t.Error("sweep finished on a node that crashed mid-sweep")
		})
	})
	c.s.Run(8 * time.Millisecond) // verifies in flight, rebuild not committed
	c.crash(lead)
	c.s.Run(time.Second)
	c.s.Restart(lead)
	c.down[lead] = false

	rep := c.sweep()
	if rep.RebuiltShards != 2 || len(rep.Failed) != 0 || len(rep.Unrepairable) != 0 {
		t.Fatalf("post-crash sweep: %+v", rep)
	}
	if got, err := c.get("phoenix", 0, -1); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("after crash+repair: equal=%v err=%v", bytes.Equal(got, body), err)
	}
}

// TestRepairDeterminism: a faulted repair run replays seed-exact.
func TestRepairDeterminism(t *testing.T) {
	run := func() coord.RepairReport {
		c := newCluster(t, 37, sim.NetConfig{
			MinLatency: time.Millisecond, MaxLatency: 10 * time.Millisecond,
			DropProb: 0.03, DuplicateProb: 0.03,
		}, 6, profile(t, "4+2"))
		c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
		if _, err := c.put("replayed", randomBody(37, 1<<20)); err != nil {
			t.Fatalf("put: %v", err)
		}
		c.corruptShard("replayed", 2)
		return c.sweep()
	}
	r1, r2 := run(), run()
	if r1.RebuiltShards != r2.RebuiltShards || r1.Healthy != r2.Healthy || len(r1.Failed) != len(r2.Failed) {
		t.Fatalf("same seed, different sweeps: %+v vs %+v", r1, r2)
	}
	if r1.RebuiltShards != 1 {
		t.Fatalf("expected one rebuilt shard: %+v", r1)
	}
}

// stubClock is the minimum seam.Clock for a Put that never schedules a timer
// — the no-layout refusal returns before any data-plane work.
type stubClock struct{}

func (stubClock) Now() time.Time                             { return time.Unix(0, 0) }
func (stubClock) AfterFunc(time.Duration, func()) seam.Timer { return nil }

// TestPutRefusesWithoutLayout proves a PUT before the first cluster layout is
// installed refuses transiently (SlowDown), not crashes — the forming-cluster
// case (ADR-0028).
func TestPutRefusesWithoutLayout(t *testing.T) {
	co := coord.New(coord.Config{
		Clock:  stubClock{},
		Rand:   rand.New(rand.NewPCG(1, 2)),
		Layout: func() (place.Layout, bool) { return place.Layout{}, false },
	})
	var gotErr error
	called := false
	co.Put("bucket", "key", []byte("hello"), coord.PutOptions{}, func(_ coord.PutResult, err error) {
		called, gotErr = true, err
	})
	if !called {
		t.Fatal("done was not called")
	}
	if !errors.Is(gotErr, coord.ErrRefused) {
		t.Fatalf("got %v, want ErrRefused", gotErr)
	}
}
