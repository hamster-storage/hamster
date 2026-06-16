package coord_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/ec"
	"github.com/hamster-storage/hamster/internal/keys"
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

	// Layout override for transition tests: when active is non-nil the layout's
	// member set is active (the new placement) and previous is the prior set, so
	// the getter resolves a mid-rebalance transition. Both nil = steady state
	// over members.
	active   []seam.NodeID
	previous []seam.NodeID
	// draining marks members the layout reports as draining out, lowering the
	// active count the profile and downsize re-encode follow.
	draining map[seam.NodeID]bool

	// encrypt and kek drive the encryption posture every node's coordinator
	// reads (ADR-0021). Off by default; encryptCluster turns it on with a
	// fixed KEK so the encrypted write/read path is exercised deterministically.
	encrypt bool
	kek     keys.KEK
	// keyring maps KEK fingerprint → loaded KEK, the rewrap sweep's view of the
	// keys this node holds (ADR-0032): the old key plus, during a rotation, the
	// new one. Populated by encryptCluster and rotateKeyBegin.
	keyring map[uint64]keys.KEK
}

// encReader is a deterministic per-node entropy source for DEK generation
// under the simulator — isolated from the world PRNG so minting a DEK never
// perturbs Raft or anything else that draws from it.
type encReader struct{ r *rand.Rand }

func (e encReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(e.r.Uint32())
	}
	return len(p), nil
}

// mkKEK loads a deterministic test KEK from a seed byte.
func (c *cluster) mkKEK(seed byte) keys.KEK {
	c.t.Helper()
	material := make([]byte, keys.KEKLen)
	for i := range material {
		material[i] = seed + byte(i)
	}
	k, err := keys.LoadKEK(material)
	if err != nil {
		c.t.Fatalf("LoadKEK: %v", err)
	}
	return k
}

// encryptCluster turns on the encryption posture with a fixed KEK. Call
// before writing the objects under test.
func (c *cluster) encryptCluster() {
	c.t.Helper()
	k := c.mkKEK(0x10)
	if c.keyring == nil {
		c.keyring = map[uint64]keys.KEK{}
	}
	c.keyring[k.Fingerprint().Uint64()] = k
	c.kek, c.encrypt = k, true
}

// rewrap drives one RewrapSweep through node id's coordinator to completion.
func (c *cluster) rewrap(id seam.NodeID) (coord.RewrapReport, error) {
	c.t.Helper()
	var rep coord.RewrapReport
	var rerr error
	done := false
	c.worlds[id].Loop.Post(func() {
		c.nodes[id].co.RewrapSweep(func(r coord.RewrapReport, e error) {
			rep, rerr, done = r, e, true
		})
	})
	for range 8000 {
		c.s.Run(tick)
		if done {
			return rep, rerr
		}
	}
	c.t.Fatal("rewrap never finished")
	return rep, rerr
}

// newCluster builds n nodes; the first min(n, 3) are Raft voters.
func newCluster(t *testing.T, seed uint64, net sim.NetConfig, n int, profile ec.Profile) *cluster {
	c := &cluster{
		t: t, s: sim.New(seed, net),
		raftIDs:  make(map[uint64]seam.NodeID),
		nodes:    make(map[seam.NodeID]*simNode),
		worlds:   make(map[seam.NodeID]*sim.World),
		down:     make(map[seam.NodeID]bool),
		draining: make(map[seam.NodeID]bool),
		profile:  profile,
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
			entropy := encReader{rand.New(rand.NewPCG(0xE0C, raftID))}
			n.co = coord.New(coord.Config{
				Clock: w.Clock, Rand: w.Rand, Data: n.data, Raft: rn,
				Entropy:    entropy,
				Encryption: func() (keys.KEK, bool) { return c.kek, c.encrypt },
				Keyring:    func(fp uint64) (keys.KEK, bool) { k, ok := c.keyring[fp]; return k, ok },
				Layout: func() (place.Layout, bool) {
					// Distinct host/zone per node: spread collapses to the
					// bare rendezvous ranking readObject verifies against. The
					// active/previous override drives transition tests.
					members := c.members
					if c.active != nil {
						members = c.active
					}
					l := place.Layout{
						Version:        1,
						PartitionCount: place.DefaultPartitionCount,
						Members:        placeNodes(members),
					}
					for i := range l.Members {
						if c.draining[l.Members[i].ID] {
							l.Members[i].Draining = true
						}
					}
					if c.previous != nil {
						l.Previous = placeNodes(c.previous)
					}
					return l, true
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

// putTimed is put, also returning how much simulated time the PUT itself
// took — the clock advance between posting it and its completion. Used to
// prove a known-down node is skipped (fast) rather than waited on (the full
// retransmit budget).
func (c *cluster) putTimed(key string, body []byte) (coord.PutResult, error, time.Duration) {
	c.t.Helper()
	id := c.leader()
	start := c.worlds[id].Clock.Now()
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
			return res, perr, c.worlds[id].Clock.Now().Sub(start)
		}
	}
	c.t.Fatal("put never finished")
	return res, perr, 0
}

// repair drives one RepairSweep through node id's coordinator to completion.
func (c *cluster) repair(id seam.NodeID) (coord.RepairReport, error) {
	c.t.Helper()
	var rep coord.RepairReport
	var rerr error
	done := false
	c.worlds[id].Loop.Post(func() {
		c.nodes[id].co.RepairSweep(func(r coord.RepairReport, e error) {
			rep, rerr, done = r, e, true
		})
	})
	for range 8000 {
		c.s.Run(tick)
		if done {
			return rep, rerr
		}
	}
	c.t.Fatal("repair never finished")
	return rep, rerr
}

// downNodes reads node id's coordinator liveness view on its loop.
func (c *cluster) downNodes(id seam.NodeID) []seam.NodeID {
	c.t.Helper()
	var out []seam.NodeID
	c.worlds[id].Loop.Post(func() { out = c.nodes[id].co.DownNodes() })
	c.s.Run(0) // single-threaded sim: dispatch the posted read now
	return out
}

// idle advances the simulation by n ticks with no new work posted — long
// enough for an outstanding operation's retransmit budget to lapse.
func (c *cluster) idle(n int) {
	for range n {
		c.s.Run(tick)
	}
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
	// An encrypted object's shards are ciphertext: unwrap its DEK from the
	// committed record to decode the plaintext, exactly as a GET does.
	var dek []byte
	if e.EncAlgorithm != meta.EncNone {
		d, err := c.kek.Unwrap(e.WrappedDEK)
		if err != nil {
			return nil, err
		}
		dek = d.Bytes()
	}
	er, err := ec.NewReader(shards)
	if err != nil {
		return nil, err
	}
	sr, err := stream.NewReader(er, er.FrameSize(), dek)
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

// rawShardBytes returns the on-disk bytes of one committed shard, for the
// confidentiality check that an encrypted object's shards are ciphertext.
func (c *cluster) rawShardBytes(key string, shard int) []byte {
	c.t.Helper()
	e, ok := c.entry(key)
	if !ok {
		c.t.Fatalf("no entry for %q", key)
	}
	width := int(e.ECDataShards + e.ECParityShards)
	nodes, err := place.Nodes(e.Partition, c.members, width)
	if err != nil {
		c.t.Fatal(err)
	}
	disk := c.worlds[nodes[shard]].Disk
	data, err := disk.ReadFile(datapath.ShardFileName(e.DataID, uint32(shard)))
	if err != nil {
		c.t.Fatalf("reading shard %d: %v", shard, err)
	}
	return data
}

func (c *cluster) crash(id seam.NodeID) {
	c.s.Crash(id)
	c.down[id] = true
}

// placeNodes labels each ID with a distinct host/zone (own ID), so spread
// collapses to the bare rendezvous ranking the tests reason about.
func placeNodes(ids []seam.NodeID) []place.Node {
	out := make([]place.Node, len(ids))
	for i, id := range ids {
		out[i] = place.Node{ID: id, Host: string(id), Zone: string(id)}
	}
	return out
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

// TestPutSkipsDownNode: a node crashes; the first PUT that targets it pays its
// retransmit timeout (that attempt is how the passive detector learns it is
// down), but the next PUT skips it and completes promptly. Both commit at the
// k+1 floor with five durable shards, and both objects read back — the skip
// changes latency, never durability. With 4+2 on six nodes every object uses
// all six, so the down node is always a holder.
func TestPutSkipsDownNode(t *testing.T) {
	c := newCluster(t, 7, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	c.crash("n6") // data-only: quorum untouched

	body1 := randomBody(70, 1<<20)
	res1, err, slow := c.putTimed("first", body1)
	if err != nil {
		t.Fatalf("first put: %v", err)
	}
	if res1.Durable != 5 {
		t.Errorf("first put durable %d, want 5", res1.Durable)
	}

	body2 := randomBody(71, 1<<20)
	res2, err, fast := c.putTimed("second", body2)
	if err != nil {
		t.Fatalf("second put: %v", err)
	}
	if res2.Durable != 5 {
		t.Errorf("second put durable %d, want 5", res2.Durable)
	}

	// The first put paid the down node's full retransmit budget (maxAttempts *
	// rto = 20 * 500ms = 10s in the datapath defaults); the second skipped it.
	// Generous margins around that 10s so the contrast, not the exact figure,
	// is what is asserted.
	if slow < 9*time.Second {
		t.Fatalf("first put took %v; expected to pay the ~10s timeout, else the test cannot prove the skip", slow)
	}
	if fast >= 5*time.Second {
		t.Fatalf("second put took %v; the down node was not skipped (it paid a timeout)", fast)
	}

	for name, want := range map[string][]byte{"first": body1, "second": body2} {
		if got, err := c.readObject(name); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s read back: equal=%v err=%v", name, bytes.Equal(got, want), err)
		}
	}
}

// TestGetFeedsLiveness: a GET learns a holder is down from its own fetch
// outcomes, not just a PUT. The read still returns fast off the five live
// shards (it abandons the straggler); the crashed holder's header fetch times
// out a little later and feeds the detector, so a subsequent PUT would skip
// it. A live holder touched by the same read is never marked down.
func TestGetFeedsLiveness(t *testing.T) {
	c := newCluster(t, 8, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	body := randomBody(80, 1<<20)
	if _, err := c.put("obj", body); err != nil {
		t.Fatalf("put: %v", err)
	}

	lead := c.leader()
	if d := c.downNodes(lead); len(d) != 0 {
		t.Fatalf("detector recorded a node down before any failure: %v", d)
	}
	c.crash("n6") // a data-plane holder; the metadata quorum is untouched

	got, err := c.get("obj", 0, -1)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("degraded get: equal=%v err=%v", bytes.Equal(got, body), err)
	}
	// The read is done, but n6's header fetch is still retrying; advance past
	// its retransmit budget (maxAttempts*rto = 10s) so it gives up and the
	// outcome reaches the detector.
	c.idle(1500)

	d := c.downNodes(lead)
	if !slices.Contains(d, seam.NodeID("n6")) {
		t.Fatalf("GET did not record the crashed holder n6 down: %v", d)
	}
	if slices.Contains(d, seam.NodeID("n1")) {
		t.Fatalf("GET marked a live holder down: %v", d)
	}
}

// TestRepairFeedsLiveness: a repair sweep's scrub touches every holder, so a
// node that never answers its verify is learned down — repair feeds the same
// detector PUT and GET do.
func TestRepairFeedsLiveness(t *testing.T) {
	c := newCluster(t, 9, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	if _, err := c.put("obj", randomBody(90, 1<<20)); err != nil {
		t.Fatalf("put: %v", err)
	}

	lead := c.leader()
	c.crash("n6")
	if d := c.downNodes(lead); len(d) != 0 {
		t.Fatalf("detector recorded a node down before the sweep: %v", d)
	}

	// The sweep verifies all six holders; n6 never answers (it rebuilds onto
	// the live cluster, but the down node's own shard cannot be reinstalled
	// while it is gone — that is the next sweep's job). We assert only that
	// the unreachable holder reached the detector.
	if _, err := c.repair(lead); err != nil {
		t.Fatalf("repair sweep: %v", err)
	}
	d := c.downNodes(lead)
	if !slices.Contains(d, seam.NodeID("n6")) {
		t.Fatalf("repair did not record the crashed holder n6 down: %v", d)
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

// TestGetDualReadAcrossTransition: a layout change relocates shards (placement
// is positional and derived from the member set), which would make existing
// objects unreadable — unless GET dual-reads. With the prior member set carried
// as the transition's Previous, every object written before the change still
// reads, fetched from its old home until repair migrates it. The control shows
// the hazard is real: drop Previous and reads fail.
func TestGetDualReadAcrossTransition(t *testing.T) {
	c := newCluster(t, 41, sim.NetConfig{}, 4, profile(t, "2+1")) // width 3 of 4 nodes
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	bodies := map[string][]byte{}
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("obj-%d", i)
		body := randomBody(uint64(100+i), 200<<10)
		if _, err := c.put(key, body); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		bodies[key] = body
	}

	// Open a transition draining n4: the new placement is the three remaining
	// nodes, the old placement the full four. Dual-read must keep every object
	// readable — found at its old home, which repair has not yet migrated.
	c.active = []seam.NodeID{"n1", "n2", "n3"}
	c.previous = c.members // {n1,n2,n3,n4}
	for key, body := range bodies {
		got, err := c.get(key, 0, -1)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("dual-read %s across transition: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
	}

	// Control: the same new placement without the transition. Positional
	// addressing now points at the wrong nodes, so reads fail — this is exactly
	// the hazard dual-read removes.
	c.previous = nil
	failed := 0
	for key := range bodies {
		if _, err := c.get(key, 0, -1); err != nil {
			failed++
		}
	}
	if failed == 0 {
		t.Fatal("expected objects to be unreadable without dual-read (the hazard); none were")
	}
}

// TestRepairMigratesAcrossTransition: the write-side counterpart of dual-read.
// A transition relocates shards (placement is positional), and repair carries
// every shard from its old home to its new one — a copy, not a reconstruct.
// After a sweep, dropping the transition (and crashing the old-only node) must
// leave every object readable purely from its new placement: proof the bytes
// moved, not that the old node still answers. A second sweep migrates nothing,
// which is the signal that closes the transition.
func TestRepairMigratesAcrossTransition(t *testing.T) {
	c := newCluster(t, 43, sim.NetConfig{}, 4, profile(t, "2+1")) // width 3 of 4 nodes
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	bodies := map[string][]byte{}
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("mig-%d", i)
		body := randomBody(uint64(300+i), 200<<10)
		if _, err := c.put(key, body); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		bodies[key] = body
	}

	// Open a transition draining n4: new placement is the three remaining nodes,
	// old placement the full four. Repair must migrate every shard to its new
	// home.
	c.active = []seam.NodeID{"n1", "n2", "n3"}
	c.previous = c.members // {n1,n2,n3,n4}

	rep := c.sweep()
	if rep.Objects != 8 {
		t.Fatalf("swept %d objects, want 8", rep.Objects)
	}
	if rep.MigratedShards == 0 {
		t.Fatal("transition migrated no shards; expected positional relocation to move some")
	}
	if len(rep.Unrepairable) != 0 || len(rep.Failed) != 0 {
		t.Fatalf("migration left work: unrepairable=%v failed=%v", rep.Unrepairable, rep.Failed)
	}

	// A second sweep over the same open transition must find everything already
	// at its new home: nothing to migrate, every object healthy. That zero is
	// the convergence signal a future reconcile uses to close the transition.
	rep = c.sweep()
	if rep.MigratedShards != 0 || rep.RebuiltShards != 0 {
		t.Fatalf("second sweep still moved shards: migrated=%d rebuilt=%d", rep.MigratedShards, rep.RebuiltShards)
	}
	if rep.Healthy != 8 {
		t.Fatalf("second sweep healthy=%d, want 8", rep.Healthy)
	}

	// Close the transition and remove the drained node entirely. Every object
	// must still read from its new placement alone — the migration populated it.
	c.active = nil
	c.previous = nil
	c.members = []seam.NodeID{"n1", "n2", "n3"}
	c.crash("n4")
	for key, body := range bodies {
		got, err := c.readObject(key)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("post-migration read %s off disk: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
		net, err := c.get(key, 0, -1)
		if err != nil || !bytes.Equal(net, body) {
			t.Fatalf("post-migration GET %s: equal=%v err=%v", key, bytes.Equal(net, body), err)
		}
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

// optimize drives one Optimize sweep through the leader's coordinator to
// completion — a repair sweep that also re-encodes under-width objects up to the
// active profile.
func (c *cluster) optimize() coord.RepairReport {
	c.t.Helper()
	id := c.leader()
	var rep coord.RepairReport
	done := false
	c.worlds[id].Loop.Post(func() {
		c.nodes[id].co.Optimize(func(r coord.RepairReport, err error) {
			if err != nil {
				c.t.Errorf("optimize: %v", err)
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
	c.t.Fatal("optimize never finished")
	return rep
}

// startScrub / stopScrub / scrubStats drive the background scrubber on node id's
// coordinator from the test goroutine, each on the node's loop.
func (c *cluster) startScrub(id seam.NodeID, cfg coord.ScrubConfig) {
	c.worlds[id].Loop.Post(func() { c.nodes[id].co.StartScrub(cfg) })
	c.s.Run(0)
}

func (c *cluster) stopScrub(id seam.NodeID) {
	c.worlds[id].Loop.Post(func() { c.nodes[id].co.StopScrub() })
	c.s.Run(0)
}

func (c *cluster) scrubStats(id seam.NodeID) (scrubbed, healed, passes int) {
	c.worlds[id].Loop.Post(func() { scrubbed, healed, passes = c.nodes[id].co.ScrubStats() })
	c.s.Run(0)
	return
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

// reencode drives one ReEncode through the leader's coordinator to completion.
func (c *cluster) reencode(key string, e meta.VersionEntry, newK, newM int) {
	c.t.Helper()
	id := c.leader()
	var rerr error
	done := false
	c.worlds[id].Loop.Post(func() {
		c.nodes[id].co.ReEncode(bucket, key, e, newK, newM, func(err error) { rerr, done = err, true })
	})
	for range 8000 {
		c.s.Run(tick)
		if done {
			break
		}
	}
	if !done {
		c.t.Fatal("re-encode never finished")
	}
	if rerr != nil {
		c.t.Fatalf("re-encode: %v", rerr)
	}
}

// TestReEncodeShrinksProfile: re-encode rewrites a 4+2 object to 3+2 — the data
// step a 6→5 node downsize takes. The object reads back identical at the new
// profile, over the network and off the disks (the new, narrower placement),
// and the old shards are gone.
func TestReEncodeShrinksProfile(t *testing.T) {
	c := newCluster(t, 51, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	body := randomBody(51, 600<<10)
	if _, err := c.put("obj", body); err != nil {
		t.Fatalf("put: %v", err)
	}

	e, ok := c.entry("obj")
	if !ok || e.ECDataShards != 4 || e.ECParityShards != 2 {
		t.Fatalf("expected a 4+2 object, got %d+%d (ok %v)", e.ECDataShards, e.ECParityShards, ok)
	}

	c.reencode("obj", e, 3, 2)

	e2, _ := c.entry("obj")
	if e2.ECDataShards != 3 || e2.ECParityShards != 2 {
		t.Fatalf("re-encode to 3+2 failed: now %d+%d", e2.ECDataShards, e2.ECParityShards)
	}
	if e2.DataID == e.DataID {
		t.Fatal("re-encode must mint a new DataID for the new shards")
	}
	if len(e2.ShardChecksums) != 5 {
		t.Fatalf("re-encoded object has %d shard checksums, want 5", len(e2.ShardChecksums))
	}

	got, err := c.get("obj", 0, -1)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("GET after re-encode: equal=%v err=%v", bytes.Equal(got, body), err)
	}
	disk, err := c.readObject("obj")
	if err != nil || !bytes.Equal(disk, body) {
		t.Fatalf("off-disk read after re-encode: equal=%v err=%v", bytes.Equal(disk, body), err)
	}
}

// TestRepairDownsizeReEncodes: draining a node from a 6-node 4+2 cluster drops
// the active count to 5, so every width-6 object no longer fits. The repair
// sweep re-encodes each to 3+2 — the data step of a downsize — converging in one
// pass; a second sweep re-encodes nothing. With the drained node then crashed,
// every object still reads identical, proving its data moved entirely onto the
// five active nodes.
func TestRepairDownsizeReEncodes(t *testing.T) {
	c := newCluster(t, 53, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	bodies := map[string][]byte{}
	for i := 0; i < 6; i++ {
		key := fmt.Sprintf("obj-%d", i)
		body := randomBody(uint64(200+i), 300<<10)
		if _, err := c.put(key, body); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		bodies[key] = body
		if e, _ := c.entry(key); e.ECDataShards != 4 || e.ECParityShards != 2 {
			t.Fatalf("%s is %d+%d, want 4+2", key, e.ECDataShards, e.ECParityShards)
		}
	}

	// Drain n6: a downsize opens a transition (Previous is the pre-drain member
	// set, where the existing 4+2 shards sit), and the active count drops to 5,
	// so every width-6 object must re-encode to 3+2 to fit.
	c.draining["n6"] = true
	c.previous = c.members

	rep := c.sweep()
	if rep.ReEncoded != 6 || len(rep.Failed) != 0 {
		t.Fatalf("downsize sweep: re-encoded=%d failed=%v, want 6 / none", rep.ReEncoded, rep.Failed)
	}
	rep = c.sweep()
	if rep.ReEncoded != 0 {
		t.Fatalf("second sweep still re-encoded %d (should have converged)", rep.ReEncoded)
	}

	// The drained node is now dead weight: crash it and every object still reads,
	// at its new 3+2 profile, off the five active nodes.
	c.crash("n6")
	for key, body := range bodies {
		if e, _ := c.entry(key); e.ECDataShards != 3 || e.ECParityShards != 2 {
			t.Fatalf("%s not re-encoded to 3+2: %d+%d", key, e.ECDataShards, e.ECParityShards)
		}
		got, err := c.get(key, 0, -1)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("post-downsize GET %s: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
	}
}

// TestRepairUpsizeReEncodes is the downsize's mirror: objects written over a
// three-node set (2+1) and then spread onto a grown six-node cluster are widened
// to 4+2 — but only by an explicit Optimize, never a plain repair sweep. Growth
// first migrates the width-3 shards to their six-node home (a transition); then
// Optimize re-encodes each up to 4+2, converging in one pass. With two nodes
// crashed every object still reads identical — durability the original 2+1 could
// not give, the whole point of growing into the larger cluster.
func TestRepairUpsizeReEncodes(t *testing.T) {
	c := newCluster(t, 61, sim.NetConfig{}, 6, profile(t, "2+1"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	// Write over a three-node member set: objects land 2+1 (width 3).
	full := c.members
	c.members = []seam.NodeID{"n1", "n2", "n3"}
	bodies := map[string][]byte{}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("obj-%d", i)
		body := randomBody(uint64(300+i), 300<<10)
		if _, err := c.put(key, body); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		bodies[key] = body
		if e, _ := c.entry(key); e.ECDataShards != 2 || e.ECParityShards != 1 {
			t.Fatalf("%s is %d+%d, want 2+1", key, e.ECDataShards, e.ECParityShards)
		}
	}

	// Grow to six nodes: growth opens a transition (the prior three-node set is
	// where the width-3 shards sit), and a sweep migrates them to their six-node
	// home so they are readable at the current placement.
	c.previous = []seam.NodeID{"n1", "n2", "n3"}
	c.members = full
	rep := c.sweep()
	if len(rep.Failed) != 0 || len(rep.Unrepairable) != 0 {
		t.Fatalf("migrate sweep: failed=%v unrepairable=%v", rep.Failed, rep.Unrepairable)
	}
	c.previous = nil // the transition converged and closed

	// A plain repair sweep widens nothing — growth never auto-re-encodes.
	if rep := c.sweep(); rep.ReEncoded != 0 {
		t.Fatalf("repair sweep widened %d objects; upsize must be explicit", rep.ReEncoded)
	}

	// Optimize widens every width-3 object to 4+2, converging in one pass; a
	// second optimize re-encodes nothing.
	rep = c.optimize()
	if rep.ReEncoded != 5 || len(rep.Failed) != 0 {
		t.Fatalf("optimize: re-encoded=%d failed=%v, want 5 / none", rep.ReEncoded, rep.Failed)
	}
	if rep := c.optimize(); rep.ReEncoded != 0 {
		t.Fatalf("second optimize re-encoded %d (should have converged)", rep.ReEncoded)
	}

	// Now 4+2: crash two nodes and every object still reads identical.
	for key := range bodies {
		if e, _ := c.entry(key); e.ECDataShards != 4 || e.ECParityShards != 2 {
			t.Fatalf("%s not re-encoded to 4+2: %d+%d", key, e.ECDataShards, e.ECParityShards)
		}
	}
	c.crash("n5")
	c.crash("n6")
	for key, body := range bodies {
		got, err := c.get(key, 0, -1)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("post-optimize GET %s with two nodes down: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
	}
}

// TestScrubHealsContinuously: the background scrubber finds and rebuilds bitrot on
// its own, without any operator sweep. Every object's shard is corrupted in
// place; the scrubber, started on the leader and paced object-by-object, walks
// the keyspace and rebuilds each. Afterward a manual sweep finds nothing left to
// heal — the scrubber converged it — and every object reads identical.
func TestScrubHealsContinuously(t *testing.T) {
	c := newCluster(t, 71, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	bodies := map[string][]byte{}
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("obj-%d", i)
		body := randomBody(uint64(400+i), 300<<10)
		if _, err := c.put(key, body); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		bodies[key] = body
	}
	// Bitrot one shard of every object — silent corruption no read has tripped yet.
	for key := range bodies {
		c.corruptShard(key, 1)
	}

	// Start the continuous scrubber on the leader and let virtual time run. No
	// operator sweep is ever called; the scrubber must heal everything on its own.
	lead := c.leader()
	c.startScrub(lead, coord.ScrubConfig{Pace: 30 * time.Millisecond, PassInterval: 100 * time.Millisecond})
	healed := 0
	for range 200 {
		c.idle(50)
		if _, healed, _ = c.scrubStats(lead); healed >= len(bodies) {
			break
		}
	}
	if healed < len(bodies) {
		t.Fatalf("scrubber healed %d of %d objects before the budget ran out", healed, len(bodies))
	}
	_, _, passes := c.scrubStats(lead)
	if passes < 1 {
		t.Fatalf("scrubber reported %d full passes, want at least 1", passes)
	}

	// Stop it and drain any in-flight scrub, then a manual sweep must find nothing
	// to rebuild — proof the scrubber already converged the cluster.
	c.stopScrub(lead)
	c.idle(200)
	rep := c.sweep()
	if rep.RebuiltShards != 0 || len(rep.Failed) != 0 || rep.Healthy != len(bodies) {
		t.Fatalf("post-scrub sweep: rebuilt=%d healthy=%d failed=%v, want 0 / %d / none",
			rep.RebuiltShards, rep.Healthy, rep.Failed, len(bodies))
	}
	for key, body := range bodies {
		got, err := c.get(key, 0, -1)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("post-scrub GET %s: equal=%v err=%v", key, bytes.Equal(got, body), err)
		}
	}
}

// TestScrubYieldsDuringTransition: the scrubber stands aside while a layout
// transition is open — migration is driveTransitionClose's to drive, not the
// background scrub's. With a transition open it examines nothing, even with
// corruption present; once the transition closes it heals.
func TestScrubYieldsDuringTransition(t *testing.T) {
	c := newCluster(t, 73, sim.NetConfig{}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	body := randomBody(7, 300<<10)
	if _, err := c.put("obj", body); err != nil {
		t.Fatalf("put: %v", err)
	}
	c.corruptShard("obj", 1)

	// Open a transition (Previous set) and run the scrubber: it must yield —
	// examine nothing, heal nothing — for as long as the transition is open.
	c.previous = c.members
	lead := c.leader()
	c.startScrub(lead, coord.ScrubConfig{Pace: 20 * time.Millisecond, PassInterval: 50 * time.Millisecond})
	c.idle(500)
	if scrubbed, healed, _ := c.scrubStats(lead); scrubbed != 0 || healed != 0 {
		t.Fatalf("scrubber ran during an open transition: scrubbed=%d healed=%d, want 0/0", scrubbed, healed)
	}

	// Close the transition; the scrubber resumes and heals the corruption.
	c.previous = nil
	healed := 0
	for range 100 {
		c.idle(50)
		if _, healed, _ = c.scrubStats(lead); healed >= 1 {
			break
		}
	}
	if healed < 1 {
		t.Fatal("scrubber did not resume after the transition closed")
	}
}
