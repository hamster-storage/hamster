// Package sim is the deterministic simulation harness designed in
// docs/SIMULATION.md and committed to by ADR-0009.
//
// A Sim owns a global event queue ordered by virtual time and a seeded PRNG
// that makes every choice: message latency and ordering, drops, duplicates,
// and crash-time torn writes. Same seed, same binary — identical execution,
// event for event. Everything runs on the calling goroutine; a Sim is not
// safe for concurrent use, by design.
//
// Nodes live in the world the simulator provides: a virtual Clock, a faulty
// Transport, and a Disk with real crash semantics (unsynced writes may be
// lost or torn on restart). Each node is a single logical thread — the
// simulator dispatches one event at a time and runs it to completion.
package sim

import (
	"container/heap"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// Node is a node's core logic under simulation. Implementations are driven
// entirely by events: messages delivered here and timer callbacks scheduled
// through the node's Clock. Both arrive one at a time, so a Node needs no
// locking.
type Node interface {
	// HandleMessage delivers one message from the simulated network.
	HandleMessage(from seam.NodeID, msg []byte)
}

// BootFunc constructs a node's logic. It is called once when the node is
// added and again after every Restart, modeling process start: boot is the
// place to read the disk and schedule initial timers. The same World is
// passed every time — the Disk persists across restarts, and the Rand stream
// continues.
type BootFunc func(w *World) Node

// World is everything a node may touch, handed to its BootFunc. It is the
// simulated side of the seam (see internal/seam).
type World struct {
	ID        seam.NodeID
	Clock     seam.Clock
	Transport seam.Transport
	Disk      seam.Disk
	Rand      *rand.Rand
}

// Sim is the simulated world: virtual time, the event queue, the network,
// and the nodes. Create one with New.
type Sim struct {
	now   time.Time
	queue eventQueue
	seq   uint64
	rng   *rand.Rand
	net   NetConfig
	nodes map[seam.NodeID]*slot

	// blocked holds directional network partitions, checked at delivery
	// time so partitions also kill messages already in flight.
	blocked map[link]bool
}

type link struct {
	from, to seam.NodeID
}

// slot is the simulator's per-node bookkeeping. The epoch increments on
// every crash; timer callbacks capture the epoch they were scheduled in and
// are dropped if it has moved on, because a process's timers die with it.
type slot struct {
	id    seam.NodeID
	boot  BootFunc
	node  Node // nil while crashed
	epoch uint64
	disk  *disk
	world *World
}

// New creates a simulated world. The seed determines every random choice the
// world makes; replaying a seed replays the run exactly.
func New(seed uint64, net NetConfig) *Sim {
	net.validate()
	return &Sim{
		now:     time.Unix(0, 0).UTC(),
		rng:     rand.New(rand.NewPCG(seed, 0)),
		net:     net,
		nodes:   make(map[seam.NodeID]*slot),
		blocked: make(map[link]bool),
	}
}

// Now returns the current virtual time.
func (s *Sim) Now() time.Time { return s.now }

// AddNode creates a node with an empty disk and boots it. It panics if the
// ID is already in use — that is a bug in the test, not a simulated fault.
func (s *Sim) AddNode(id seam.NodeID, boot BootFunc) {
	if _, ok := s.nodes[id]; ok {
		panic(fmt.Sprintf("sim: duplicate node %q", id))
	}
	sl := &slot{id: id, boot: boot, disk: newDisk()}
	sl.world = &World{
		ID:        id,
		Clock:     &nodeClock{s: s, slot: sl},
		Transport: &nodeTransport{s: s, from: id},
		Disk:      sl.disk,
		Rand:      rand.New(rand.NewPCG(s.rng.Uint64(), s.rng.Uint64())),
	}
	s.nodes[id] = sl
	sl.node = boot(sl.world)
}

// Crash kills a node's process: its logic and timers are gone, and unsynced
// disk writes are lost or torn (the PRNG decides). The durable disk content
// survives for Restart. Crashing a node that is already down panics.
func (s *Sim) Crash(id seam.NodeID) {
	sl := s.mustSlot(id)
	if sl.node == nil {
		panic(fmt.Sprintf("sim: node %q is already crashed", id))
	}
	sl.node = nil
	sl.epoch++
	sl.disk.crash(s.rng)
}

// Restart boots a crashed node from its surviving disk, exactly as a real
// restart recovers from a real one. Restarting a running node panics.
func (s *Sim) Restart(id seam.NodeID) {
	sl := s.mustSlot(id)
	if sl.node != nil {
		panic(fmt.Sprintf("sim: node %q is already running", id))
	}
	sl.node = sl.boot(sl.world)
}

// Partition blocks message delivery from one node to another, in that
// direction only — call it twice for a symmetric partition. It applies to
// messages already in flight.
func (s *Sim) Partition(from, to seam.NodeID) {
	s.blocked[link{from, to}] = true
}

// Heal removes a directional partition installed by Partition.
func (s *Sim) Heal(from, to seam.NodeID) {
	delete(s.blocked, link{from, to})
}

// Run dispatches events in virtual-time order until the queue is empty or
// the next event lies beyond d from now, then advances the clock to exactly
// d from where it started. Idle virtual time costs nothing.
func (s *Sim) Run(d time.Duration) {
	deadline := s.now.Add(d)
	for len(s.queue) > 0 && !s.queue[0].at.After(deadline) {
		ev := heap.Pop(&s.queue).(*event)
		if ev.at.After(s.now) {
			s.now = ev.at
		}
		if ev.state == pending {
			ev.state = fired
			ev.fn()
		}
	}
	s.now = deadline
}

// schedule enqueues fn to run after delay. Negative delays clamp to now.
// The sequence number breaks virtual-time ties in scheduling order, keeping
// dispatch deterministic.
func (s *Sim) schedule(delay time.Duration, fn func()) *event {
	if delay < 0 {
		delay = 0
	}
	ev := &event{at: s.now.Add(delay), seq: s.seq, fn: fn}
	s.seq++
	heap.Push(&s.queue, ev)
	return ev
}

// deliver hands a message to its destination node, unless a partition
// blocks the link or the node is down.
func (s *Sim) deliver(from, to seam.NodeID, msg []byte) {
	if s.blocked[link{from, to}] {
		return
	}
	sl := s.nodes[to]
	if sl == nil || sl.node == nil {
		return
	}
	sl.node.HandleMessage(from, msg)
}

func (s *Sim) mustSlot(id seam.NodeID) *slot {
	sl := s.nodes[id]
	if sl == nil {
		panic(fmt.Sprintf("sim: unknown node %q", id))
	}
	return sl
}
