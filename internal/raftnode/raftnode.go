// Package raftnode drives etcd-io/raft's RawNode over the seam: virtual or
// real time through seam.Clock, the faulty or real network through
// seam.Transport, durability through a wal.Log on a seam.Disk. The library
// is the inert consensus state machine ADR-0012 chose; this package is the
// assembly it left to us — the write-ahead log, the transport glue, apply,
// and (ADR-0024) the election timer.
//
// The metadata store is rebuilt at boot by replaying the committed prefix
// of the Raft log into a fresh meta.Store: the log is the durability, the
// store is a deterministic function of it. Snapshots arrive later to
// compact the replay; until then the log only grows.
//
// A Node is owned by its event loop, like every core component: every
// method, timer callback, and message must run on the node's single
// logical thread. Nothing here locks.
package raftnode

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/wal"
)

// ErrNotLeader rejects a proposal made anywhere but the leader. Proposals
// are not forwarded (DisableProposalForwarding, below): the caller routes
// to the leader, which is the only place a result callback can be paired
// with a log index.
var ErrNotLeader = errors.New("raftnode: not the leader")

// Config assembles a Node. All fields are required.
type Config struct {
	// ID is this node's Raft ID; Peers maps every cluster member's Raft ID
	// to its transport address, this node included. v0.2's first cut is a
	// static cluster — membership changes arrive with cluster join.
	ID    uint64
	Peers map[uint64]seam.NodeID

	Clock     seam.Clock
	Transport seam.Transport
	Disk      seam.Disk
	Rand      *rand.Rand

	// TickInterval drives heartbeats (one heartbeat tick each). The
	// election timeout is ElectionTicks plus jitter of up to the same
	// again, drawn from Rand per ADR-0024.
	TickInterval  time.Duration
	ElectionTicks int
}

// Node is one Raft-replicated metadata replica.
type Node struct {
	cfg     Config
	rn      *raft.RawNode
	storage *raft.MemoryStorage
	log     *wal.Log
	store   *meta.Store

	applied   uint64
	waiters   map[uint64][]func(any, error) // log index → callbacks for this node's proposals
	proposing []func(any, error)            // accepted by Propose, not yet paired to an index

	lastHeard     time.Time     // last leader contact (or own leadership)
	electionAfter time.Duration // silence budget before campaigning; re-drawn per campaign
}

// New boots a node from its disk: an empty log bootstraps a fresh cluster
// from Peers; anything else is a restart, replayed. The returned node has
// scheduled its ticks and is ready for messages.
func New(cfg Config) (*Node, error) {
	n := &Node{
		cfg:     cfg,
		storage: raft.NewMemoryStorage(),
		waiters: make(map[uint64][]func(any, error)),
		store:   meta.NewStore(),
	}

	log, records, err := wal.Open(cfg.Disk, "raft/log")
	if err != nil {
		return nil, err
	}
	n.log = log
	for i, rec := range records {
		hs, entries, err := decodeRecord(rec)
		if err != nil {
			return nil, fmt.Errorf("raftnode: replaying record %d: %w", i, err)
		}
		if err := n.storage.Append(entries); err != nil {
			return nil, fmt.Errorf("raftnode: replaying record %d: %w", i, err)
		}
		if !raft.IsEmptyHardState(hs) {
			if err := n.storage.SetHardState(hs); err != nil {
				return nil, fmt.Errorf("raftnode: replaying record %d: %w", i, err)
			}
		}
	}
	if err := n.applyCommitted(); err != nil {
		return nil, err
	}

	rc := &raft.Config{
		ID:      cfg.ID,
		Storage: n.storage,
		// ADR-0024: the library's internal election timer must never fire —
		// its randomized timeout draws crypto entropy the simulator cannot
		// seed. This package owns the election clock instead.
		ElectionTick:  math.MaxInt32,
		HeartbeatTick: 1,
		PreVote:       true,
		// Proposals are leader-only by construction (ErrNotLeader), which
		// keeps the waiter↔index pairing exact; forwarding would create
		// entries this node cannot account for.
		DisableProposalForwarding: true,
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           256,
		Applied:                   n.applied,
		Logger:                    quietLogger{},
	}
	rn, err := raft.NewRawNode(rc)
	if err != nil {
		return nil, fmt.Errorf("raftnode: %w", err)
	}
	n.rn = rn

	if len(records) == 0 {
		// A fresh cluster. Sorted: map order must not shape the log.
		peers := make([]raft.Peer, 0, len(cfg.Peers))
		for _, id := range slices.Sorted(maps.Keys(cfg.Peers)) {
			peers = append(peers, raft.Peer{ID: id})
		}
		if err := rn.Bootstrap(peers); err != nil {
			return nil, fmt.Errorf("raftnode: bootstrap: %w", err)
		}
	}

	n.lastHeard = cfg.Clock.Now()
	n.rearmElection()
	cfg.Clock.AfterFunc(cfg.TickInterval, n.onTick)
	n.processReady()
	return n, nil
}

// Store is the replica's metadata state, for reads. Loop-owned, like the
// node itself.
func (n *Node) Store() *meta.Store { return n.store }

// Leader reports the node's view of leadership: the leader's Raft ID
// (zero when unknown) and whether this node is it.
func (n *Node) Leader() (uint64, bool) {
	st := n.rn.Status()
	return st.Lead, st.RaftState == raft.StateLeader
}

// Propose submits one metadata proposal on the leader. done is called on
// the node's loop exactly once if the proposal commits — with apply's
// deterministic result and error — and never if leadership is lost first;
// the caller owns timeouts and retries. On a non-leader it fails fast with
// ErrNotLeader.
func (n *Node) Propose(p any, done func(any, error)) {
	if _, leader := n.Leader(); !leader {
		done(nil, ErrNotLeader)
		return
	}
	n.proposing = append(n.proposing, done)
	if err := n.rn.Propose(meta.EncodeProposal(p)); err != nil {
		n.proposing = n.proposing[:len(n.proposing)-1]
		done(nil, fmt.Errorf("raftnode: propose: %w", err))
		return
	}
	n.processReady()
}

// HandleMessage implements seam.MessageHandler: one Raft message off the
// wire.
func (n *Node) HandleMessage(from seam.NodeID, msg []byte) {
	var m raftpb.Message
	if err := m.Unmarshal(msg); err != nil {
		return // a corrupt frame is dropped; the sender will retry
	}
	switch m.Type {
	case raftpb.MsgApp, raftpb.MsgHeartbeat, raftpb.MsgSnap:
		// Leader-origin traffic: the election clock resets.
		n.lastHeard = n.cfg.Clock.Now()
	}
	if err := n.rn.Step(m); err != nil {
		return // e.g. a message from a removed peer; raft says ignore
	}
	n.processReady()
}

// onTick drives heartbeats and the ADR-0024 election clock.
func (n *Node) onTick() {
	n.cfg.Clock.AfterFunc(n.cfg.TickInterval, n.onTick)
	n.rn.Tick()

	now := n.cfg.Clock.Now()
	if _, leader := n.Leader(); leader {
		n.lastHeard = now
	} else if now.Sub(n.lastHeard) >= n.electionAfter {
		// Silence outlasted the randomized timeout: campaign. PreVote
		// makes a wrong guess a probe, not a disruption.
		_ = n.rn.Campaign()
		n.lastHeard = now
		n.rearmElection()
	}
	n.processReady()
}

// rearmElection draws the next silence budget: a full timeout plus up to
// another timeout of jitter, in ticks, from the node's seeded PRNG — the
// draw ADR-0024 moved out of the library.
func (n *Node) rearmElection() {
	ticks := n.cfg.ElectionTicks + n.cfg.Rand.IntN(n.cfg.ElectionTicks)
	n.electionAfter = time.Duration(ticks) * n.cfg.TickInterval
}

// processReady drains the RawNode: persist, send, apply, advance — the
// synchronous Ready loop from ADR-0012, repeated until quiet.
func (n *Node) processReady() {
	for n.rn.HasReady() {
		rd := n.rn.Ready()

		// 1. Durability first: hard state and entries hit the WAL before
		// anything that depends on them leaves the node.
		if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 {
			if err := n.log.Append(encodeRecord(rd.HardState, rd.Entries)); err != nil {
				// A node that cannot persist cannot participate. Storage
				// failure handling is a later pass; for now this is loud.
				panic(fmt.Sprintf("raftnode %d: wal append: %v", n.cfg.ID, err))
			}
			if err := n.storage.Append(rd.Entries); err != nil {
				panic(fmt.Sprintf("raftnode %d: storage append: %v", n.cfg.ID, err))
			}
			if !raft.IsEmptyHardState(rd.HardState) {
				if err := n.storage.SetHardState(rd.HardState); err != nil {
					panic(fmt.Sprintf("raftnode %d: hard state: %v", n.cfg.ID, err))
				}
			}
		}
		n.claimIndexes(rd.Entries)

		// 2. Messages, only after persistence.
		for _, m := range rd.Messages {
			to, ok := n.cfg.Peers[m.To]
			if !ok {
				continue
			}
			data, err := m.Marshal()
			if err != nil {
				panic(fmt.Sprintf("raftnode %d: marshal message: %v", n.cfg.ID, err))
			}
			n.cfg.Transport.Send(to, data)
		}

		// 3. Apply what committed.
		for _, e := range rd.CommittedEntries {
			n.applyEntry(e)
		}

		n.rn.Advance(rd)
	}
}

// claimIndexes pairs this node's in-flight proposals with the log indexes
// they landed at. The leader appends proposals to rd.Entries in propose
// order, and proposals are leader-only, so pending callbacks pair with new
// normal entries FIFO.
func (n *Node) claimIndexes(entries []raftpb.Entry) {
	for _, e := range entries {
		if len(n.proposing) == 0 {
			return
		}
		if e.Type != raftpb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		n.waiters[e.Index] = append(n.waiters[e.Index], n.proposing[0])
		n.proposing = n.proposing[1:]
	}
}

// applyEntry applies one committed entry to the store and settles any
// local waiter. Apply outcomes — results and validation errors alike — are
// deterministic, computed identically on every replica; only the proposing
// node has a callback to deliver them to.
func (n *Node) applyEntry(e raftpb.Entry) {
	n.applied = e.Index
	switch e.Type {
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(e.Data); err != nil {
			panic(fmt.Sprintf("raftnode %d: conf change at %d: %v", n.cfg.ID, e.Index, err))
		}
		if n.rn != nil {
			n.rn.ApplyConfChange(cc)
		}
	case raftpb.EntryNormal:
		if len(e.Data) == 0 {
			return // the leader's commit-barrier no-op
		}
		p, err := meta.DecodeProposal(e.Data)
		if err != nil {
			// An undecodable committed entry means a newer node proposed
			// something this version cannot apply: the replica must stop
			// rather than diverge (METADATA.md).
			panic(fmt.Sprintf("raftnode %d: entry %d: %v", n.cfg.ID, e.Index, err))
		}
		res, err := n.store.Apply(p)
		for _, done := range n.waiters[e.Index] {
			done(res, err)
		}
		delete(n.waiters, e.Index)
	}
}

// applyCommitted replays the already-committed prefix of the log into the
// fresh store at boot. Deterministic apply makes the rebuilt store
// bit-identical to the pre-crash one. ConfChange entries are applied to
// membership later by raft itself (Applied in the config tells it where
// replay ended); here they only advance the cursor.
func (n *Node) applyCommitted() error {
	hs, _, err := n.storage.InitialState()
	if err != nil {
		return err
	}
	last, err := n.storage.LastIndex()
	if err != nil {
		return err
	}
	commit := min(hs.Commit, last)
	if commit == 0 {
		return nil
	}
	first, err := n.storage.FirstIndex()
	if err != nil {
		return err
	}
	ents, err := n.storage.Entries(first, commit+1, math.MaxUint64)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal && len(e.Data) > 0 {
			p, err := meta.DecodeProposal(e.Data)
			if err != nil {
				return fmt.Errorf("raftnode: replay entry %d: %w", e.Index, err)
			}
			if _, err := n.store.Apply(p); err != nil {
				// Deterministic validation refusals replay too; they are
				// outcomes, not failures.
				_ = err
			}
		}
		n.applied = e.Index
	}
	return nil
}

// quietLogger drops the library's logging: the simulator's traces and this
// package's own failures are the observability story for now.
type quietLogger struct{}

func (quietLogger) Debug(...any)              {}
func (quietLogger) Debugf(string, ...any)     {}
func (quietLogger) Error(...any)              {}
func (quietLogger) Errorf(string, ...any)     {}
func (quietLogger) Info(...any)               {}
func (quietLogger) Infof(string, ...any)      {}
func (quietLogger) Warning(...any)            {}
func (quietLogger) Warningf(string, ...any)   {}
func (quietLogger) Fatal(v ...any)            { panic(fmt.Sprint(v...)) }
func (quietLogger) Fatalf(f string, v ...any) { panic(fmt.Sprintf(f, v...)) }
func (quietLogger) Panic(v ...any)            { panic(fmt.Sprint(v...)) }
func (quietLogger) Panicf(f string, v ...any) { panic(fmt.Sprintf(f, v...)) }
