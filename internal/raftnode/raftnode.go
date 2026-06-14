// Package raftnode drives etcd-io/raft's RawNode over the seam: virtual or
// real time through seam.Clock, the faulty or real network through
// seam.Transport, durability through a wal.Log on a seam.Disk. The library
// is the inert consensus state machine ADR-0012 chose; this package is the
// assembly it left to us — the write-ahead log, snapshots, the transport
// glue, apply, and (ADR-0024) the election timer.
//
// Metadata durability is layered (ADR-0005, docs/PLAN.md). In production each
// applied entry commits to a per-replica BadgerDB store together with its
// Raft index, atomically, through the Persister. At boot that durable store is
// the source of truth: it is loaded and only the un-applied WAL tail is
// replayed, with entries at or below its index skipped rather than re-applied.
// If the durable store is missing, behind, or corrupt, boot falls back to the
// WAL — the newest snapshot (a complete store dump) plus the committed tail
// rebuild the store and re-materialise the durable copy, so corruption of the
// durable store costs a rebuild, never the data. The simulator sets no
// persister and recovers purely from the WAL, proving that path. Every
// SnapshotEntries applied entries, the node dumps the store, hands the dump to
// raft as the snapshot at that index, and
// rotates its log: one new file whose opening frame carries the snapshot,
// the hard state, and the uncompacted tail — one frame, so a torn rotation
// is simply an invalid file and boot falls back to the previous one, which
// is only removed once the new frame is durable. The same rotation installs
// a snapshot a leader streams to a lagging follower (MsgSnap).
//
// A Node is owned by its event loop, like every core component: every
// method, timer callback, and message must run on the node's single
// logical thread. Nothing here locks.
package raftnode

import (
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
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

// Config assembles a Node. All fields are required unless noted.
type Config struct {
	// ID is this node's Raft ID; Peers seeds the address book: Raft ID →
	// transport address. On a fresh bootstrap it is the founding voter set,
	// this node included (capped at five — ADR-0017). On a joining or
	// restarting node it only needs enough addresses to reach the cluster;
	// the rest arrive with the log and snapshots, which carry every
	// member's address.
	ID    uint64
	Peers map[uint64]seam.NodeID

	// Dials optionally maps Raft IDs to TCP dial addresses, alongside the
	// transport identities in Peers. The simulator never sets it (identity
	// is the address there); production does, and the addresses replicate
	// with membership so Members can report where everyone lives.
	Dials map[uint64]string

	// Join marks a node started to join an existing cluster: a fresh disk
	// does not bootstrap a new cluster, and until membership includes this
	// node it periodically asks the cluster to admit it (an admit message
	// any current leader answers with AddNode). Admission is driven by the
	// joiner so nobody has to chase the leader around.
	Join bool

	Clock     seam.Clock
	Transport seam.Transport
	Disk      seam.Disk
	Rand      *rand.Rand

	// TickInterval drives heartbeats (one heartbeat tick each). The
	// election timeout is ElectionTicks plus jitter of up to the same
	// again, drawn from Rand per ADR-0024.
	TickInterval  time.Duration
	ElectionTicks int

	// SnapshotEntries is how many applied entries accumulate before the
	// node snapshots the store and compacts its log. Zero means the
	// default.
	SnapshotEntries uint64

	// OnMembershipChange, when set, is called on the node's loop with the
	// new member list whenever the applied configuration changes — a
	// join, a promotion, a removal, a snapshot install, or boot replaying
	// any of those. The composition root logs it; the simulator leaves it
	// nil.
	OnMembershipChange func(members []Member)

	// Persister, when set, is the durable metadata store (BadgerDB in
	// production, ADR-0005) this replica makes authoritative on boot: every
	// applied entry commits to it together with the entry's Raft index, and
	// a restart loads it instead of replaying the whole log. Nil leaves the
	// store purely log-recovered — the simulator sets it nil, proving the WAL
	// recovery path that is also the corruption fallback.
	Persister MetaPersister
}

// MetaPersister is the durable, indexed metadata store raftnode commits applies
// into and loads at boot. CommitAt and ResetAt write the applied index in the
// same atomic transaction as the rows, so the durable state and the index it
// reflects never disagree across a crash; ResetAt replaces the whole store (a
// snapshot install or a rebuild), robustly enough to overwrite a corrupt one.
type MetaPersister interface {
	CommitAt(appliedIndex uint64, rows []meta.Row) error
	ResetAt(appliedIndex uint64, rows []meta.Row) error
	LoadState() (rows []meta.Row, appliedIndex uint64, ok bool, err error)
}

// indexedPersister bridges meta.Store's per-transaction Persister (which knows
// only rows) to the durable store's indexed CommitAt, stamping each commit with
// the Raft index of the entry being applied. index points at the node's
// applyingIndex, set on the loop just before each apply.
type indexedPersister struct {
	db    MetaPersister
	index *uint64
}

func (p *indexedPersister) Commit(rows []meta.Row) error {
	return p.db.CommitAt(*p.index, rows)
}

const defaultSnapshotEntries = 4096

// maxVoters is the ADR-0017 cap: five voting members, everyone else a
// learner. Quorum cost stays constant no matter how large the cluster
// grows.
const maxVoters = 5

// promoteLag is how many entries a learner may trail the leader's log by
// and still count as caught up for promotion. Tight enough that a promoted
// voter joins quorum immediately; loose enough that a steadily writing
// cluster can still promote.
const promoteLag = 16

// Node is one Raft-replicated metadata replica.
type Node struct {
	cfg     Config
	rn      *raft.RawNode
	storage *raft.MemoryStorage
	log     *wal.Log
	logSeq  uint64
	store   *meta.Store

	indexed        *indexedPersister // bridges store applies to the durable indexed commit; nil under the simulator
	applyingIndex  uint64            // Raft index of the entry currently applying, stamped into its durable commit
	usePersisted   bool              // boot trusted the durable store (Tier 1); false means a WAL rebuild
	persistedIndex uint64            // the durable store's applied index; boot replay skips normal entries at or below it

	applied   uint64
	snapIndex uint64
	confState raftpb.ConfState
	peers     map[uint64]peerInfo           // the address book: seeded by Config.Peers/Dials, maintained by conf changes and snapshots
	waiters   map[uint64][]func(any, error) // log index → callbacks for this node's proposals
	proposing []func(any, error)            // accepted by Propose, not yet paired to an index

	confCooldown  int  // ticks before the next membership proposal; paces promotion retries
	confChanged   bool // a conf change applied since the last snapshot; forces the next one
	admitCooldown int  // ticks before a joiner's next admission request

	lastHeard     time.Time     // last leader contact (or own leadership)
	electionAfter time.Duration // silence budget before campaigning; re-drawn per campaign

	snapshotsReceived int // streamed installs (MsgSnap), not self-compactions
}

// New boots a node from its disk: an empty disk bootstraps a fresh cluster
// from Peers; anything else is a restart, recovered from the newest valid
// log file. The returned node has scheduled its ticks and is ready for
// messages.
func New(cfg Config) (*Node, error) {
	if cfg.SnapshotEntries == 0 {
		cfg.SnapshotEntries = defaultSnapshotEntries
	}
	n := &Node{
		cfg:     cfg,
		storage: raft.NewMemoryStorage(),
		peers:   make(map[uint64]peerInfo),
		waiters: make(map[uint64][]func(any, error)),
		store:   meta.NewStore(),
	}
	for id, node := range cfg.Peers {
		n.peers[id] = peerInfo{node: node, dial: cfg.Dials[id]}
	}
	// Bring up the durable metadata store and decide whether boot can trust
	// it (Tier 1) or must rebuild from the WAL (Tier 2). recover then uses
	// usePersisted/persistedIndex to replay only the un-applied tail.
	if err := n.openPersistedStore(); err != nil {
		return nil, err
	}

	fresh, err := n.recover()
	if err != nil {
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

	if fresh && !cfg.Join {
		// A fresh cluster. Sorted: map order must not shape the log.
		if len(cfg.Peers) > maxVoters {
			return nil, fmt.Errorf("raftnode: bootstrapping %d voters exceeds the cap of %d (ADR-0017); bootstrap small and grow with join", len(cfg.Peers), maxVoters)
		}
		peers := make([]raft.Peer, 0, len(cfg.Peers))
		for _, id := range slices.Sorted(maps.Keys(cfg.Peers)) {
			peers = append(peers, raft.Peer{ID: id, Context: encodeMember(id, n.peers[id])})
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

// recover finds the newest valid log file, replays it, and removes every
// other log file (older ones are superseded, newer ones are torn
// rotations). It reports whether the disk held no log at all — a fresh
// node.
func (n *Node) recover() (fresh bool, err error) {
	names, err := n.cfg.Disk.List()
	if err != nil {
		return false, fmt.Errorf("raftnode: listing disk: %w", err)
	}
	var seqs []uint64
	for _, name := range names {
		if rest, ok := strings.CutPrefix(name, "raft/log."); ok {
			seq, err := strconv.ParseUint(rest, 10, 64)
			if err != nil {
				return false, fmt.Errorf("raftnode: alien log file %q", name)
			}
			seqs = append(seqs, seq)
		}
	}
	if len(seqs) == 0 {
		n.logSeq = 1
		n.log, _, err = wal.Open(n.cfg.Disk, logName(1))
		return true, err
	}
	slices.Sort(seqs)
	slices.Reverse(seqs)

	for i, seq := range seqs {
		log, records, err := wal.Open(n.cfg.Disk, logName(seq))
		if err != nil {
			return false, err
		}
		if !validLog(records, i == len(seqs)-1) {
			continue // a torn rotation; fall back to the previous file
		}
		n.log, n.logSeq = log, seq
		if err := n.replay(records); err != nil {
			return false, err
		}
		for _, other := range seqs {
			if other != seq {
				n.removeLog(other)
			}
		}
		return false, nil
	}
	return false, fmt.Errorf("raftnode: no valid log file among %d candidates", len(seqs))
}

// validLog reports whether a log file's records can carry a node's state:
// the opening frame holds a snapshot (every rotated file does), or this is
// the oldest file — the bootstrap log, which starts bare.
func validLog(records [][]byte, oldest bool) bool {
	if oldest {
		return true
	}
	if len(records) == 0 {
		return false
	}
	rec, err := decodeRecord(records[0])
	return err == nil && !raft.IsEmptySnap(rec.snap)
}

// replay loads a log file's records into raft storage: snapshot, entries,
// hard state. It deliberately applies nothing — boot leaves Applied at the
// snapshot index, so raft redelivers the committed tail through the first
// Ready and applyEntry rebuilds the store and the configuration on the
// same path a live node uses. (Applying here and declaring the prefix
// Applied would skip the conf-change entries raft never re-reads: a node
// restarting before its first snapshot would come back memberless.)
func (n *Node) replay(records [][]byte) error {
	for i, raw := range records {
		rec, err := decodeRecord(raw)
		if err != nil {
			return fmt.Errorf("raftnode: replaying record %d: %w", i, err)
		}
		if !raft.IsEmptySnap(rec.snap) {
			if err := n.restoreSnapshot(rec.snap); err != nil {
				return fmt.Errorf("raftnode: replaying record %d: %w", i, err)
			}
		}
		if err := n.storage.Append(rec.entries); err != nil {
			return fmt.Errorf("raftnode: replaying record %d: %w", i, err)
		}
		if !raft.IsEmptyHardState(rec.hs) {
			if err := n.storage.SetHardState(rec.hs); err != nil {
				return fmt.Errorf("raftnode: replaying record %d: %w", i, err)
			}
		}
	}
	return nil
}

// openPersistedStore wires the durable metadata store to the node and decides
// whether boot can trust it. With a readable, initialised store, that store
// becomes the source of truth (Tier 1): it is loaded and persistedIndex set,
// so recovery replays only the un-applied WAL tail. An empty store (fresh or
// wiped) or an unreadable one (corrupt) leaves usePersisted false, so recovery
// rebuilds from the WAL and re-materialises the durable copy (Tier 2). The
// simulator passes no persister and recovers purely from the WAL.
func (n *Node) openPersistedStore() error {
	if n.cfg.Persister == nil {
		return nil
	}
	n.indexed = &indexedPersister{db: n.cfg.Persister, index: &n.applyingIndex}
	rows, idx, ok, err := n.cfg.Persister.LoadState()
	if err == nil && ok {
		store := meta.NewStore()
		restoreErr := error(nil)
		for _, r := range rows {
			if e := store.Restore(r.Key, r.Value); e != nil {
				restoreErr = e
				break
			}
		}
		if restoreErr == nil {
			store.SetPersister(n.indexed)
			n.store = store
			n.usePersisted = true
			n.persistedIndex = idx
			return nil
		}
	}
	// Empty, or unreadable/undecodable (corrupt): rebuild from the WAL. The
	// empty store gets the persister so the rebuild's applies persist; a
	// snapshot install resets the durable store (ResetAt discards any
	// corruption), and a no-snapshot rebuild simply re-commits the replayed
	// log into the empty store.
	n.store.SetPersister(n.indexed)
	return nil
}

// adoptStore installs store as the replica's metadata state and, when a durable
// persister is configured, resets it to store's contents at index and attaches
// it — the wholesale-replacement path for a snapshot install or a WAL rebuild.
// A reset failure is fatal: a replica that cannot make its state durable cannot
// participate.
func (n *Node) adoptStore(store *meta.Store, index uint64) {
	if n.cfg.Persister != nil {
		if err := n.cfg.Persister.ResetAt(index, store.Dump()); err != nil {
			panic(fmt.Sprintf("raftnode %d: reset persister: %v", n.cfg.ID, err))
		}
		store.SetPersister(n.indexed)
	}
	n.store = store
	n.persistedIndex = index
}

// restoreSnapshot resets storage to a snapshot — the shared core of boot replay
// and MsgSnap installation. The store itself is replaced from the snapshot
// (resetting the durable copy) unless the durable store is already trusted and
// at or beyond this snapshot's index, in which case it is kept as-is and the
// tail above it is replayed.
func (n *Node) restoreSnapshot(snap raftpb.Snapshot) error {
	store, members, err := decodeSnapshotData(snap.Data)
	if err != nil {
		return fmt.Errorf("snapshot data: %w", err)
	}
	if err := n.storage.ApplySnapshot(snap); err != nil {
		return fmt.Errorf("apply snapshot: %w", err)
	}
	s := snap.Metadata.Index
	if !(n.usePersisted && n.persistedIndex >= s) {
		// The snapshot is the source of truth: a WAL rebuild, or a leader's
		// stream to a follower that had fallen behind it.
		n.adoptStore(store, s)
	}
	maps.Copy(n.peers, members)
	n.applied = s
	n.snapIndex = s
	n.confState = snap.Metadata.ConfState
	n.notifyMembership()
	return nil
}

// notifyMembership reports an applied configuration change to the
// composition root, if it asked.
func (n *Node) notifyMembership() {
	if n.cfg.OnMembershipChange != nil {
		n.cfg.OnMembershipChange(n.Members())
	}
}

// Store is the replica's metadata state, for reads. Loop-owned, like the
// node itself.
func (n *Node) Store() *meta.Store { return n.store }

// SnapshotsReceived counts the snapshots this node installed from a
// leader's stream (MsgSnap) — the catch-up path for a replica whose log
// fell behind the cluster's compaction. Self-compactions do not count.
func (n *Node) SnapshotsReceived() int { return n.snapshotsReceived }

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

// peerInfo is one address-book entry: the transport identity messages are
// sent to, and (in production) the TCP address it dials to.
type peerInfo struct {
	node seam.NodeID
	dial string
}

// Member is one cluster member as a replica knows it: Raft ID, transport
// identity, dial address (empty under the simulator), and role.
type Member struct {
	ID      uint64
	Addr    seam.NodeID
	Dial    string
	Learner bool
}

// Members lists the cluster membership this replica has applied, sorted by
// ID. Like any replicated state it can trail the leader's.
func (n *Node) Members() []Member {
	var ms []Member
	for _, id := range n.confState.Voters {
		ms = append(ms, Member{ID: id, Addr: n.peers[id].node, Dial: n.peers[id].dial})
	}
	for _, id := range n.confState.Learners {
		ms = append(ms, Member{ID: id, Addr: n.peers[id].node, Dial: n.peers[id].dial, Learner: true})
	}
	slices.SortFunc(ms, func(a, b Member) int { return cmp.Compare(a.ID, b.ID) })
	return ms
}

// isMember reports whether a Raft ID is in the applied configuration.
func (n *Node) isMember(id uint64) bool {
	return slices.Contains(n.confState.Voters, id) || slices.Contains(n.confState.Learners, id)
}

// AddNode proposes admitting a node at the given addresses, leader-only.
// Every node is admitted as a learner — it has a whole log to catch up on
// before its vote could help anyone — and promotion to voter is automatic
// (ADR-0017) once it is caught up, while the voter count is under the cap.
// A current member is left exactly as it is: re-admitting a voter as a
// learner would demote it, and a retried admission must not. The change is
// asynchronous and raft drops a membership change proposed while another
// is uncommitted, so callers confirm through Members and retry.
func (n *Node) AddNode(id uint64, addr seam.NodeID, dial string) error {
	if n.isMember(id) {
		return nil
	}
	return n.proposeConfChange(raftpb.ConfChange{
		Type: raftpb.ConfChangeAddLearnerNode, NodeID: id,
		Context: encodeMember(id, peerInfo{node: addr, dial: dial}),
	})
}

// RemoveNode proposes removing a member, leader-only, with the same
// asynchronous confirm-and-retry contract as AddNode. Removing a voter
// opens a vacancy that promotion refills from the learners.
func (n *Node) RemoveNode(id uint64) error {
	return n.proposeConfChange(raftpb.ConfChange{
		Type: raftpb.ConfChangeRemoveNode, NodeID: id,
	})
}

func (n *Node) proposeConfChange(cc raftpb.ConfChange) error {
	if _, leader := n.Leader(); !leader {
		return ErrNotLeader
	}
	if err := n.rn.ProposeConfChange(cc); err != nil {
		return fmt.Errorf("raftnode: conf change: %w", err)
	}
	n.confCooldown = n.cfg.ElectionTicks
	n.processReady()
	return nil
}

// maybePromote fills voter vacancies: while the cluster has fewer than
// five voters (ADR-0017), the lowest-ID learner whose log has caught up to
// the leader's is promoted through an ordinary conf change. One change at
// a time, paced by confCooldown — raft drops a change proposed while
// another is uncommitted, and pacing plus idempotence makes the retry
// harmless. Zone-aware selection arrives with the cluster layout (v0.4);
// replacing a voter that stays down awaits the health machinery.
func (n *Node) maybePromote() {
	if n.confCooldown > 0 || len(n.confState.Voters) >= maxVoters || len(n.confState.Learners) == 0 {
		return
	}
	last, err := n.storage.LastIndex()
	if err != nil {
		panic(fmt.Sprintf("raftnode %d: last index: %v", n.cfg.ID, err))
	}
	progress := n.rn.Status().Progress
	for _, id := range slices.Sorted(slices.Values(n.confState.Learners)) {
		pr, ok := progress[id]
		if !ok || pr.Match == 0 || pr.Match+promoteLag < last {
			continue
		}
		_ = n.proposeConfChange(raftpb.ConfChange{
			Type: raftpb.ConfChangeAddNode, NodeID: id,
			Context: encodeMember(id, n.peers[id]),
		})
		return
	}
}

// maybeRequestAdmission is the joining side of membership: until the
// applied configuration includes this node, periodically send an admit
// message to every known peer. Whichever of them is leader answers with
// AddNode; everyone else drops it. Idempotent, paced, and joiner-driven —
// the joiner is the only party with an interest in retrying.
func (n *Node) maybeRequestAdmission() {
	if !n.cfg.Join || n.isMember(n.cfg.ID) {
		return
	}
	if n.admitCooldown > 0 {
		n.admitCooldown--
		return
	}
	n.admitCooldown = n.cfg.ElectionTicks
	admit := encodeEnvelope(kindAdmit, encodeMember(n.cfg.ID, n.peers[n.cfg.ID]))
	for _, id := range slices.Sorted(maps.Keys(n.peers)) {
		if id != n.cfg.ID {
			n.cfg.Transport.Send(n.peers[id].node, admit)
		}
	}
}

// HandleMessage implements seam.MessageHandler: one enveloped message off
// the wire — raft traffic, or a joiner asking to be admitted.
func (n *Node) HandleMessage(from seam.NodeID, msg []byte) {
	kind, payload, err := decodeEnvelope(msg)
	if err != nil {
		return // a corrupt frame is dropped; the sender will retry
	}
	switch kind {
	case kindRaft:
		var m raftpb.Message
		if err := m.Unmarshal(payload); err != nil {
			return
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
	case kindAdmit:
		id, info, err := decodeMember(payload)
		if err != nil || id == 0 {
			return
		}
		// Leader-only by AddNode's own check; current members are left
		// alone there too. A non-leader simply drops it — the joiner
		// retries everyone.
		_ = n.AddNode(id, info.node, info.dial)
	}
	// Unknown kinds fall through: an upgraded peer may speak kinds this
	// version does not know, and the transport contract allows loss.
}

// onTick drives heartbeats and the ADR-0024 election clock.
func (n *Node) onTick() {
	n.cfg.Clock.AfterFunc(n.cfg.TickInterval, n.onTick)
	n.rn.Tick()
	if n.confCooldown > 0 {
		n.confCooldown--
	}

	n.maybeRequestAdmission()
	now := n.cfg.Clock.Now()
	if _, leader := n.Leader(); leader {
		n.lastHeard = now
		n.maybePromote()
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

		// 1. Durability first: snapshot, hard state, and entries hit the
		// WAL before anything that depends on them leaves the node. A
		// snapshot install is a log rotation; otherwise it is one
		// appended record.
		if !raft.IsEmptySnap(rd.Snapshot) {
			n.snapshotsReceived++
			if err := n.restoreSnapshot(rd.Snapshot); err != nil {
				panic(fmt.Sprintf("raftnode %d: install snapshot: %v", n.cfg.ID, err))
			}
			if err := n.storage.Append(rd.Entries); err != nil {
				panic(fmt.Sprintf("raftnode %d: storage append: %v", n.cfg.ID, err))
			}
			if !raft.IsEmptyHardState(rd.HardState) {
				if err := n.storage.SetHardState(rd.HardState); err != nil {
					panic(fmt.Sprintf("raftnode %d: hard state: %v", n.cfg.ID, err))
				}
			}
			n.rotate(record{hs: n.currentHardState(rd.HardState), entries: rd.Entries, snap: rd.Snapshot})
		} else if !raft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 {
			if err := n.log.Append(encodeRecord(record{hs: rd.HardState, entries: rd.Entries})); err != nil {
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

		// 2. Messages, only after persistence. A snapshot message is
		// complete the moment Send returns (the transport owns delivery),
		// so report it sent: raft resumes normal replication probing.
		for _, m := range rd.Messages {
			to, ok := n.peers[m.To]
			if !ok {
				continue
			}
			data, err := m.Marshal()
			if err != nil {
				panic(fmt.Sprintf("raftnode %d: marshal message: %v", n.cfg.ID, err))
			}
			n.cfg.Transport.Send(to.node, encodeEnvelope(kindRaft, data))
			if m.Type == raftpb.MsgSnap {
				n.rn.ReportSnapshot(m.To, raft.SnapshotFinish)
			}
		}

		// 3. Apply what committed, then consider compacting.
		for _, e := range rd.CommittedEntries {
			n.applyEntry(e)
		}
		n.maybeSnapshot()

		n.rn.Advance(rd)
	}
}

// currentHardState resolves the hard state a rotation must carry: the
// Ready's, or storage's standing one when the Ready brought none — a
// rotated log without a hard state would forget votes.
func (n *Node) currentHardState(rdHS raftpb.HardState) raftpb.HardState {
	if !raft.IsEmptyHardState(rdHS) {
		return rdHS
	}
	hs, _, err := n.storage.InitialState()
	if err != nil {
		panic(fmt.Sprintf("raftnode %d: initial state: %v", n.cfg.ID, err))
	}
	return hs
}

// maybeSnapshot compacts once enough entries have applied since the last
// snapshot: dump the store, hand raft the snapshot at the applied index,
// drop the log prefix, rotate the WAL. A membership change forces it
// regardless of the threshold: raft refuses to install a snapshot whose
// ConfState omits the recipient, so a stored snapshot from before a join
// could never catch that member up — every member must appear in the
// snapshot the leader would stream.
func (n *Node) maybeSnapshot() {
	if !n.confChanged && n.applied-n.snapIndex < n.cfg.SnapshotEntries {
		return
	}
	if n.applied <= n.snapIndex {
		return // nothing newer than the standing snapshot
	}
	n.confChanged = false
	snap, err := n.storage.CreateSnapshot(n.applied, &n.confState, encodeSnapshotData(n.store.Dump(), n.peers))
	if err != nil {
		panic(fmt.Sprintf("raftnode %d: create snapshot: %v", n.cfg.ID, err))
	}
	if err := n.storage.Compact(n.applied); err != nil {
		panic(fmt.Sprintf("raftnode %d: compact: %v", n.cfg.ID, err))
	}
	n.snapIndex = n.applied

	// The rotation frame carries the uncompacted tail — entries above the
	// snapshot that peers have been promised — and the standing hard
	// state.
	var tail []raftpb.Entry
	last, err := n.storage.LastIndex()
	if err != nil {
		panic(fmt.Sprintf("raftnode %d: last index: %v", n.cfg.ID, err))
	}
	if last > n.applied {
		tail, err = n.storage.Entries(n.applied+1, last+1, math.MaxUint64)
		if err != nil {
			panic(fmt.Sprintf("raftnode %d: tail entries: %v", n.cfg.ID, err))
		}
	}
	n.rotate(record{hs: n.currentHardState(raftpb.HardState{}), entries: tail, snap: snap})
}

// rotate replaces the log with a new file whose opening frame is rec —
// snapshot, hard state, and tail together, one frame, so a crash anywhere
// leaves either the complete new file or a fallback to the old one. The
// old file is removed only after the new frame is durable.
func (n *Node) rotate(rec record) {
	seq := n.logSeq + 1
	n.removeLog(seq) // a torn rotation may have left a corpse with this name
	log, records, err := wal.Open(n.cfg.Disk, logName(seq))
	if err != nil || len(records) != 0 {
		panic(fmt.Sprintf("raftnode %d: rotate open: %d leftover records, %v", n.cfg.ID, len(records), err))
	}
	if err := log.Append(encodeRecord(rec)); err != nil {
		panic(fmt.Sprintf("raftnode %d: rotate append: %v", n.cfg.ID, err))
	}
	old := n.logSeq
	n.log, n.logSeq = log, seq
	n.removeLog(old)
}

// removeLog deletes a log file, best effort: a leftover is wasted space
// the next rotation reclaims, never a correctness problem.
func (n *Node) removeLog(seq uint64) {
	if err := n.cfg.Disk.Remove(logName(seq)); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			panic(fmt.Sprintf("raftnode %d: remove log %d: %v", n.cfg.ID, seq, err))
		}
		return
	}
	_ = n.cfg.Disk.Sync(logName(seq))
}

func logName(seq uint64) string {
	return "raft/log." + strconv.FormatUint(seq, 10)
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
		// The address book changes with the configuration, identically on
		// every replica: an admission's context carries the address, a
		// removal drops it.
		switch cc.Type {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			if len(cc.Context) > 0 {
				id, info, err := decodeMember(cc.Context)
				if err != nil || id != cc.NodeID {
					panic(fmt.Sprintf("raftnode %d: conf change at %d: member context for %d: %v", n.cfg.ID, e.Index, cc.NodeID, err))
				}
				n.peers[id] = info
			}
		case raftpb.ConfChangeRemoveNode:
			delete(n.peers, cc.NodeID)
		}
		n.confState = *n.rn.ApplyConfChange(cc)
		n.confChanged = true
		n.notifyMembership()
	case raftpb.EntryNormal:
		if len(e.Data) == 0 {
			return // the leader's commit-barrier no-op
		}
		if e.Index <= n.persistedIndex {
			// The durable store, loaded at boot, already holds this entry's
			// effect. Re-applying would redo work and could raise a spurious
			// validation error (e.g. "bucket exists"). Membership conf-changes
			// are not skipped — only normal metadata entries reach here.
			return
		}
		p, err := meta.DecodeProposal(e.Data)
		if err != nil {
			// An undecodable committed entry means a newer node proposed
			// something this version cannot apply: the replica must stop
			// rather than diverge (METADATA.md).
			panic(fmt.Sprintf("raftnode %d: entry %d: %v", n.cfg.ID, e.Index, err))
		}
		// Stamp the index this apply commits at, so the persister writes it
		// atomically with the rows (the bridge reads applyingIndex).
		n.applyingIndex = e.Index
		res, err := n.store.Apply(p)
		if errors.Is(err, meta.ErrPersist) {
			// The entry committed cluster-wide but this replica could not
			// make it durable; apply already rolled back in memory, which
			// would diverge this replica from peers that persisted. Stop
			// loudly — cannot persist, cannot participate — like the WAL
			// append failures above.
			panic(fmt.Sprintf("raftnode %d: apply entry %d: %v", n.cfg.ID, e.Index, err))
		}
		for _, done := range n.waiters[e.Index] {
			done(res, err)
		}
		delete(n.waiters, e.Index)
	}
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
