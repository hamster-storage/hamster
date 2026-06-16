package cluster

import (
	"log"
	"slices"
	"strings"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/place"
)

// reconcileLayout advances the stored cluster layout (ADR-0028, ADR-0016)
// toward the current Raft membership, labeling each member with its
// failure-domain host/zone and capacity weight. Runs on the loop. Every node
// may call it freely: Propose is leader-gated and the applies are
// idempotent/compare-and-set, so a non-leader or a stale generation is an
// ignored outcome, and a layout already matching membership proposes nothing.
//
// Two steps. First, the registry: the leader commits a RegisterNode for every
// member whose labels it knows — the issuer holds them all (it accumulated
// them at admission), a plain member only its own. Only the leader's proposals
// commit (no forwarding in v0.3), but the issuer leads while joins happen, so
// each member's record lands; once committed it persists. Second, composition:
// the layout is built from the *replicated* registry (Store().Nodes()), so any
// leader — not only the issuer — can compose a complete one. This is the gap
// closed versus the earlier issuer-local registry, where a non-issuer leader
// could only hold the last good layout.
//
// The completeness guard remains the safety rule: if a member's record has not
// committed yet, propose nothing rather than compose a layout with degraded
// labels, which would move placement and mislocate existing shards. Nodes are
// sorted so the proposal is byte-identical whichever leader composes it —
// placement ranks by hash, so record order never affects placement.
func (n *Node) reconcileLayout() {
	if n.raft == nil {
		return
	}
	store := n.raft.Store()

	// Step 1: ensure this node's known registrations are committed. The labels
	// were defaulted at admission (handleJoin / Init), so they are recorded
	// verbatim. A registration that already matches proposes nothing; a fresh
	// commit re-triggers reconcile so composition follows without a tick's wait.
	n.issueMu.Lock()
	known := append([]Member(nil), n.cfg.NodeLabels...)
	n.issueMu.Unlock()
	for _, m := range known {
		if cur, ok := store.Node(m.NodeID); ok &&
			cur.Host == m.Host && cur.Zone == m.Zone && cur.Capacity == m.Capacity {
			continue
		}
		n.raft.Propose(meta.RegisterNode{
			ProposedAtUnixMS: n.clock.Now().UnixMilli(),
			NodeID:           m.NodeID,
			Host:             m.Host,
			Zone:             m.Zone,
			Capacity:         m.Capacity,
		}, func(_ any, err error) {
			if err == nil {
				n.loop.Post(n.reconcileLayout)
			}
		})
	}

	// Step 2: compose the layout from the replicated registry.
	recs := make(map[string]meta.NodeRecord)
	for _, r := range store.Nodes() {
		recs[r.NodeID] = r
	}
	raftMember := make(map[string]bool)
	for _, m := range n.raft.Members() {
		raftMember[string(m.Addr)] = true
	}

	desired, have := n.desiredLayout()
	if !have {
		return // a member's record has not committed yet (or no members) — hold
	}

	cur, ok := n.raft.Store().ClusterLayout()
	// One transition at a time — the single-pair model (ADR-0004). While one is
	// in flight, drive it to completion (migrate shards to their new home, then
	// close it) and open nothing new: a queued change waits for the open one to
	// finish, then opens on a later tick. Driven from here rather than its own
	// per-tick post — a second post perturbs the conf-change pipeline enough to
	// stall formation.
	if ok && len(cur.Previous) > 0 {
		n.driveTransitionClose()
		return
	}
	// Finish a completed replacement (ADR-0004): once the swap transition has
	// closed, the replaced node is out of the layout and holds nothing, so evict
	// it from Raft (its ID is tombstoned, like any removal).
	if ok {
		for _, m := range n.raft.Members() {
			id := string(m.Addr)
			r, known := recs[id]
			if known && r.ReplacedBy != "" && raftMember[r.ReplacedBy] && !inLayout(cur, id) {
				n.evictReplaced(id)
				return
			}
		}
	}
	if ok && slices.Equal(cur.EffectiveNodes(), desired) {
		return // already current, no transition
	}
	// A layout change relocates shards — placement is positional (ADR-0004), so
	// any change to the member set reshuffles which node holds shard i. That
	// strands existing data unless reads can find it at its old home while repair
	// migrates it, so the change opens a transition carrying the prior member set
	// as Previous. Both directions need it: a node leaving (subtractive), and a
	// node joining (additive) — a join inserts at its rendezvous rank and shifts
	// the positional assignment of every later shard, beyond what rebuild-from-k
	// can heal. The exception is an empty cluster: with no objects at risk there
	// is nothing to migrate, so it swaps outright — formation (all additive joins)
	// never serializes behind a transition.
	next := uint64(1)
	pc := uint32(place.DefaultPartitionCount)
	var previous []meta.LayoutNode
	if ok {
		next = cur.Version + 1
		pc = cur.PartitionCount
		if subtractiveLayout(cur.EffectiveNodes(), desired) || n.hasStoredObjects() {
			previous = cur.EffectiveNodes()
		}
	}
	n.raft.Propose(meta.SetClusterLayout{
		ProposedAtUnixMS: n.clock.Now().UnixMilli(),
		Version:          next,
		PartitionCount:   pc,
		Nodes:            desired,
		Previous:         previous,
	}, func(any, error) {}) // stale / not-leader outcomes are benign
}

// subtractiveLayout reports whether moving from old to new takes a node that was
// actively placed (present and not draining) and drops or demotes it (absent, or
// now draining). Those changes relocate shards off a node that may be leaving,
// so they open a transition (ADR-0004); pure additions — a new node, or a drain
// undone — do not, and ride the ordinary rebuild-from-k repair.
func subtractiveLayout(old, next []meta.LayoutNode) bool {
	activeIn := func(ns []meta.LayoutNode, id string) bool {
		for _, m := range ns {
			if m.ID == id {
				return !m.Draining
			}
		}
		return false
	}
	for _, o := range old {
		if o.Draining {
			continue
		}
		if !activeIn(next, o.ID) {
			return true
		}
	}
	return false
}

// inLayout reports whether nodeID is one of the layout's effective members.
func inLayout(cl meta.ClusterLayout, nodeID string) bool {
	for _, m := range cl.EffectiveNodes() {
		if m.ID == nodeID {
			return true
		}
	}
	return false
}

// desiredLayout composes the layout the registry and Raft membership imply
// (ADR-0004): every current member, labeled from its NodeRecord, minus any node
// whose replacement is already present. have is false while a member's record has
// not committed yet, or there are no members — the same hold reconcileLayout
// takes. Self-contained so the optimize readiness check can reuse it. Runs on the
// loop.
func (n *Node) desiredLayout() (desired []meta.LayoutNode, have bool) {
	store := n.raft.Store()
	recs := make(map[string]meta.NodeRecord)
	for _, r := range store.Nodes() {
		recs[r.NodeID] = r
	}
	raftMember := make(map[string]bool)
	for _, m := range n.raft.Members() {
		raftMember[string(m.Addr)] = true
	}
	for _, m := range n.raft.Members() {
		r, known := recs[string(m.Addr)]
		if !known {
			return nil, false
		}
		if r.ReplacedBy != "" && raftMember[r.ReplacedBy] {
			continue
		}
		desired = append(desired, meta.LayoutNode{ID: r.NodeID, Host: r.Host, Zone: r.Zone, Weight: r.Capacity, Draining: r.Draining})
	}
	if len(desired) == 0 {
		return nil, false
	}
	slices.SortFunc(desired, func(a, b meta.LayoutNode) int { return strings.Compare(a.ID, b.ID) })
	return desired, true
}

// layoutSettled reports whether the committed layout already reflects current
// membership: no transition open, and its effective nodes equal the set the
// registry and Raft membership imply. It is false in the window right after a
// join or drain, before reconcile has incorporated the change — exactly when an
// optimize would otherwise read a node count that is about to grow and conclude
// the data already fits. Runs on the loop.
func (n *Node) layoutSettled() bool {
	cur, ok := n.raft.Store().ClusterLayout()
	if !ok || len(cur.Previous) > 0 {
		return false
	}
	desired, have := n.desiredLayout()
	if !have {
		return false
	}
	return slices.Equal(cur.EffectiveNodes(), desired)
}

// evictReplaced removes a node whose replacement has taken over (ADR-0004): the
// swap transition has closed, so it holds nothing. Leader-gated like reconcile's
// other proposals (benign on a non-leader); RemoveNode tombstones the ID.
func (n *Node) evictReplaced(nodeID string) {
	for _, m := range n.raft.Members() {
		if string(m.Addr) == nodeID {
			if err := n.raft.RemoveNode(m.ID); err == nil {
				log.Printf("cluster: node %s has been replaced; evicted from the cluster", nodeID)
			}
			return
		}
	}
}

// driveTransitionClose advances an in-flight layout transition (ADR-0004),
// called from reconcileLayout only once it has confirmed a transition is open.
// It sweeps the cluster — which during a transition migrates shards from their
// old home to their new one — and, once a sweep finds nothing left to move and
// nothing it cannot heal, closes the transition by installing a layout with
// Previous dropped. Gated so only the leader sweeps and only one sweep runs at a
// time. Production scrub scheduling outside a transition is a later pass.
func (n *Node) driveTransitionClose() {
	if n.coord == nil || n.sweeping {
		return
	}
	if lead, _ := n.raft.Leader(); lead != n.cfg.RaftID {
		return
	}
	n.sweeping = true
	n.coord.RepairSweep(func(rep coord.RepairReport, err error) {
		n.sweeping = false
		if err != nil {
			log.Printf("cluster: transition repair sweep failed: %v", err)
			return
		}
		// Converged: a sweep that moved, rebuilt, and re-encoded nothing and
		// left nothing unhealable means every shard is at its new home and every
		// object fits the active set — the old set is dead weight.
		if rep.MigratedShards == 0 && rep.RebuiltShards == 0 && rep.ReEncoded == 0 &&
			len(rep.Unrepairable) == 0 && len(rep.Failed) == 0 {
			n.closeTransition()
		}
	})
}

// closeTransition installs a layout that drops Previous, ending the rebalance.
// Re-reads the layout so the version is fresh and the close is a no-op if the
// transition already closed or changed under a slow sweep.
func (n *Node) closeTransition() {
	cur, ok := n.raft.Store().ClusterLayout()
	if !ok || len(cur.Previous) == 0 {
		return
	}
	version := cur.Version + 1
	n.raft.Propose(meta.SetClusterLayout{
		ProposedAtUnixMS: n.clock.Now().UnixMilli(),
		Version:          version,
		PartitionCount:   cur.PartitionCount,
		Nodes:            cur.EffectiveNodes(),
		Previous:         nil,
	}, func(_ any, err error) {
		if err == nil {
			log.Printf("cluster: layout transition complete (version %d)", version)
		}
	})
}

// hasStoredObjects reports whether any erasure-coded object exists — the cheap
// "is there data at risk" check that decides whether an additive layout change
// opens a transition (ADR-0004). Stops at the first object, so it is cheap on an
// empty store (formation) and only ever runs when the layout is changing.
func (n *Node) hasStoredObjects() bool {
	store := n.raft.Store()
	found := false
	for _, b := range store.ListBuckets() {
		store.ScanVersions(b.Name, func(_ string, e meta.VersionEntry) bool {
			if e.Kind == meta.KindObject && len(e.Parts) == 0 {
				found = true
				return false // first object found — stop the scan
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

// maxStoredWidth is the largest shard width (k+m) of any stored object version —
// the number of distinct nodes the cluster must keep to hold every object at
// full spread. Multipart parts (not on the cluster path yet) carry no width.
func (n *Node) maxStoredWidth() int {
	store := n.raft.Store()
	maxW := 0
	for _, b := range store.ListBuckets() {
		store.ScanVersions(b.Name, func(_ string, e meta.VersionEntry) bool {
			if e.Kind == meta.KindObject && len(e.Parts) == 0 {
				if w := int(e.ECDataShards + e.ECParityShards); w > maxW {
					maxW = w
				}
			}
			return true
		})
	}
	return maxW
}
