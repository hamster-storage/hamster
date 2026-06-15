package cluster

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hamster-storage/hamster/internal/certs"
	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/place"
	"github.com/hamster-storage/hamster/internal/raftnode"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sys"
)

// Tunables for the production composition. Variables rather than
// constants so the package tests can run a whole cluster on one machine
// at simulation-like speeds.
var (
	tickInterval  = 100 * time.Millisecond
	electionTicks = 10 // election timeout 1–2s
	peerSyncEvery = time.Second
)

// Node is one running cluster node: the Raft-replicated metadata plane over
// the production adapters. The join/status protocol shares the peer transport's
// port (ADR-0030), so a node binds one cluster address, not two. In v0.2 this
// is the whole node — the S3 gateway joins it when the data path can replicate
// (v0.3).
type Node struct {
	cfg    NodeConfig
	dir    string
	ca     *certs.CA // nil on nodes that cannot issue
	loop   *sys.Loop
	metadb *sys.MetaDB // durable metadata store mirrored under Raft (ADR-0005)

	transport *sys.Transport
	raft      *raftnode.Node
	rng       *mathrand.Rand // loop-owned, shared by raft timing and minting
	clock     seam.Clock     // loop-owned; stamps layout-reconcile proposals
	data      *datapath.Service
	coord     *coord.Coordinator
	s3        *s3Server     // nil unless ServeS3 was asked for
	ready     chan struct{} // closed once the node is built; gates control conns
	stopSync  chan struct{}

	sweeping bool // loop-owned: a transition repair sweep is in flight (one at a time)

	issueMu sync.Mutex // serializes joins: ID allocation and its durable record
	stopped sync.Once
}

// Run starts a cluster node from its data directory. The caller owns it
// and must Stop it.
func Run(dataDir string) (*Node, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return nil, err
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return nil, err
	}
	var ca *certs.CA
	if _, statErr := os.Stat(filepath.Join(dir, "ca.key")); statErr == nil {
		// This node holds the issuance key: a problem loading it is an
		// error, not a silent demotion to non-issuer.
		if ca, err = loadCA(dir); err != nil {
			return nil, err
		}
	}

	disk, err := sys.NewDisk(dataDir)
	if err != nil {
		return nil, err
	}
	// The metadata store: BadgerDB on this replica (ADR-0005), the durable
	// source of truth the node loads on boot. If it cannot even be opened it
	// is corrupt beyond use; the Raft WAL holds a complete copy (its snapshots
	// are full metadata dumps), so the store is a rebuildable cache — discard
	// it and let recovery rebuild from the log.
	metaDir := filepath.Join(dataDir, "meta")
	mdb, err := sys.OpenMetaDB(metaDir)
	if err != nil {
		log.Printf("cluster: metadata store at %s is unreadable (%v); discarding and rebuilding from the Raft log", metaDir, err)
		if rmErr := os.RemoveAll(metaDir); rmErr != nil {
			return nil, fmt.Errorf("cluster: removing unreadable metadata store: %w", rmErr)
		}
		if mdb, err = sys.OpenMetaDB(metaDir); err != nil {
			return nil, fmt.Errorf("cluster: reopening metadata store: %w", err)
		}
	}
	loop := sys.NewLoop()
	n := &Node{cfg: cfg, dir: dir, ca: ca, loop: loop, metadb: mdb, ready: make(chan struct{}), stopSync: make(chan struct{})}

	peers := make(map[seam.NodeID]string)
	raftPeers := make(map[uint64]seam.NodeID)
	dials := make(map[uint64]string)
	for _, m := range cfg.Members {
		peers[seam.NodeID(m.NodeID)] = m.Dial
		raftPeers[m.RaftID] = seam.NodeID(m.NodeID)
		dials[m.RaftID] = m.Dial
	}

	transport, err := sys.NewTransport(sys.TransportConfig{
		NodeID: seam.NodeID(cfg.NodeID), Listen: cfg.ClusterAddr, Peers: peers,
		Cert: cert, CA: pool,
		// The channel envelope (ADR-0027 decision 6): Raft and shard
		// traffic share the transport; the demux routes by channel.
		// Unknown channels and malformed envelopes drop — silence is
		// always safe on this transport, peers retry or re-elect.
		Deliver: func(from seam.NodeID, msg []byte) {
			loop.Post(func() {
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
					if n.data != nil {
						_ = n.data.HandleData(from, payload)
					}
				}
			})
		},
		// The join/status protocol shares this port (ADR-0030): a client that
		// does not negotiate the peer ALPN lands here. One listen address per
		// node, not two.
		OnControl: func(conn *tls.Conn) { n.handleConn(conn) },
	})
	if err != nil {
		mdb.Close()
		loop.Stop()
		if errors.Is(err, syscall.EADDRINUSE) {
			return nil, fmt.Errorf("cluster listen address %s is already in use — another node on this machine, or a stale process? choose a free port with -listen: %w", cfg.ClusterAddr, err)
		}
		return nil, err
	}
	n.transport = transport

	// The Raft node is loop-owned: construct it on the loop, so its timers
	// and the transport's deliveries serialize behind its construction.
	var seed [16]byte
	if _, err := rand.Read(seed[:]); err != nil {
		transport.Close()
		loop.Stop()
		return nil, err
	}
	// Membership changes are rare and high-signal: log the roster every
	// time it shifts — joins, promotions, removals. Loop-owned, so the
	// last-roster comparison needs no lock.
	lastRoster := ""
	onMembership := func(members []raftnode.Member) {
		roster := make([]string, 0, len(members))
		for _, m := range members {
			role := "voter"
			if m.Learner {
				role = "learner"
			}
			roster = append(roster, fmt.Sprintf("%s=%s", m.Addr, role))
		}
		line := strings.Join(roster, " ")
		if line != lastRoster {
			lastRoster = line
			log.Printf("cluster: membership: %s", line)
		}
	}

	built := make(chan error, 1)
	loop.Post(func() {
		clock := sys.LoopClock(sys.Clock{}, loop)
		rng := mathrand.New(mathrand.NewPCG(
			binary.LittleEndian.Uint64(seed[0:8]), binary.LittleEndian.Uint64(seed[8:16])))
		n.rng = rng
		n.clock = clock
		rn, err := raftnode.New(raftnode.Config{
			ID: cfg.RaftID, Peers: raftPeers, Dials: dials, Join: cfg.Join,
			Clock:     clock,
			Transport: raftTransport{transport}, Disk: disk,
			Rand:         rng,
			TickInterval: tickInterval, ElectionTicks: electionTicks,
			OnMembershipChange: onMembership,
			OnRemoved: func() {
				// This node was evicted from the cluster (ADR-0004). Its ID is
				// tombstoned, so it can never re-admit; stop it so it does not
				// keep asking. Asynchronous — Stop joins the loop this fires on.
				log.Printf("cluster: this node was removed from the cluster; shutting down")
				go n.Stop()
			},
			Persister: mdb,
		})
		n.raft = rn
		// Every cluster node is a shard holder: the data-plane service
		// answers writes, reads, verifies, and deletes from its peers.
		n.data = datapath.New(datapath.Config{Clock: clock, Transport: transport, Disk: disk})
		// The coordinator drives this node's own S3 operations. Placement
		// resolves from the stored, versioned cluster layout (ADR-0028):
		// every node reads the same committed member set and partition
		// count, so the same object lands on the same nodes regardless of
		// transient membership views. The layout is reconciled toward the
		// Raft membership by the leader (reconcileLayout); the auto-ladder
		// profile follows the layout's member count inside the coordinator.
		n.coord = coord.New(coord.Config{
			Clock: clock, Rand: rng, Data: n.data, Raft: rn,
			Layout: func() (place.Layout, bool) {
				cl, ok := rn.Store().ClusterLayout()
				if !ok {
					return place.Layout{}, false
				}
				return place.Layout{
					Version:        cl.Version,
					PartitionCount: cl.PartitionCount,
					Members:        placeNodes(cl.EffectiveNodes()),
					Previous:       placeNodes(cl.Previous), // nil in steady state
				}, true
			},
		})
		built <- err
	})
	if err := <-built; err != nil {
		transport.Close()
		mdb.Close()
		loop.Stop()
		return nil, err
	}
	// The node is built: raft and the handlers exist, so control connections
	// (join/status) the shared listener has been accepting since NewTransport
	// can now be served. handleConn waits on this.
	close(n.ready)

	// Membership grows the transport's address book: joined members appear
	// in the replicated state, the transport learns where they dial.
	go n.syncPeers()

	return n, nil
}

// placeNodes maps committed layout members to the placement function's nodes.
// nil in, nil out — so a steady-state layout (no Previous) yields a nil old set.
func placeNodes(members []meta.LayoutNode) []place.Node {
	if len(members) == 0 {
		return nil
	}
	out := make([]place.Node, len(members))
	for i, e := range members {
		out[i] = place.Node{ID: seam.NodeID(e.ID), Host: e.Host, Zone: e.Zone, Weight: e.Weight, Draining: e.Draining}
	}
	return out
}

// raftTransport wraps Raft traffic in the channel envelope on its way to
// the shared transport.
type raftTransport struct{ t seam.Transport }

func (rt raftTransport) Send(to seam.NodeID, msg []byte) {
	rt.t.Send(to, datapath.Wrap(datapath.ChannelRaft, msg))
}

// Stop shuts the node down: S3 listener, peer sync, transport (which also
// carries join/status), loop — HTTP before the loop, per the gateway
// contract. Stopping twice is fine.
func (n *Node) Stop() {
	n.stopped.Do(func() {
		if n.s3 != nil {
			n.s3.stop()
		}
		close(n.stopSync)
		n.transport.Close()
		n.loop.Stop()
		// After the loop has stopped, no apply can touch the store; the
		// durable metadata mirror is safe to close.
		if n.metadb != nil {
			n.metadb.Close()
		}
	})
}

// Done is closed when the node stops — on Stop, including the self-stop after
// this node is removed from the cluster (ADR-0004). A server loop selects on it
// to exit the process when the node is evicted, not only on a signal.
func (n *Node) Done() <-chan struct{} { return n.stopSync }

// Addr is the transport address; JoinAddr the join/status listener's.
func (n *Node) Addr() string { return n.transport.Addr() }

// JoinAddr is where join and status clients connect — the same shared port as
// the peer transport (ADR-0030); join/status is routed off it by ALPN.
func (n *Node) JoinAddr() string { return n.transport.Addr() }

// members snapshots membership and leadership from the loop, labeling each
// member with its failure-domain host/zone from the replicated layout (so
// `cluster status` reports the same labels placement uses).
func (n *Node) members() []Member {
	done := make(chan []Member, 1)
	n.loop.Post(func() {
		lead, _ := n.raft.Leader()
		labels := map[string]meta.LayoutNode{}
		if cl, ok := n.raft.Store().ClusterLayout(); ok {
			for _, e := range cl.EffectiveNodes() {
				labels[e.ID] = e
			}
		}
		// This node's local, best-effort liveness view (ADR-0027): peers a
		// PUT currently skips to avoid their write timeout. Reported as-is —
		// a different node may see a different set.
		down := map[string]bool{}
		if n.coord != nil {
			for _, id := range n.coord.DownNodes() {
				down[string(id)] = true
			}
		}
		var ms []Member
		for _, m := range n.raft.Members() {
			mem := Member{
				RaftID: m.ID, NodeID: string(m.Addr), Dial: m.Dial,
				Learner: m.Learner, Leader: m.ID == lead,
				Down: down[string(m.Addr)],
			}
			if lbl, ok := labels[string(m.Addr)]; ok {
				mem.Host, mem.Zone, mem.Capacity, mem.Draining = lbl.Host, lbl.Zone, lbl.Weight, lbl.Draining
			}
			ms = append(ms, mem)
		}
		done <- ms
	})
	select {
	case ms := <-done:
		return ms
	case <-time.After(5 * time.Second):
		return nil // the loop is wedged or stopping; callers see an empty cluster
	}
}

func (n *Node) syncPeers() {
	t := time.NewTicker(peerSyncEvery)
	defer t.Stop()
	for {
		select {
		case <-n.stopSync:
			return
		case <-t.C:
			for _, m := range n.members() {
				if m.Dial != "" {
					n.transport.AddPeer(seam.NodeID(m.NodeID), m.Dial)
				}
			}
			// Drive the stored cluster layout toward membership. Only the
			// leader's proposal commits; every other node's is a benign
			// no-op (ADR-0028). This is what installs the first layout once
			// a leader exists and advances it as the cluster forms. While a
			// transition is in flight reconcileLayout also drives its migration
			// and close, so no second per-tick post is needed — adding one
			// perturbs the conf-change pipeline enough to stall formation.
			n.loop.Post(n.reconcileLayout)
		}
	}
}

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
	labels := make(map[string]meta.LayoutNode)
	for _, r := range store.Nodes() {
		labels[r.NodeID] = meta.LayoutNode{ID: r.NodeID, Host: r.Host, Zone: r.Zone, Weight: r.Capacity, Draining: r.Draining}
	}

	var desired []meta.LayoutNode
	for _, m := range n.raft.Members() {
		lbl, known := labels[string(m.Addr)]
		if !known {
			return // a member's record has not committed yet — hold (see above)
		}
		desired = append(desired, lbl)
	}
	if len(desired) == 0 {
		return
	}
	slices.SortFunc(desired, func(a, b meta.LayoutNode) int { return strings.Compare(a.ID, b.ID) })

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
	if ok && slices.Equal(cur.EffectiveNodes(), desired) {
		return // already current, no transition
	}
	// A layout change relocates shards — placement is positional (ADR-0004). A
	// subtractive change (an actively-placed node draining out or removed) would
	// strand existing data, so it opens a transition: the prior member set is
	// carried as Previous, so reads dual-read and repair migrates shards old→new
	// before the old set is dropped. Additive changes (a node joining, a drain
	// undone) ride the existing rebuild-from-k repair, as capacity weighting
	// does, so cluster formation never serializes behind a migration.
	next := uint64(1)
	pc := uint32(place.DefaultPartitionCount)
	var previous []meta.LayoutNode
	if ok {
		next = cur.Version + 1
		pc = cur.PartitionCount
		if subtractiveLayout(cur.EffectiveNodes(), desired) {
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
		// Converged: a sweep that moved nothing and left nothing unhealable
		// means every shard is at its new home — the old set is dead weight.
		if rep.MigratedShards == 0 && rep.RebuiltShards == 0 &&
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

// upsertLabel records a member's labels by node ID, replacing any prior entry
// so a re-join with changed labels takes effect.
func upsertLabel(labels []Member, m Member) []Member {
	for i := range labels {
		if labels[i].NodeID == m.NodeID {
			labels[i] = m
			return labels
		}
	}
	return append(labels, m)
}

// handleConn serves one control request — join or status — and closes. It is
// the transport's OnControl handler: the connection arrived on the shared
// cluster port without the peer ALPN, its handshake already complete. It waits
// for the node to finish building (the listener accepts from the moment the
// transport exists, which can precede raft/handler construction), bounded so a
// pre-build connection cannot pin a goroutine.
func (n *Node) handleConn(conn *tls.Conn) {
	defer conn.Close()
	select {
	case <-n.ready:
	case <-n.stopSync:
		return
	case <-time.After(10 * time.Second):
		return
	}
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := conn.Handshake(); err != nil {
		return
	}
	buf, err := readFrame(conn)
	if err != nil {
		return
	}
	kind, payload, err := decodeRequest(buf)
	if err != nil {
		return
	}
	switch kind {
	case reqJoin:
		resp := n.handleJoin(payload)
		if resp.Error != "" {
			log.Printf("cluster: join refused: %s", resp.Error)
		} else {
			log.Printf("cluster: issued identity to node %q (raft id %d)", resp.joinedNodeID, resp.RaftID)
		}
		_ = writeFrame(conn, encodeJoinResponse(resp.joinResponse))
	case reqStatus:
		var resp statusResponse
		if len(conn.ConnectionState().PeerCertificates) == 0 {
			resp.Error = "status requires a cluster certificate"
		} else {
			resp.Members = n.members()
		}
		_ = writeFrame(conn, encodeStatusResponse(resp))
	case reqDrain:
		_ = writeFrame(conn, encodeDrainResponse(n.handleDrain(conn, payload)))
	case reqRemove:
		_ = writeFrame(conn, encodeRemoveResponse(n.handleRemove(conn, payload)))
	}
	// Unknown kinds get no response: an upgraded client will know why.
}

// handleDrain serves a drain/undrain request: a leader-only metadata proposal
// (ADR-0004), authenticated by a cluster certificate like status. A non-leader
// answers with the leader's dial address so the client can retry there — the
// same leader-only shape as S3 writes, no proposal forwarding yet.
func (n *Node) handleDrain(conn *tls.Conn, payload []byte) drainResponse {
	if len(conn.ConnectionState().PeerCertificates) == 0 {
		return drainResponse{Error: "drain requires a cluster certificate"}
	}
	req, err := decodeDrainRequest(payload)
	if err != nil {
		return drainResponse{Error: "malformed drain request"}
	}
	switch err := n.proposeSetDraining(req.NodeID, req.Draining); {
	case errors.Is(err, raftnode.ErrNotLeader):
		resp := drainResponse{Error: "this node is not the metadata leader"}
		for _, m := range n.members() {
			if m.Leader {
				resp.Leader = m.Dial
				break
			}
		}
		return resp
	case err != nil:
		return drainResponse{Error: err.Error()}
	}
	return drainResponse{}
}

// proposeSetDraining submits the drain-flag proposal through this node's Raft
// and waits for its commit. Leader-only: a non-leader's Propose returns
// ErrNotLeader unchanged for the caller to translate. A new drain is refused
// while one is already underway — one node at a time (drainBlockedReason).
func (n *Node) proposeSetDraining(nodeID string, draining bool) error {
	ch := make(chan error, 1)
	n.loop.Post(func() {
		if draining {
			if reason := n.drainBlockedReason(nodeID); reason != "" {
				ch <- errors.New(reason)
				return
			}
		}
		n.raft.Propose(meta.SetNodeDraining{
			ProposedAtUnixMS: n.clock.Now().UnixMilli(), NodeID: nodeID, Draining: draining,
		}, func(_ any, e error) { ch <- e })
	})
	select {
	case err := <-ch:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("cluster: drain proposal timed out")
	}
}

// drainBlockedReason returns a non-empty explanation when a new drain must be
// refused: one node drains at a time (ADR-0004, the single-transition model). A
// layout transition already in flight, or another node already flagged draining
// (the flag set before its transition has opened), blocks a second so the
// operator gets a clear error rather than a silent queue. Runs on the loop.
// Draining multiple nodes at once is future work — the format already supports a
// set; only the open logic would change. Empty means clear to proceed.
func (n *Node) drainBlockedReason(nodeID string) string {
	if cl, ok := n.raft.Store().ClusterLayout(); ok && len(cl.Previous) > 0 {
		return "a layout transition is already in progress; wait for it to finish before draining another node"
	}
	for _, r := range n.raft.Store().Nodes() {
		if r.Draining && r.NodeID != nodeID {
			return fmt.Sprintf("node %s is already draining; only one node may drain at a time", r.NodeID)
		}
	}
	return ""
}

// handleRemove serves a remove request (ADR-0004): a leader-only metadata
// operation, authenticated by a cluster certificate like drain. A non-leader
// answers with the leader's dial address so the client can retry there.
func (n *Node) handleRemove(conn *tls.Conn, payload []byte) removeResponse {
	if len(conn.ConnectionState().PeerCertificates) == 0 {
		return removeResponse{Error: "remove requires a cluster certificate"}
	}
	req, err := decodeRemoveRequest(payload)
	if err != nil {
		return removeResponse{Error: "malformed remove request"}
	}
	switch err := n.proposeRemove(req.NodeID); {
	case errors.Is(err, raftnode.ErrNotLeader):
		resp := removeResponse{Error: "this node is not the metadata leader"}
		for _, m := range n.members() {
			if m.Leader {
				resp.Leader = m.Dial
				break
			}
		}
		return resp
	case err != nil:
		return removeResponse{Error: err.Error()}
	}
	return removeResponse{}
}

// proposeRemove evicts nodeID, retrying the asynchronous conf change (which raft
// drops while another is pending) until membership reflects it or the deadline
// passes. Runs off the loop (the control handler's goroutine), so it may wait.
// Leadership and membership are judged on the leader: a non-leader redirects
// (ErrNotLeader) rather than answering from its own possibly-lagging view.
func (n *Node) proposeRemove(nodeID string) error {
	if !n.isLeader() {
		return raftnode.ErrNotLeader
	}
	if !n.isClusterMember(nodeID) {
		return fmt.Errorf("node %s is not a cluster member", nodeID)
	}
	deadline := n.clock.Now().Add(30 * time.Second)
	for n.clock.Now().Before(deadline) {
		if err := n.removeAttempt(nodeID); err != nil {
			return err
		}
		for until := time.Now().Add(3 * time.Second); time.Now().Before(until); {
			if !n.isClusterMember(nodeID) {
				return nil
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return fmt.Errorf("cluster: remove did not take effect before the deadline")
}

// removeAttempt runs one removal attempt on the loop: validate the gate, then
// propose the Raft conf change. Idempotent — a node already gone is success.
func (n *Node) removeAttempt(nodeID string) error {
	ch := make(chan error, 1)
	n.loop.Post(func() {
		if lead, _ := n.raft.Leader(); lead != n.cfg.RaftID {
			ch <- raftnode.ErrNotLeader
			return
		}
		var raftID uint64
		found := false
		for _, m := range n.raft.Members() {
			if string(m.Addr) == nodeID {
				raftID, found = m.ID, true
				break
			}
		}
		if !found {
			ch <- nil // already removed (a retry that raced a commit)
			return
		}
		if raftID == n.cfg.RaftID {
			ch <- fmt.Errorf("a node cannot remove itself; issue remove from another node")
			return
		}
		if reason := n.removeBlockedReason(nodeID); reason != "" {
			ch <- errors.New(reason)
			return
		}
		if err := n.raft.RemoveNode(raftID); err != nil {
			ch <- err
			return
		}
		log.Printf("cluster: removing node %s (raft id %d)", nodeID, raftID)
		ch <- nil
	})
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		return fmt.Errorf("cluster: remove timed out")
	}
}

// removeBlockedReason returns a non-empty explanation when a node may not be
// removed (ADR-0004): durability is never traded for a shrink. A node must be
// drained (its shards migrated off) and empty — the remaining nodes must still
// hold every stored object at its full width, since k is never downgraded and
// re-encode to a smaller profile is not yet available. Runs on the loop.
func (n *Node) removeBlockedReason(nodeID string) string {
	cl, ok := n.raft.Store().ClusterLayout()
	if !ok {
		return "" // no layout yet — no committed data to strand
	}
	if len(cl.Previous) > 0 {
		return "a layout transition is in progress; wait for it to finish before removing a node"
	}
	draining := false
	active := 0
	for _, m := range cl.EffectiveNodes() {
		if m.ID == nodeID {
			draining = m.Draining
		}
		if !m.Draining {
			active++
		}
	}
	if !draining {
		return fmt.Sprintf("node %s must be drained before removal — run `cluster drain %s` and let it finish", nodeID, nodeID)
	}
	if w := n.maxStoredWidth(); w > active {
		return fmt.Sprintf("removing %s would leave %d active node(s), fewer than the widest stored object needs (%d shards); its data has not fully migrated off (re-encoding existing data to a smaller profile is not yet available)", nodeID, active, w)
	}
	return ""
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

// isLeader reports, on the loop, whether this node is the current Raft leader.
func (n *Node) isLeader() bool {
	ch := make(chan bool, 1)
	n.loop.Post(func() {
		lead, _ := n.raft.Leader()
		ch <- lead == n.cfg.RaftID
	})
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		return false
	}
}

// isClusterMember reports, on the loop, whether nodeID is in the current Raft
// membership.
func (n *Node) isClusterMember(nodeID string) bool {
	ch := make(chan bool, 1)
	n.loop.Post(func() {
		for _, m := range n.raft.Members() {
			if string(m.Addr) == nodeID {
				ch <- true
				return
			}
		}
		ch <- false
	})
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		return true // loop wedged; do not falsely report removed
	}
}

type joinOutcome struct {
	joinResponse
	joinedNodeID string
}

func refuse(format string, args ...any) joinOutcome {
	return joinOutcome{joinResponse: joinResponse{Error: fmt.Sprintf(format, args...)}}
}

// handleJoin runs the issuing side of the join protocol: token, identity
// checks, certificate, Raft ID, address book.
func (n *Node) handleJoin(payload []byte) joinOutcome {
	if n.ca == nil {
		return refuse("this node cannot issue certificates; join through the init node")
	}
	req, err := decodeJoinRequest(payload)
	if err != nil {
		return refuse("malformed join request")
	}
	if req.NodeID == "" || req.ClusterAddr == "" {
		return refuse("a node ID and a cluster address are required")
	}
	tok, err := decodeToken(req.Token)
	if err != nil {
		return refuse("%v", err)
	}
	if err := consumeToken(n.dir, tok.ID, tok.Secret, time.Now()); err != nil {
		return refuse("%v", err)
	}

	n.issueMu.Lock()
	defer n.issueMu.Unlock()
	members := n.members()
	for _, m := range members {
		if m.NodeID == req.NodeID {
			return refuse("node ID %q is already a cluster member", req.NodeID)
		}
	}

	// Allocate the Raft ID durably before handing it out: a crash between
	// the two must waste an ID, never reuse one. Record the joiner's
	// failure-domain labels in the same durable write (ADR-0016): the issuer
	// is the one place every member's host/zone is known, and the layout
	// reconcile reads this registry to compose a labeled layout.
	raftID := n.cfg.NextRaftID
	n.cfg.NextRaftID++
	host, zone := req.Host, req.Zone
	if host == "" {
		host = req.NodeID
	}
	if zone == "" {
		zone = host
	}
	n.cfg.NodeLabels = upsertLabel(n.cfg.NodeLabels, Member{NodeID: req.NodeID, Host: host, Zone: zone, Capacity: req.Capacity})
	if err := saveConfig(n.dir, n.cfg); err != nil {
		return refuse("recording the new member: %v", err)
	}

	cert, err := n.ca.Issue(req.NodeID, time.Now())
	if err != nil {
		return refuse("issuing certificate: %v", err)
	}
	certPEM, keyPEM, err := certs.CertPEMs(cert)
	if err != nil {
		return refuse("encoding certificate: %v", err)
	}
	return joinOutcome{
		joinedNodeID: req.NodeID,
		joinResponse: joinResponse{
			Cluster: n.cfg.Cluster, RaftID: raftID,
			CAPEM: n.ca.CertPEM(), CertPEM: certPEM, KeyPEM: keyPEM,
			Members: members,
		},
	}
}
