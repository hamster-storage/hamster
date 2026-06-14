package cluster

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log"
	mathrand "math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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

// Node is one running cluster node: the Raft-replicated metadata plane
// over the production adapters, plus the join/status listener. In v0.2
// this is the whole node — the S3 gateway joins it when the data path
// can replicate (v0.3).
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
	s3        *s3Server // nil unless ServeS3 was asked for
	joinLn    net.Listener
	stopSync  chan struct{}

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
	n := &Node{cfg: cfg, dir: dir, ca: ca, loop: loop, metadb: mdb, stopSync: make(chan struct{})}

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
	})
	if err != nil {
		mdb.Close()
		loop.Stop()
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
			Persister:          mdb,
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
				eff := cl.EffectiveNodes()
				nodes := make([]place.Node, len(eff))
				for i, e := range eff {
					nodes[i] = place.Node{ID: seam.NodeID(e.ID), Host: e.Host, Zone: e.Zone, Weight: e.Weight}
				}
				return place.Layout{Version: cl.Version, PartitionCount: cl.PartitionCount, Members: nodes}, true
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

	// Membership grows the transport's address book: joined members appear
	// in the replicated state, the transport learns where they dial.
	go n.syncPeers()

	ln, err := tls.Listen("tcp", cfg.JoinAddr, &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		// Join requests arrive without a certificate (that is the point);
		// status requires one, checked per request.
		ClientAuth: tls.VerifyClientCertIfGiven,
		ClientCAs:  pool,
	})
	if err != nil {
		n.Stop()
		return nil, fmt.Errorf("cluster: join listener on %s: %w", cfg.JoinAddr, err)
	}
	n.joinLn = ln
	go n.acceptLoop()
	return n, nil
}

// raftTransport wraps Raft traffic in the channel envelope on its way to
// the shared transport.
type raftTransport struct{ t seam.Transport }

func (rt raftTransport) Send(to seam.NodeID, msg []byte) {
	rt.t.Send(to, datapath.Wrap(datapath.ChannelRaft, msg))
}

// Stop shuts the node down: S3 listener, join listener, peer sync,
// transport, loop — HTTP before the loop, per the gateway contract.
// Stopping twice is fine.
func (n *Node) Stop() {
	n.stopped.Do(func() {
		if n.s3 != nil {
			n.s3.stop()
		}
		close(n.stopSync)
		if n.joinLn != nil {
			n.joinLn.Close()
		}
		n.transport.Close()
		n.loop.Stop()
		// After the loop has stopped, no apply can touch the store; the
		// durable metadata mirror is safe to close.
		if n.metadb != nil {
			n.metadb.Close()
		}
	})
}

// Addr is the transport address; JoinAddr the join/status listener's.
func (n *Node) Addr() string     { return n.transport.Addr() }
func (n *Node) JoinAddr() string { return n.joinLn.Addr().String() }

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
				mem.Host, mem.Zone, mem.Capacity = lbl.Host, lbl.Zone, lbl.Weight
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
			// a leader exists and advances it as the cluster forms.
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
		labels[r.NodeID] = meta.LayoutNode{ID: r.NodeID, Host: r.Host, Zone: r.Zone, Weight: r.Capacity}
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
	if ok && slices.Equal(cur.EffectiveNodes(), desired) {
		return // already current
	}
	next := uint64(1)
	pc := uint32(place.DefaultPartitionCount)
	if ok {
		next = cur.Version + 1
		pc = cur.PartitionCount
	}
	n.raft.Propose(meta.SetClusterLayout{
		ProposedAtUnixMS: n.clock.Now().UnixMilli(),
		Version:          next,
		PartitionCount:   pc,
		Nodes:            desired,
	}, func(any, error) {}) // stale / not-leader outcomes are benign
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

func (n *Node) acceptLoop() {
	for {
		conn, err := n.joinLn.Accept()
		if err != nil {
			return // closed
		}
		go n.handleConn(conn.(*tls.Conn))
	}
}

// handleConn serves one request — join or status — and closes.
func (n *Node) handleConn(conn *tls.Conn) {
	defer conn.Close()
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
	}
	// Unknown kinds get no response: an upgraded client will know why.
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
