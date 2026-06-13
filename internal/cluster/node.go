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
	"strings"
	"sync"
	"time"

	"github.com/hamster-storage/hamster/internal/certs"
	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/ec"
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
	cfg  NodeConfig
	dir  string
	ca   *certs.CA // nil on nodes that cannot issue
	loop *sys.Loop

	transport *sys.Transport
	raft      *raftnode.Node
	rng       *mathrand.Rand // loop-owned, shared by raft timing and minting
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
	loop := sys.NewLoop()
	n := &Node{cfg: cfg, dir: dir, ca: ca, loop: loop, stopSync: make(chan struct{})}

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
		rn, err := raftnode.New(raftnode.Config{
			ID: cfg.RaftID, Peers: raftPeers, Dials: dials, Join: cfg.Join,
			Clock:     clock,
			Transport: raftTransport{transport}, Disk: disk,
			Rand:         rng,
			TickInterval: tickInterval, ElectionTicks: electionTicks,
			OnMembershipChange: onMembership,
		})
		n.raft = rn
		// Every cluster node is a shard holder: the data-plane service
		// answers writes, reads, verifies, and deletes from its peers.
		n.data = datapath.New(datapath.Config{Clock: clock, Transport: transport, Disk: disk})
		// The coordinator drives this node's own S3 operations. The
		// member set and the auto-ladder profile are read per operation
		// from the replicated membership, so every node places across
		// the same converged set with no restarts (derived placement,
		// ADR-0027 decision 2 — joins after data exists still wait for
		// the stored layout in v0.4 to move existing shards).
		members := func() []seam.NodeID {
			var ms []seam.NodeID
			for _, m := range rn.Members() {
				ms = append(ms, m.Addr)
			}
			return ms
		}
		n.coord = coord.New(coord.Config{
			Clock: clock, Rand: rng, Data: n.data, Raft: rn,
			Members:        members,
			PartitionCount: place.DefaultPartitionCount,
			Profile:        func() ec.Profile { return ec.AutoProfile(len(members())) },
		})
		built <- err
	})
	if err := <-built; err != nil {
		transport.Close()
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
	})
}

// Addr is the transport address; JoinAddr the join/status listener's.
func (n *Node) Addr() string     { return n.transport.Addr() }
func (n *Node) JoinAddr() string { return n.joinLn.Addr().String() }

// members snapshots membership and leadership from the loop.
func (n *Node) members() []Member {
	done := make(chan []Member, 1)
	n.loop.Post(func() {
		lead, _ := n.raft.Leader()
		var ms []Member
		for _, m := range n.raft.Members() {
			ms = append(ms, Member{
				RaftID: m.ID, NodeID: string(m.Addr), Dial: m.Dial,
				Learner: m.Learner, Leader: m.ID == lead,
			})
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
		}
	}
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
	// the two must waste an ID, never reuse one.
	raftID := n.cfg.NextRaftID
	n.cfg.NextRaftID++
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
