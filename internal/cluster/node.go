package cluster

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hamster-storage/hamster/internal/certs"
	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/datapath"
	"github.com/hamster-storage/hamster/internal/keys"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/metrics"
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

// Background-scrub pacing (ADR-0009). Gentle by intent: the scrub is a safety
// net against bitrot, not a hot path, so it walks objects unhurriedly and idles
// between passes. Byte-rate throttling and an operator-tunable rate arrive with
// the operational repair system; these are the v0.4 dev-preview defaults.
const (
	scrubPace         = 500 * time.Millisecond
	scrubPassInterval = 15 * time.Second
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
	masterKey keys.KEK      // the cluster KEK (ADR-0021), loaded from the operator's key source; zero if none
	newKey    keys.KEK      // the incoming KEK during a master-key rotation (ADR-0032), loaded from -new-master-key-file; zero otherwise
	ready     chan struct{} // closed once the node is built; gates control conns
	stopSync  chan struct{}

	// Live mTLS material for CA rotation (ADR-0033): the transport reads these
	// per handshake, so a reissued leaf and a widened trust bundle take effect
	// without a restart. leaf is this node's current certificate; trust is the
	// pool built from the replicated TrustBundle (or the boot CA before one is
	// installed). bootCAPEM is the founding CA certificate, used to seed the
	// first bundle. trustVersion tracks the bundle generation trust was built
	// from, so refreshTrust rebuilds only on change.
	leaf         atomic.Pointer[tls.Certificate]
	trust        atomic.Pointer[x509.CertPool]
	bootCAPEM    []byte
	trustVersion uint64      // loop-owned
	caRotating   atomic.Bool // a CA rotation is driving on this node — reject a concurrent one (e.g. a dropped-call retry)

	sweeping bool // loop-owned: a transition repair sweep is in flight (one at a time)

	// Version advertisement (ADR-0034): this binary's release string and declared
	// protocol generation, supplied at startup (WithVersion) and not persisted —
	// the binary owns them, read fresh each boot. The leader's version monitor
	// replicates them into each member's NodeRecord so the cluster's effective
	// generation (the min across live members) rolls forward etcd-style as the
	// last node upgrades. Zero generation means unset (treated as behind).
	binaryVersion string
	generation    uint32

	// Observability (ADR-0035): the node's metrics registry, and the start time
	// uptime is measured from (via the seam clock). Collectors registered on the
	// registry read live cluster state at scrape time.
	metrics       *metrics.Registry
	s3Requests    *metrics.Counter   // incremented by the ServeS3 middleware
	s3ReqDuration *metrics.Histogram // per-operation latency, observed by the coordinator (ADR-0039)
	s3RequestShed *metrics.Counter   // requests shed at admission by the load shedder, by method (ADR-0039)

	putInflight          *metrics.Gauge   // streaming PUTs currently in flight
	putBytes             *metrics.Counter // object bytes accepted by completed PUTs
	putBackpressureWaits *metrics.Counter // feeder stalls waiting on the coordinator

	startAt time.Time

	issueMu sync.Mutex // serializes joins: ID allocation and its durable record
	stopped sync.Once
}

// Option configures a node at startup. Used for runtime inputs that must
// not persist in node.conf — chiefly the cluster KEK (ADR-0021), which
// lives only in memory.
type Option func(*nodeOptions)

type nodeOptions struct {
	masterKey     keys.KEK
	newKey        keys.KEK
	binaryVersion string
	generation    uint32
}

// WithVersion supplies this binary's release string (for display) and its
// declared protocol generation (ADR-0034): the monotonic integer the binary
// owns, used for the cluster's effective generation and the skew check. Not
// persisted — the binary owns these and they are read fresh each boot, so an
// in-place upgrade advertises the new values without a re-join. Omit on a node
// that does not advertise (generation 0 = unset).
func WithVersion(release string, generation uint32) Option {
	return func(o *nodeOptions) { o.binaryVersion, o.generation = release, generation }
}

// WithMasterKey supplies the loaded cluster KEK the node uses to wrap and
// unwrap object keys. The key is held in memory only and never persisted —
// the caller reads it from the operator's key source (a mounted file) at
// startup. Omit it for an unencrypted cluster.
func WithMasterKey(kek keys.KEK) Option {
	return func(o *nodeOptions) { o.masterKey = kek }
}

// WithNewMasterKey supplies the incoming KEK during a master-key rotation
// (ADR-0032): the key the cluster is rotating to, loaded from
// -new-master-key-file. Held in memory only, like the master key, and never
// sent over the wire — the operator provisions it on every node out of band
// (the same way the master key arrives). Omit it when no rotation is in flight.
func WithNewMasterKey(kek keys.KEK) Option {
	return func(o *nodeOptions) { o.newKey = kek }
}

// keyByFingerprint resolves one of the node's loaded KEKs by its content
// fingerprint (ADR-0032): the master key, or the new key during a rotation.
// A zero fingerprint, or one the node does not hold, reports not-found.
func (n *Node) keyByFingerprint(fingerprint uint64) (keys.KEK, bool) {
	if fingerprint == 0 {
		return keys.KEK{}, false
	}
	if n.masterKey.Loaded() && n.masterKey.Fingerprint().Uint64() == fingerprint {
		return n.masterKey, true
	}
	if n.newKey.Loaded() && n.newKey.Fingerprint().Uint64() == fingerprint {
		return n.newKey, true
	}
	return keys.KEK{}, false
}

// Run starts a cluster node from its data directory. The caller owns it
// and must Stop it.
func Run(dataDir string, opts ...Option) (*Node, error) {
	var o nodeOptions
	for _, opt := range opts {
		opt(&o)
	}
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return nil, err
	}
	cert, pool, bootCAPEM, err := loadNodeTLS(dir)
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
	n := &Node{cfg: cfg, dir: dir, ca: ca, loop: loop, metadb: mdb, masterKey: o.masterKey, newKey: o.newKey, binaryVersion: o.binaryVersion, generation: o.generation, bootCAPEM: bootCAPEM, ready: make(chan struct{}), stopSync: make(chan struct{})}
	// Seed the live mTLS material from the boot certificate and CA (ADR-0033);
	// refreshTrust later rebuilds the pool from the replicated trust bundle.
	bootCert := cert
	n.leaf.Store(&bootCert)
	n.trust.Store(pool)

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
		// Live mTLS material (ADR-0033): the transport reads this node's current
		// leaf and trust pool per handshake, so a CA rotation's reissued leaf and
		// widened bundle take effect without a restart.
		Leaf:  func() tls.Certificate { return *n.leaf.Load() },
		Roots: func() *x509.CertPool { return n.trust.Load() },
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
		n.startAt = clock.Now()
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
			Clock: clock, Rand: rng, Data: n.data, Raft: forwardingProposer{n: n},
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
			// Encryption at rest (ADR-0021, ADR-0032): the write posture is the
			// replicated cluster fact; the KEK is this node's in-memory key. The
			// write key is the rotating-to key while a master-key rotation is open
			// (so new writes land on the new key and never extend the rotation),
			// else the cluster's current key. A node whose posture is on but whose
			// key never loaded carries a zero KEK here, so the coordinator refuses
			// encrypted work loudly rather than serving ciphertext. Entropy is
			// crypto/rand in production (the DEK's only randomness).
			Encryption: func() (keys.KEK, bool) {
				post := rn.Store().EncryptionPosture()
				if post.Algorithm == meta.EncNone {
					return keys.KEK{}, false
				}
				target := post.CurrentKEKFingerprint
				if post.RotatingToKEKFingerprint != 0 {
					target = post.RotatingToKEKFingerprint
				}
				if k, ok := n.keyByFingerprint(target); ok {
					return k, true
				}
				return n.masterKey, true // current unestablished, or key not held: fail-closed on the master key
			},
			// Keyring resolves a loaded KEK by fingerprint (ADR-0032): a GET reads
			// an object under whichever key wrapped it, and the rewrap sweep needs
			// the old key to unwrap and the new to rewrap.
			Keyring: n.keyByFingerprint,
			Entropy: rand.Reader,
			// Per-operation latency (ADR-0039 part 1): the coordinator times each
			// PUT and GET through the seam clock and reports the service time here,
			// which feeds the request-latency histogram. The histogram is created
			// later by initMetrics, so this reads the field at call time and
			// tolerates the not-yet-built window (like the scrape collectors) —
			// real S3 operations only run well after startup.
			ObserveLatency: func(op string, seconds float64) {
				if n.s3ReqDuration != nil {
					n.s3ReqDuration.Observe(seconds, op)
				}
			},
		})
		// The continuous background scrubber (ADR-0009): every node starts it, but
		// only the leader actually scrubs (it gates on leadership), so this simply
		// follows leadership without extra wiring. It finds bitrot and rebuilds
		// lost shards on its own, paced object-by-object.
		n.coord.StartScrub(coord.ScrubConfig{
			Pace:         scrubPace,
			PassInterval: scrubPassInterval,
			OnHeal: func(bucket, key string) {
				log.Printf("cluster: scrub repaired %s/%s", bucket, key)
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

	// The metrics registry and its collectors (ADR-0035): build/node info set
	// once, cluster-wide gauges refreshed from live state at scrape time.
	n.initMetrics()

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

// onLoop runs fn on the node's loop and returns its result. ok is false if the
// loop does not answer within timeout — wedged or stopping — leaving the caller
// to choose what a non-answer means (an empty view, or the safe assumption). A
// zero timeout waits forever. The single place the loop-roundtrip dance lives, so
// the many call sites that need a value computed on the loop stay one line each.
func onLoop[T any](n *Node, timeout time.Duration, fn func() T) (T, bool) {
	return onLoopAsync(n, timeout, func(done func(T)) { done(fn()) })
}

// onLoopAsync is onLoop for results that arrive from a later callback rather than
// fn's return — a Raft Propose completion, say. fn is posted to the loop and is
// handed a done it calls once, synchronously for an early refusal or from the
// async callback for a committed proposal. ok is false on timeout (zero waits
// forever, for sweeps that may run minutes).
func onLoopAsync[T any](n *Node, timeout time.Duration, fn func(done func(T))) (T, bool) {
	ch := make(chan T, 1)
	n.loop.Post(func() { fn(func(v T) { ch <- v }) })
	var timed <-chan time.Time
	if timeout > 0 {
		timed = time.After(timeout)
	}
	select {
	case v := <-ch:
		return v, true
	case <-timed: // a nil channel blocks forever, so timeout==0 never fires
		var zero T
		return zero, false
	}
}

// members snapshots membership and leadership from the loop, labeling each
// member with its failure-domain host/zone from the replicated layout (so
// `cluster status` reports the same labels placement uses).
func (n *Node) members() []Member {
	ms, _ := onLoop(n, 5*time.Second, func() []Member {
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
		// Each member's advertised version (ADR-0034) lives in the replicated
		// NodeRecord, not the layout — read it for display.
		recs := map[string]meta.NodeRecord{}
		for _, r := range n.raft.Store().Nodes() {
			recs[r.NodeID] = r
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
			if rec, ok := recs[string(m.Addr)]; ok {
				mem.BinaryVersion, mem.Generation = rec.BinaryVersion, rec.Generation
			}
			ms = append(ms, mem)
		}
		return ms // a wedged or stopping loop yields nil — callers see an empty cluster
	})
	return ms
}

// notLeaderMsg is what a non-leader returns for a leader-only control request
// (drain, remove, optimize); the response also carries leaderDial so the client
// can retry there — there is no proposal forwarding yet (ADR-0027).
const notLeaderMsg = "this node is not the metadata leader"

// leaderDial returns the current leader's dial address, or "" if none is known —
// the redirect target a non-leader hands back on a leader-only request.
func (n *Node) leaderDial() string {
	for _, m := range n.members() {
		if m.Leader {
			return m.Dial
		}
	}
	return ""
}

func (n *Node) syncPeers() {
	t := time.NewTicker(peerSyncEvery)
	defer t.Stop()
	tick := 0
	for {
		select {
		case <-n.stopSync:
			return
		case <-t.C:
			tick++
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
			// The version monitor (ADR-0034) polls peers and replicates their
			// advertised versions — leader-only and on a slower cadence than peer
			// sync, since a roll takes seconds to minutes per node and the poll
			// dials every peer's status.
			if tick%versionMonitorEvery == 0 {
				n.versionMonitor()
			}
		}
	}
}
