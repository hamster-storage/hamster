package sys

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/certs"
	"github.com/hamster-storage/hamster/internal/seam"
)

// TestTransportLiveTrustRotation: the server consults its trust pool per
// handshake (ADR-0033), so widening it to a new CA admits that CA's leaves
// without a restart — the dual-trust rollover. A new-CA client is rejected
// before the widening and accepted after, on fresh dials.
func TestTransportLiveTrustRotation(t *testing.T) {
	now := time.Now()
	oldCA, err := certs.NewCA("old", now)
	if err != nil {
		t.Fatal(err)
	}
	newCA, err := certs.NewCA("new", now)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := certs.PoolFromCAs([][]byte{oldCA.CertPEM(), newCA.CertPEM()})
	if err != nil {
		t.Fatal(err)
	}

	// The server n2 reads its trust pool live; it starts trusting only the old CA.
	var roots atomic.Pointer[x509.CertPool]
	roots.Store(oldCA.Pool())
	n2Leaf, _ := oldCA.Issue("n2", now)
	inbox := make(chan delivery, 8)
	n2, err := NewTransport(TransportConfig{
		NodeID: "n2", Listen: "127.0.0.1:0", Peers: map[seam.NodeID]string{},
		Cert:    n2Leaf,
		Roots:   func() *x509.CertPool { return roots.Load() },
		Deliver: func(from seam.NodeID, msg []byte) { inbox <- delivery{from, msg} },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()

	// A fresh client presenting a NEW-CA leaf for "n1". Each Send from a fresh
	// transport dials fresh, so it always sees the server's current trust.
	dialNewCAClient := func(msg string) {
		leaf, _ := newCA.Issue("n1", now)
		c, err := NewTransport(TransportConfig{
			NodeID: "n1", Listen: "127.0.0.1:0",
			Peers: map[seam.NodeID]string{"n2": n2.Addr()},
			Cert:  leaf, CA: oldCA.Pool(), // trusts n2's old-CA server cert
			Deliver: func(seam.NodeID, []byte) {},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.Send("n2", []byte(msg))
		time.Sleep(300 * time.Millisecond) // let the dial + handshake resolve
	}

	// Before widening: the new-CA leaf does not verify, nothing is delivered.
	dialNewCAClient("rejected")
	select {
	case d := <-inbox:
		t.Fatalf("new-CA leaf accepted before trust widened: %q", d.msg)
	default:
	}

	// Widen the trust pool to the bundle — live, no restart.
	roots.Store(bundle)

	dialNewCAClient("accepted")
	select {
	case d := <-inbox:
		if string(d.msg) != "accepted" || d.from != "n1" {
			t.Fatalf("delivery %q from %s, want accepted from n1", d.msg, d.from)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("new-CA leaf not accepted after trust widened (live pool not consulted)")
	}
}

type delivery struct {
	from seam.NodeID
	msg  []byte
}

// testNet is a little cluster of transports sharing one CA, with delivery
// channels per node.
type testNet struct {
	ca    *certs.CA
	cfg   map[seam.NodeID]TransportConfig
	trans map[seam.NodeID]*Transport
	inbox map[seam.NodeID]chan delivery
}

func newTestNet(t *testing.T, ids ...seam.NodeID) *testNet {
	t.Helper()
	now := time.Now()
	ca, err := certs.NewCA("test", now)
	if err != nil {
		t.Fatal(err)
	}
	n := &testNet{
		ca:    ca,
		cfg:   make(map[seam.NodeID]TransportConfig),
		trans: make(map[seam.NodeID]*Transport),
		inbox: make(map[seam.NodeID]chan delivery),
	}
	peers := make(map[seam.NodeID]string)
	for _, id := range ids {
		cert, err := ca.Issue(string(id), now)
		if err != nil {
			t.Fatal(err)
		}
		inbox := make(chan delivery, 64)
		n.inbox[id] = inbox
		n.cfg[id] = TransportConfig{
			NodeID: id, Listen: "127.0.0.1:0", Peers: peers, // shared map, filled below
			Cert: cert, CA: ca.Pool(),
			Deliver: func(from seam.NodeID, msg []byte) {
				inbox <- delivery{from, msg}
			},
		}
	}
	for _, id := range ids {
		n.start(t, id)
	}
	// The transport copies Peers at construction, so nodes started before
	// a later node bound its port never saw its address — register the
	// full book on everyone, the same way cluster growth does.
	for _, tr := range n.trans {
		for id, addr := range peers {
			tr.AddPeer(id, addr)
		}
	}
	t.Cleanup(func() {
		for _, tr := range n.trans {
			tr.Close()
		}
	})
	return n
}

// start (re)opens one node's transport. The first start binds :0 and pins
// the assigned address — restarts rebind it, so the shared peer map never
// changes once sends begin (the production contract: addresses are
// static).
func (n *testNet) start(t *testing.T, id seam.NodeID) {
	t.Helper()
	cfg := n.cfg[id]
	tr, err := NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	n.trans[id] = tr
	if _, pinned := cfg.Peers[id]; !pinned {
		cfg.Peers[id] = tr.Addr()
		cfg.Listen = tr.Addr()
		n.cfg[id] = cfg
	}
}

func (n *testNet) expect(t *testing.T, at seam.NodeID, from seam.NodeID, msg []byte) {
	t.Helper()
	select {
	case d := <-n.inbox[at]:
		if d.from != from || !bytes.Equal(d.msg, msg) {
			t.Fatalf("delivery at %s: %q from %s, want %q from %s", at, d.msg, d.from, msg, from)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no delivery at %s within 10s", at)
	}
}

func TestTransportRoundTrip(t *testing.T) {
	n := newTestNet(t, "n1", "n2")

	n.trans["n1"].Send("n2", []byte("hello from n1"))
	n.expect(t, "n2", "n1", []byte("hello from n1"))

	n.trans["n2"].Send("n1", []byte("hello back"))
	n.expect(t, "n1", "n2", []byte("hello back"))

	// A multi-megabyte frame (a snapshot-sized message) arrives intact.
	big := bytes.Repeat([]byte("snapshot"), 1<<19) // 4 MiB
	n.trans["n1"].Send("n2", big)
	n.expect(t, "n2", "n1", big)
}

// TestTransportAuthenticatesPeers: a process with a certificate from a
// different CA reaches the port and gets nothing — its messages never
// surface, and the cluster's own traffic is unaffected.
func TestTransportAuthenticatesPeers(t *testing.T) {
	n := newTestNet(t, "n1", "n2")

	rogueCA, err := certs.NewCA("rogue", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	rogueCert, err := rogueCA.Issue("n2", time.Now()) // claims to be n2
	if err != nil {
		t.Fatal(err)
	}
	rogue, err := NewTransport(TransportConfig{
		NodeID: "n2", Listen: "127.0.0.1:0",
		Peers: map[seam.NodeID]string{"n1": n.trans["n1"].Addr()},
		Cert:  rogueCert, CA: rogueCA.Pool(),
		Deliver: func(seam.NodeID, []byte) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rogue.Close()

	rogue.Send("n1", []byte("let me in"))
	n.trans["n2"].Send("n1", []byte("legitimate"))
	n.expect(t, "n1", "n2", []byte("legitimate"))
	select {
	case d := <-n.inbox["n1"]:
		t.Fatalf("rogue delivery surfaced: %q from %s", d.msg, d.from)
	default:
	}
}

// TestTransportControlConnRouted: a client that does not negotiate the peer
// ALPN — the shape of a join/status client, certless and protocol-less — is
// handed to OnControl, not the peer delivery path. This is what lets one port
// serve both the transport and the join/status protocol.
func TestTransportControlConnRouted(t *testing.T) {
	now := time.Now()
	ca, err := certs.NewCA("test", now)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ca.Issue("n1", now)
	if err != nil {
		t.Fatal(err)
	}
	control := make(chan struct{}, 1)
	delivered := make(chan struct{}, 1)
	tr, err := NewTransport(TransportConfig{
		NodeID: "n1", Listen: "127.0.0.1:0", Peers: map[seam.NodeID]string{},
		Cert: cert, CA: ca.Pool(),
		Deliver:   func(seam.NodeID, []byte) { delivered <- struct{}{} },
		OnControl: func(c *tls.Conn) { control <- struct{}{}; c.Close() },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	conn, err := tls.Dial("tcp", tr.Addr(), &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    ca.Pool(),
		ServerName: "n1", // verify the server; present no client cert, no ALPN
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	select {
	case <-control:
	case <-time.After(5 * time.Second):
		t.Fatal("a non-peer connection was not routed to OnControl")
	}
	select {
	case <-delivered:
		t.Fatal("a control connection reached the peer delivery path")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestTransportPlaintextRefused: the constructor enforces ADR-0022.
func TestTransportPlaintextRefused(t *testing.T) {
	_, err := NewTransport(TransportConfig{
		NodeID: "n1", Listen: "127.0.0.1:0",
		Peers: map[seam.NodeID]string{},
		Cert:  tls.Certificate{}, CA: nil,
		Deliver: func(seam.NodeID, []byte) {},
	})
	if err == nil {
		t.Fatal("a transport without mTLS material was constructed")
	}
}

// TestTransportPeerOutage: sends to a dead peer return immediately and
// drop; once the peer is back, traffic flows again.
func TestTransportPeerOutage(t *testing.T) {
	n := newTestNet(t, "n1", "n2")

	n.trans["n2"].Close()
	start := time.Now()
	for i := range 10 {
		n.trans["n1"].Send("n2", []byte{byte(i)})
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Send blocked %v on a dead peer; the contract is never-blocks", took)
	}

	n.start(t, "n2")
	// Keep sending until a post-recovery message lands. Outage-era
	// messages may surface late — the seam contract allows delay — so
	// drain anything that is not ours.
	deadline := time.After(15 * time.Second)
	for i := 0; ; i++ {
		n.trans["n1"].Send("n2", fmt.Appendf(nil, "recovered-%d", i))
		select {
		case d := <-n.inbox["n2"]:
			if d.from != "n1" {
				t.Fatalf("delivery from unexpected sender %s", d.from)
			}
			if bytes.HasPrefix(d.msg, []byte("recovered-")) {
				return
			}
		case <-deadline:
			t.Fatal("no delivery after peer recovery")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
