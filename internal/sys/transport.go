package sys

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// Transport implements seam.Transport over TCP with mutual TLS — always
// (ADR-0022): there is no plaintext mode, and the constructor refuses to
// build one. A peer's identity is its certificate: the client config pins
// ServerName to the peer's node ID, the listener requires a client
// certificate from the cluster CA, and the delivered "from" is the common
// name the handshake verified — never a header a peer could forge.
//
// The semantics are exactly the seam contract: Send never blocks and never
// reports delivery. Each peer gets one sender goroutine and a bounded
// queue; a full queue, a failed dial, or a broken connection drops
// messages, and the next message redials. Raft is built for that —
// anything stronger is the core's job.
//
// Per the no-logic rule, deliveries are handed to the Deliver callback on
// reader goroutines; the composition root posts them to the node's loop.
type Transport struct {
	cfg  TransportConfig
	ln   net.Listener
	done chan struct{}

	mu     sync.Mutex
	addrs  map[seam.NodeID]string // peer dial addresses; grows via AddPeer
	peers  map[seam.NodeID]*peer
	conns  map[net.Conn]bool // inbound, so Close can unblock readers
	closed bool
	wg     sync.WaitGroup
}

// TransportConfig assembles a Transport. All fields are required.
type TransportConfig struct {
	// NodeID is this node's identity; the certificate must be issued to
	// it (internal/certs binds the ID as common name and DNS SAN).
	NodeID seam.NodeID
	// Listen is the address to accept peers on.
	Listen string
	// Peers seeds the dial addresses: node ID → address. The transport
	// copies it; AddPeer registers peers discovered later (cluster
	// growth). A peer's first registered address wins — moving a node
	// means restarting the cluster's transports, addresses are static
	// state (v0.2).
	Peers map[seam.NodeID]string
	// Cert is this node's certificate and key; CA is the cluster CA both
	// sides verify against.
	Cert tls.Certificate
	CA   *x509.CertPool
	// Deliver receives each inbound message with its authenticated
	// sender. It is called on reader goroutines: post to the loop.
	Deliver func(from seam.NodeID, msg []byte)
}

// Frame and queue limits. The frame cap bounds a peer's ability to balloon
// memory and comfortably fits a metadata snapshot (MsgSnap); the queue
// absorbs bursts while a connection dials; the write deadline keeps a
// stalled peer from wedging its sender.
const (
	maxFrame     = 128 << 20
	peerQueue    = 256
	dialWait     = 2 * time.Second
	writeTimeout = 10 * time.Second
)

// NewTransport starts listening and returns the transport.
func NewTransport(cfg TransportConfig) (*Transport, error) {
	if cfg.Cert.Certificate == nil || cfg.CA == nil {
		return nil, fmt.Errorf("transport: mTLS material is required (ADR-0022); there is no plaintext mode")
	}
	ln, err := tls.Listen("tcp", cfg.Listen, &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cfg.Cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    cfg.CA,
	})
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", cfg.Listen, err)
	}
	t := &Transport{
		cfg:   cfg,
		ln:    ln,
		done:  make(chan struct{}),
		addrs: make(map[seam.NodeID]string),
		peers: make(map[seam.NodeID]*peer),
		conns: make(map[net.Conn]bool),
	}
	for id, addr := range cfg.Peers {
		t.addrs[id] = addr
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

// Addr is the address the transport accepts peers on.
func (t *Transport) Addr() string { return t.ln.Addr().String() }

// AddPeer registers a peer discovered after construction — a node that
// joined the cluster. The first registered address for an ID wins.
func (t *Transport) AddPeer(id seam.NodeID, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.addrs[id]; !ok {
		t.addrs[id] = addr
	}
}

// Send implements seam.Transport.
func (t *Transport) Send(to seam.NodeID, msg []byte) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	addr, ok := t.addrs[to]
	if !ok {
		t.mu.Unlock()
		return
	}
	p, ok := t.peers[to]
	if !ok {
		p = &peer{queue: make(chan []byte, peerQueue)}
		t.peers[to] = p
		t.wg.Add(1)
		go t.sendLoop(p, to, addr)
	}
	t.mu.Unlock()

	select {
	case p.queue <- slices.Clone(msg): // the caller may reuse msg
	default: // full queue: drop, per the seam contract
	}
}

// Close stops the listener, the senders, and the readers, then waits.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.done)
	for conn := range t.conns {
		conn.Close() // unblock readers
	}
	t.mu.Unlock()
	err := t.ln.Close()
	t.wg.Wait()
	return err
}

type peer struct {
	queue chan []byte
}

// sendLoop owns one peer's connection: dial on demand, frame and write
// each queued message, drop and redial on any error.
func (t *Transport) sendLoop(p *peer, to seam.NodeID, addr string) {
	defer t.wg.Done()
	var conn net.Conn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	clientTLS := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{t.cfg.Cert},
		RootCAs:      t.cfg.CA,
		ServerName:   string(to), // the peer must prove it is this node
	}
	for {
		var msg []byte
		select {
		case <-t.done:
			return
		case msg = <-p.queue:
		}
		if conn == nil {
			d := &net.Dialer{Timeout: dialWait}
			c, err := tls.DialWithDialer(d, "tcp", addr, clientTLS)
			if err != nil {
				continue // drop; the next message redials
			}
			conn = c
		}
		frame := binary.BigEndian.AppendUint32(nil, uint32(len(msg)))
		frame = append(frame, msg...)
		conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if _, err := conn.Write(frame); err != nil {
			conn.Close()
			conn = nil // drop; the next message redials
		}
	}
}

// acceptLoop admits peer connections and spawns a reader per connection.
func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			return // closed
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			conn.Close()
			return
		}
		t.conns[conn] = true
		t.wg.Add(1)
		t.mu.Unlock()
		go t.readLoop(conn.(*tls.Conn))
	}
}

// readLoop authenticates one inbound connection and delivers its frames.
func (t *Transport) readLoop(conn *tls.Conn) {
	defer t.wg.Done()
	defer func() {
		conn.Close()
		t.mu.Lock()
		delete(t.conns, conn)
		t.mu.Unlock()
	}()
	if err := conn.Handshake(); err != nil {
		return // not a cluster peer
	}
	peerCerts := conn.ConnectionState().PeerCertificates
	if len(peerCerts) == 0 || peerCerts[0].Subject.CommonName == "" {
		return
	}
	from := seam.NodeID(peerCerts[0].Subject.CommonName)

	var header [4]byte
	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return
		}
		size := binary.BigEndian.Uint32(header[:])
		if size > maxFrame {
			return // a peer this confused gets disconnected
		}
		msg := make([]byte, size)
		if _, err := io.ReadFull(conn, msg); err != nil {
			return
		}
		t.cfg.Deliver(from, msg)
	}
}
