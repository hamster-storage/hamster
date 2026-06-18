package cluster

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hamster-storage/hamster/internal/certs"
	"github.com/hamster-storage/hamster/internal/metrics"
	"github.com/hamster-storage/hamster/internal/raftnode"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sys"
)

// detectHost returns this machine's identity for failure-domain placement
// (ADR-0016): the OS hostname, lowercased and trimmed. Processes on one box
// share it with zero configuration. Falls back to the node ID when the OS
// gives nothing usable, so a node always carries a host label.
func detectHost(nodeID string) string {
	if h, err := os.Hostname(); err == nil {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			return h
		}
	}
	return nodeID
}

// Init creates a new cluster: the CA, the founding node's certificate and
// identity, all under <data-dir>/cluster. The node becomes a running cluster
// the first time `cluster run` starts it (a fresh Raft log bootstraps a
// single-voter configuration). listenAddr is the node's one cluster port: the
// mTLS peer transport and the join/status protocol share it (ADR-0030).
func Init(dataDir, clusterName, nodeID, listenAddr, zone string, capacity uint32, now time.Time) error {
	if Initialized(dataDir) {
		return fmt.Errorf("cluster: %s already holds a cluster identity", Dir(dataDir))
	}
	if nodeID == "" || clusterName == "" {
		return errors.New("cluster: a cluster name and a node ID are required")
	}
	host := detectHost(nodeID)
	if zone == "" {
		zone = host
	}
	dir := Dir(dataDir)
	if err := os.MkdirAll(tokensDir(dir), 0o700); err != nil {
		return fmt.Errorf("cluster: creating %s: %w", dir, err)
	}
	ca, err := certs.NewCA(clusterName, now)
	if err != nil {
		return err
	}
	cert, err := ca.Issue(nodeID, now)
	if err != nil {
		return err
	}
	certPEM, keyPEM, err := certs.CertPEMs(cert)
	if err != nil {
		return err
	}
	caKeyPEM, err := ca.KeyPEM()
	if err != nil {
		return err
	}
	for name, data := range map[string][]byte{
		"ca.pem": ca.CertPEM(), "ca.key": caKeyPEM,
		"node.pem": certPEM, "node.key": keyPEM,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return fmt.Errorf("cluster: writing %s: %w", name, err)
		}
	}
	return saveConfig(dir, NodeConfig{
		Cluster: clusterName, NodeID: nodeID, RaftID: 1,
		ClusterAddr: listenAddr, JoinAddr: listenAddr,
		Members:    []Member{{RaftID: 1, NodeID: nodeID, Dial: listenAddr, Host: host, Zone: zone, Capacity: capacity}},
		NextRaftID: 2,
		Host:       host, Zone: zone, Capacity: capacity,
		NodeLabels: []Member{{NodeID: nodeID, Host: host, Zone: zone, Capacity: capacity}},
	})
}

// UpdateListenAddr rewrites a node's persisted cluster listen address — the
// local bind/advertise endpoint, not its identity — so an operator can move a
// node's port across restarts (chiefly to correct a first boot that failed to
// bind). The node's own entry in its address book follows. A no-op when the
// address is empty or unchanged.
//
// Caveat (v0.3): a changed advertised address is not propagated to peers, whose
// address books are frozen at admission ([ADR-0027]); so this reliably moves a
// port only while peers can still reach the node — a member re-advertise path
// is future work.
//
// [ADR-0027]: ../../docs/adr/0027-v03-distributed-data-path.md
func UpdateListenAddr(dataDir, listenAddr string) error {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return err
	}
	if listenAddr == "" || (cfg.ClusterAddr == listenAddr && cfg.JoinAddr == listenAddr) {
		return nil
	}
	cfg.ClusterAddr = listenAddr
	cfg.JoinAddr = listenAddr
	for i := range cfg.Members {
		if cfg.Members[i].NodeID == cfg.NodeID {
			cfg.Members[i].Dial = listenAddr
		}
	}
	return saveConfig(dir, cfg)
}

// MintToken mints a join token on a node holding the CA key, valid for
// ttl, single-use. The running node (or the next `cluster run`) honors it.
func MintToken(dataDir string, ttl time.Duration, now time.Time) (string, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return "", err
	}
	ca, err := loadCA(dir)
	if err != nil {
		return "", fmt.Errorf("cluster: only a node holding the cluster CA key can mint join tokens (the init node, in v0.2): %w", err)
	}
	return mintToken(dir, cfg.JoinAddr, ca.Hash(), now, ttl)
}

// Join performs the joining side of the join protocol: dial the token's
// issuer, authenticate it against the token's pinned CA hash, present the
// token, and persist the identity it returns. The node is a cluster member
// once `cluster run` starts it and admission commits.
// Join performs the joining side of the join protocol. listenAddr is the
// node's one cluster port — peer transport and join/status share it (ADR-0030)
// — advertised to the cluster as this node's dial address.
// Join admits this node to a cluster using a token (ADR-0022). replaces, when
// non-empty, names an existing member this node takes the place of (ADR-0004):
// the issuer pairs them, and the cluster swaps the new node in for the old at
// constant size — the storage profile is unchanged — then evicts the old node.
func Join(dataDir, nodeID, listenAddr, tokenStr, zone string, capacity uint32, replaces string) error {
	if Initialized(dataDir) {
		return fmt.Errorf("cluster: %s already holds a cluster identity", Dir(dataDir))
	}
	if nodeID == "" {
		return errors.New("cluster: a node ID is required")
	}
	if replaces == nodeID {
		return errors.New("cluster: a node cannot replace itself")
	}
	host := detectHost(nodeID)
	if zone == "" {
		zone = host
	}
	tok, err := decodeToken(tokenStr)
	if err != nil {
		return err
	}
	conn, err := dialPinned(tok.JoinAddr, tok.CAHash)
	if err != nil {
		return err
	}
	defer conn.Close()
	req := encodeRequest(reqJoin, encodeJoinRequest(joinRequest{
		Token: tokenStr, NodeID: nodeID, ClusterAddr: listenAddr,
		Host: host, Zone: zone, Capacity: capacity, Replaces: replaces,
	}))
	if err := writeFrame(conn, req); err != nil {
		return fmt.Errorf("cluster: sending join request: %w", err)
	}
	buf, err := readFrame(conn)
	if err != nil {
		return fmt.Errorf("cluster: reading join response: %w", err)
	}
	resp, err := decodeJoinResponse(buf)
	if err != nil {
		return fmt.Errorf("cluster: decoding join response: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("cluster: join refused: %s", resp.Error)
	}

	dir := Dir(dataDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("cluster: creating %s: %w", dir, err)
	}
	for name, data := range map[string][]byte{
		"ca.pem": resp.CAPEM, "node.pem": resp.CertPEM, "node.key": resp.KeyPEM,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return fmt.Errorf("cluster: writing %s: %w", name, err)
		}
	}
	members := append(resp.Members, Member{RaftID: resp.RaftID, NodeID: nodeID, Dial: listenAddr, Host: host, Zone: zone, Capacity: capacity})
	return saveConfig(dir, NodeConfig{
		Cluster: resp.Cluster, NodeID: nodeID, RaftID: resp.RaftID,
		ClusterAddr: listenAddr, JoinAddr: listenAddr,
		Join: true, Members: members,
		Host: host, Zone: zone, Capacity: capacity,
		NodeLabels: []Member{{NodeID: nodeID, Host: host, Zone: zone, Capacity: capacity}},
	})
}

// Recover rewrites a stopped node into a new single-voter cluster — the
// disaster exit (ADR-0025) for a cluster whose quorum is permanently
// lost. Destructive and irreversible: the other members' data directories
// hold a competing history afterward and must never run again. The
// caller (the CLI) is responsible for making the operator say so
// explicitly.
func Recover(dataDir string) (raftnode.RecoverySummary, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return raftnode.RecoverySummary{}, err
	}
	// Recovery is offline: a bound transport port means the node is up.
	probe, err := net.Listen("tcp", cfg.ClusterAddr)
	if err != nil {
		return raftnode.RecoverySummary{}, fmt.Errorf("cluster: this node appears to be running (%s is bound); stop it first", cfg.ClusterAddr)
	}
	probe.Close()

	disk, err := sys.NewDisk(dataDir)
	if err != nil {
		return raftnode.RecoverySummary{}, err
	}
	sum, err := raftnode.ForceNewCluster(disk, cfg.RaftID, seam.NodeID(cfg.NodeID), cfg.ClusterAddr)
	if err != nil {
		return raftnode.RecoverySummary{}, err
	}

	// The node's own identity record follows: sole member, founder
	// semantics, and an ID counter past everything this cluster has ever
	// handed out — a removed member's ID must never be reissued.
	next := max(cfg.NextRaftID, cfg.RaftID+1)
	for _, m := range sum.Removed {
		next = max(next, m.ID+1)
	}
	cfg.Members = []Member{{RaftID: cfg.RaftID, NodeID: cfg.NodeID, Dial: cfg.ClusterAddr}}
	cfg.Join = false
	cfg.NextRaftID = next
	if err := saveConfig(dir, cfg); err != nil {
		return raftnode.RecoverySummary{}, err
	}
	return sum, nil
}

// CanIssue reports whether this node holds the CA key — whether a
// recovered cluster can grow again by join.
func CanIssue(dataDir string) bool {
	_, err := os.Stat(filepath.Join(Dir(dataDir), "ca.key"))
	return err == nil
}

// Status queries a running node's join/status listener with this node's
// own certificate. An empty addr asks the local node.
// StatusReport is what `cluster status` shows: the members and the cluster's
// encryption-at-rest posture (ADR-0021).
type StatusReport struct {
	Members    []Member
	Encryption string // the algorithm name, or "" when the cluster does not encrypt
	// Master-key rotation (ADR-0032): the current key fingerprint when
	// encrypting, and — while a rotation is open — the target fingerprint and how
	// many versions are still on the old key.
	KEKFingerprint string
	RotatingTo     string
	Remaining      uint64
	// CA trust (ADR-0033): the trust-bundle generation this node is on, whether a
	// CA rotation is open, and how many members still hold an old-CA leaf.
	TrustVersion uint64
	CARotating   bool
	CAStragglers uint64
	// EffectiveGeneration is the cluster's effective protocol generation
	// (ADR-0034): the minimum across live members, as the answering node sees it.
	// Each member's own generation is on its Member record.
	EffectiveGeneration uint32
}

func Status(dataDir, addr string) (StatusReport, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return StatusReport{}, err
	}
	if addr == "" {
		addr = cfg.JoinAddr
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return StatusReport{}, err
	}
	buf, err := controlRoundTrip(addr, cert, pool, encodeRequest(reqStatus, nil))
	if err != nil {
		return StatusReport{}, err
	}
	resp, err := decodeStatusResponse(buf)
	if err != nil {
		return StatusReport{}, err
	}
	if resp.Error != "" {
		return StatusReport{}, fmt.Errorf("cluster: status refused: %s", resp.Error)
	}
	return StatusReport{
		Members: resp.Members, Encryption: resp.Encryption,
		KEKFingerprint: resp.KEKFingerprint, RotatingTo: resp.RotatingTo, Remaining: resp.Remaining,
		TrustVersion: resp.TrustVersion, CARotating: resp.CARotating, CAStragglers: resp.CAStragglers,
		EffectiveGeneration: resp.EffectiveGeneration,
	}, nil
}

// Metrics fetches a node's metrics snapshot (ADR-0035) over the control channel,
// authenticated by this node's own certificate (like Status). An empty addr asks
// the local node. Read-only and per-node — answered by whichever node is asked.
func Metrics(dataDir, addr string) ([]metrics.Family, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return nil, err
	}
	if addr == "" {
		addr = cfg.JoinAddr
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return nil, err
	}
	buf, err := controlRoundTrip(addr, cert, pool, encodeRequest(reqMetrics, nil))
	if err != nil {
		return nil, err
	}
	resp, err := decodeMetricsResponse(buf)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("cluster: metrics refused: %s", resp.Error)
	}
	return metrics.UnmarshalSnapshot(resp.Snapshot)
}

// CanStop asks a node whether taking nodeID down for maintenance or upgrade is
// safe (ADR-0034) — the advisory health interlock. An empty addr asks the local
// node, authenticated by this node's own certificate (like Status). The verdict
// is read-only and answered from the asked node's view, so no leader redirect.
func CanStop(dataDir, addr, nodeID string) (safe bool, reason string, err error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return false, "", err
	}
	if addr == "" {
		addr = cfg.JoinAddr
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return false, "", err
	}
	buf, err := controlRoundTrip(addr, cert, pool, encodeRequest(reqCanStop, encodeCanStopRequest(canStopRequest{NodeID: nodeID})))
	if err != nil {
		return false, "", err
	}
	resp, err := decodeCanStopResponse(buf)
	if err != nil {
		return false, "", err
	}
	if resp.Error != "" {
		return false, "", fmt.Errorf("cluster: can-stop refused: %s", resp.Error)
	}
	return resp.Safe, resp.Reason, nil
}

// Drain marks a member draining (or clears it) — an operator command that
// commits a leader-only metadata proposal (ADR-0004) over the cluster control
// port, authenticated by this node's own certificate (like Status). An empty
// addr asks the local node; a non-leader answer carries the leader's address,
// which Drain follows once so the operator need not look it up.
func Drain(dataDir, addr, nodeID string, draining bool) error {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return err
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return err
	}
	target := addr
	if target == "" {
		target = cfg.ClusterAddr
	}
	return leaderRedirect("drain", target, 0, func(addr string) (controlOutcome, error) {
		resp, err := requestDrain(addr, cert, pool, drainRequest{NodeID: nodeID, Draining: draining})
		if err != nil {
			return controlOutcome{}, err
		}
		return controlOutcome{errStr: resp.Error, leader: resp.Leader}, nil
	})
}

// requestDrain dials a node's control port with this node's certificate and
// runs one drain request/response.
// Remove evicts a node from the cluster (ADR-0004): a leader-only metadata
// operation that drops the node from Raft membership and tombstones its ID so it
// can never re-admit itself. The node must already be drained and empty — its
// shards migrated off — or removal is refused, so durability is never traded.
// addr is the cluster control address to dial; empty uses this node's own.
func Remove(dataDir, addr, nodeID string) error {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return err
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return err
	}
	target := addr
	if target == "" {
		target = cfg.ClusterAddr
	}
	return leaderRedirect("remove", target, 0, func(addr string) (controlOutcome, error) {
		resp, err := requestRemove(addr, cert, pool, removeRequest{NodeID: nodeID})
		if err != nil {
			return controlOutcome{}, err
		}
		return controlOutcome{errStr: resp.Error, leader: resp.Leader}, nil
	})
}

// OptimizeReport summarizes an optimize sweep for the CLI.
type OptimizeReport struct {
	Objects   uint64
	ReEncoded uint64
}

// Optimize re-encodes existing data up to the active-count storage profile
// (ADR-0004, ADR-0031): a leader-only sweep that spreads objects written when the
// cluster was smaller across the nodes added since, widening their erasure
// coding. An empty addr asks the local node; a non-leader answer carries the
// leader's address, which Optimize follows once. The sweep can run a while; the
// call blocks until it completes.
//
// A fresh join takes a moment to reconcile into the layout, and optimizing before
// it lands would target the old, smaller node count. The leader reports that as a
// retryable refusal rather than a misleading no-op, and Optimize waits it out (up
// to optimizeSettleWait) so one invocation does the right thing after growth —
// no guessing at a sleep.
func Optimize(dataDir, addr string) (OptimizeReport, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return OptimizeReport{}, err
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return OptimizeReport{}, err
	}
	target := addr
	if target == "" {
		target = cfg.ClusterAddr
	}
	var report OptimizeReport
	err = leaderRedirect("optimize", target, optimizeSettleWait, func(addr string) (controlOutcome, error) {
		resp, err := requestOptimize(addr, cert, pool)
		if err != nil {
			return controlOutcome{}, err
		}
		report = OptimizeReport{Objects: resp.Objects, ReEncoded: resp.ReEncoded}
		return controlOutcome{errStr: resp.Error, leader: resp.Leader, retry: resp.Retry}, nil
	})
	if err != nil {
		return OptimizeReport{}, err
	}
	return report, nil
}

// Encrypt turns on the cluster's encryption-at-rest posture (ADR-0021): a
// leader-only proposal over the control port, authenticated by this node's own
// certificate (like Status and Optimize). New writes encrypt from then on;
// existing objects are unchanged and stay readable. It is enable-only — there
// is no disable. The key itself is never sent: every node must already hold it
// (-master-key-file). Returns the posture in effect after the call.
func Encrypt(dataDir, addr string) (string, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return "", err
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return "", err
	}
	target := addr
	if target == "" {
		target = cfg.ClusterAddr
	}
	var label string
	err = leaderRedirect("encrypt", target, 0, func(addr string) (controlOutcome, error) {
		resp, err := requestEncrypt(addr, cert, pool)
		if err != nil {
			return controlOutcome{}, err
		}
		label = resp.Encryption
		return controlOutcome{errStr: resp.Error, leader: resp.Leader}, nil
	})
	if err != nil {
		return "", err
	}
	return label, nil
}

// RotateKeyReport is what `cluster rotate-key` reports (ADR-0032).
type RotateKeyReport struct {
	Rewrapped uint64 // versions moved onto the new key by this rotation
	Remaining uint64 // versions still on the old key (zero on success)
	Completed bool   // the rotation closed — the old key may be retired
}

// RotateKey rotates the cluster's master key (ADR-0032): a leader-only rewrap
// sweep over the control port, authenticated by this node's own certificate
// (like Encrypt). Every encrypted version's DEK is rewrapped from the old key to
// the new one — metadata only, the object bytes never move. The new key is never
// sent over the wire: every node must already hold it (-new-master-key-file).
// On success the rotation is closed and the old key may be retired.
func RotateKey(dataDir, addr string) (RotateKeyReport, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return RotateKeyReport{}, err
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return RotateKeyReport{}, err
	}
	target := addr
	if target == "" {
		target = cfg.ClusterAddr
	}
	var report RotateKeyReport
	err = leaderRedirect("rotate-key", target, 0, func(addr string) (controlOutcome, error) {
		resp, err := requestRotateKey(addr, cert, pool)
		if err != nil {
			return controlOutcome{}, err
		}
		report = RotateKeyReport{Rewrapped: resp.Rewrapped, Remaining: resp.Remaining, Completed: resp.Completed}
		return controlOutcome{errStr: resp.Error, leader: resp.Leader}, nil
	})
	if err != nil {
		return RotateKeyReport{}, err
	}
	return report, nil
}

// RotateCAReport is what `cluster rotate-ca` reports (ADR-0033).
type RotateCAReport struct {
	Reissued  uint64 // members moved onto the new CA
	Completed bool   // the old CA was dropped — it may be retired
}

// RotateCA rotates the cluster's CA (ADR-0033): a leader-driven dual-trust
// rollover over the control port, authenticated by this node's own certificate.
// The leader mints a new CA, widens the replicated trust bundle to it, reissues
// every member's node certificate onto it, then drops the old CA. The new CA key
// never leaves the leader; each member's new leaf rides the existing mTLS
// channel, like a join. On success the old CA may be retired.
func RotateCA(dataDir, addr string) (RotateCAReport, error) {
	dir := Dir(dataDir)
	cfg, err := loadConfig(dir)
	if err != nil {
		return RotateCAReport{}, err
	}
	cert, pool, _, err := loadNodeTLS(dir)
	if err != nil {
		return RotateCAReport{}, err
	}
	target := addr
	if target == "" {
		target = cfg.ClusterAddr
	}
	var report RotateCAReport
	err = leaderRedirect("rotate-ca", target, 0, func(addr string) (controlOutcome, error) {
		buf, err := controlRoundTrip(addr, cert, pool, encodeRequest(reqRotateCA, nil))
		if err != nil {
			return controlOutcome{}, err
		}
		resp, err := decodeRotateCAResponse(buf)
		if err != nil {
			return controlOutcome{}, err
		}
		report = RotateCAReport{Reissued: resp.Reissued, Completed: resp.Completed}
		return controlOutcome{errStr: resp.Error, leader: resp.Leader}, nil
	})
	if err != nil {
		return RotateCAReport{}, err
	}
	return report, nil
}

// optimizeSettleWait bounds how long Optimize waits for a recent membership
// change to reconcile before giving up — generous, since a join's transition
// migrates data before the layout settles.
const optimizeSettleWait = 5 * time.Minute

// controlRetries / controlRetryWait bound the redial of a control request after
// a transient connection or TLS error. The cluster control port runs over real
// loopback mTLS — plumbing the simulator does not model — and under heavy load a
// freshly dialed connection can drop or desync mid-record (surfacing as a "bad
// record MAC"). A short, idempotent control request is safe to redial. An
// application-level refusal is not a transient error: it rides the decoded
// response and never reaches here, so it is never retried.
const (
	controlRetries   = 4
	controlRetryWait = 200 * time.Millisecond
)

// controlExchange dials addr with this node's cluster certificate, sends one
// framed control request, and returns the framed response — a single attempt.
// The peer is authenticated as a holder of a cluster-CA certificate, not by name
// (a request may be asked of any member, whose node ID the caller need not know).
func controlExchange(addr string, cert tls.Certificate, pool *x509.CertPool, req []byte) ([]byte, error) {
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{cert},
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: verifyChainToPool(pool),
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: dialing %s: %w", addr, err)
	}
	defer conn.Close()
	if err := writeFrame(conn, req); err != nil {
		return nil, fmt.Errorf("cluster: sending request to %s: %w", addr, err)
	}
	buf, err := readFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("cluster: reading response from %s: %w", addr, err)
	}
	return buf, nil
}

// controlRoundTrip is controlExchange with a bounded redial on a transient
// connection or TLS error. Only for idempotent requests (status, drain/undrain,
// remove): a retry that races a server-side commit is harmless for those. Join
// is excluded — its token is single-use, so a redial after a consumed token is
// not the same request.
func controlRoundTrip(addr string, cert tls.Certificate, pool *x509.CertPool, req []byte) ([]byte, error) {
	var buf []byte
	var err error
	for attempt := 0; attempt < controlRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(controlRetryWait)
		}
		if buf, err = controlExchange(addr, cert, pool, req); err == nil {
			return buf, nil
		}
	}
	return nil, err
}

// controlSettlePoll is how long leaderRedirect waits between re-asks while the
// cluster converges a membership change (the server answered "retry").
const controlSettlePoll = 2 * time.Second

// controlOutcome carries the redirect/refusal fields every leader-only control
// response shares, extracted so leaderRedirect drives the retry loop uniformly.
type controlOutcome struct {
	errStr string // application-level refusal; "" means success
	leader string // the leader's dial address, when this node is not it
	retry  bool   // the cluster is converging — re-ask after a wait
}

// leaderRedirect runs a leader-only control request against target, following a
// single redirect to the named leader and, when the server reports the cluster is
// still converging, waiting out the change (up to settleWait) before re-asking.
// op names the operation for the refusal message; do performs one round trip
// against an address. Drain, remove, and optimize all share this loop — they
// differ only in the request do makes and whether the server ever sets retry
// (settleWait==0 disables the wait).
func leaderRedirect(op, target string, settleWait time.Duration, do func(addr string) (controlOutcome, error)) error {
	deadline := time.Now().Add(settleWait)
	redirected := false
	for {
		out, err := do(target)
		if err != nil {
			return err
		}
		if out.errStr == "" {
			return nil
		}
		// A non-leader answer carries the leader's address; follow it once.
		if out.leader != "" && out.leader != target && !redirected {
			target, redirected = out.leader, true
			continue
		}
		// The cluster is still absorbing a membership change — wait and re-ask.
		if out.retry && time.Now().Before(deadline) {
			time.Sleep(controlSettlePoll)
			continue
		}
		if out.leader != "" {
			return fmt.Errorf("cluster: %s refused: %s (leader is %s)", op, out.errStr, out.leader)
		}
		return fmt.Errorf("cluster: %s refused: %s", op, out.errStr)
	}
}

// requestOptimize runs one optimize request — a single attempt, not
// controlRoundTrip's bounded redial: the sweep can run minutes and the server
// holds the connection open until it finishes (controlExchange sets no read
// deadline), so a redial would only start a second sweep, not recover a lost
// answer.
func requestOptimize(addr string, cert tls.Certificate, pool *x509.CertPool) (optimizeResponse, error) {
	buf, err := controlExchange(addr, cert, pool, encodeRequest(reqOptimize, nil))
	if err != nil {
		return optimizeResponse{}, err
	}
	return decodeOptimizeResponse(buf)
}

func requestEncrypt(addr string, cert tls.Certificate, pool *x509.CertPool) (encryptResponse, error) {
	buf, err := controlRoundTrip(addr, cert, pool, encodeRequest(reqEncrypt, nil))
	if err != nil {
		return encryptResponse{}, err
	}
	return decodeEncryptResponse(buf)
}

func requestRotateKey(addr string, cert tls.Certificate, pool *x509.CertPool) (rotateKeyResponse, error) {
	buf, err := controlRoundTrip(addr, cert, pool, encodeRequest(reqRotateKey, nil))
	if err != nil {
		return rotateKeyResponse{}, err
	}
	return decodeRotateKeyResponse(buf)
}

func requestRemove(addr string, cert tls.Certificate, pool *x509.CertPool, req removeRequest) (removeResponse, error) {
	buf, err := controlRoundTrip(addr, cert, pool, encodeRequest(reqRemove, encodeRemoveRequest(req)))
	if err != nil {
		return removeResponse{}, err
	}
	return decodeRemoveResponse(buf)
}

func requestDrain(addr string, cert tls.Certificate, pool *x509.CertPool, req drainRequest) (drainResponse, error) {
	buf, err := controlRoundTrip(addr, cert, pool, encodeRequest(reqDrain, encodeDrainRequest(req)))
	if err != nil {
		return drainResponse{}, err
	}
	return decodeDrainResponse(buf)
}

// dialPinned dials a join listener trusting only the token's pinned CA:
// the server must present a chain containing the certificate with that
// hash, and its leaf must verify against it. kubeadm-style bootstrap — the
// joiner has no trust store yet, the token is the trust.
func dialPinned(addr string, caHash [32]byte) (*tls.Conn, error) {
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // verification happens below, against the pin
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			pool := x509.NewCertPool()
			for _, der := range rawCerts {
				if sha256.Sum256(der) == caHash {
					ca, err := x509.ParseCertificate(der)
					if err != nil {
						return fmt.Errorf("parsing pinned CA: %w", err)
					}
					pool.AddCert(ca)
				}
			}
			if len(rawCerts) == 0 {
				return errors.New("no certificate presented")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("parsing server certificate: %w", err)
			}
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
				return fmt.Errorf("server is not a member of the token's cluster: %w", err)
			}
			return nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: dialing join address %s: %w", addr, err)
	}
	return conn, nil
}

// verifyChainToPool authenticates a peer by chain membership in a pool,
// without a name check.
func verifyChainToPool(pool *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no certificate presented")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		_, err = leaf.Verify(x509.VerifyOptions{Roots: pool})
		return err
	}
}

// loadCA loads the CA with its key — issuance nodes only.
func loadCA(dir string) (*certs.CA, error) {
	certPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		return nil, err
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if err != nil {
		return nil, err
	}
	return certs.LoadCA(certPEM, keyPEM)
}

// loadNodeTLS loads a node's certificate (with the CA appended to its
// chain, so join clients can match their pin) and the CA trust pool.
func loadNodeTLS(dir string) (tls.Certificate, *x509.CertPool, []byte, error) {
	caPEM, err := os.ReadFile(filepath.Join(dir, "ca.pem"))
	if err != nil {
		return tls.Certificate{}, nil, nil, fmt.Errorf("cluster: reading ca.pem: %w", err)
	}
	pool, err := certs.PoolFromPEM(caPEM)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "node.pem"), filepath.Join(dir, "node.key"))
	if err != nil {
		return tls.Certificate{}, nil, nil, fmt.Errorf("cluster: loading node certificate: %w", err)
	}
	caCert, err := certs.LoadCertDER(caPEM)
	if err != nil {
		return tls.Certificate{}, nil, nil, err
	}
	cert.Certificate = append(cert.Certificate, caCert)
	return cert, pool, caPEM, nil
}
