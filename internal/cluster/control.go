package cluster

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/raftnode"
)

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
			resp.Encryption = n.encryptionLabel()
		}
		_ = writeFrame(conn, encodeStatusResponse(resp))
	case reqDrain:
		_ = writeFrame(conn, encodeDrainResponse(n.handleDrain(conn, payload)))
	case reqRemove:
		_ = writeFrame(conn, encodeRemoveResponse(n.handleRemove(conn, payload)))
	case reqOptimize:
		_ = writeFrame(conn, encodeOptimizeResponse(n.handleOptimize(conn)))
	case reqEncrypt:
		_ = writeFrame(conn, encodeEncryptResponse(n.handleEncrypt(conn)))
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
		return drainResponse{Error: notLeaderMsg, Leader: n.leaderDial()}
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
	err, ok := onLoopAsync(n, 30*time.Second, func(done func(error)) {
		if draining {
			if reason := n.drainBlockedReason(nodeID); reason != "" {
				done(errors.New(reason))
				return
			}
		}
		n.raft.Propose(meta.SetNodeDraining{
			ProposedAtUnixMS: n.clock.Now().UnixMilli(), NodeID: nodeID, Draining: draining,
		}, func(_ any, e error) { done(e) })
	})
	if !ok {
		return fmt.Errorf("cluster: drain proposal timed out")
	}
	return err
}

// layoutOpInProgress returns a description of an in-flight layout operation — a
// transition, a drain, or a replace — or "" if the cluster is quiescent. Only
// one at a time (ADR-0004): drain and replace both refuse while another runs, so
// the operator gets a clear error rather than a silent queue. except is a node
// ID to ignore (the one the caller is acting on). Runs on the loop. Driving
// several at once is future work; the format already supports it.
func (n *Node) layoutOpInProgress(except string) string {
	if cl, ok := n.raft.Store().ClusterLayout(); ok && len(cl.Previous) > 0 {
		return "a layout transition is already in progress"
	}
	for _, r := range n.raft.Store().Nodes() {
		if r.NodeID == except {
			continue
		}
		if r.Draining {
			return fmt.Sprintf("node %s is draining", r.NodeID)
		}
		if r.ReplacedBy != "" {
			return fmt.Sprintf("node %s is being replaced by %s", r.NodeID, r.ReplacedBy)
		}
	}
	return ""
}

// drainBlockedReason refuses a new drain while any layout operation is in
// flight — one at a time (ADR-0004).
func (n *Node) drainBlockedReason(nodeID string) string {
	return n.layoutOpInProgress(nodeID)
}

// proposeReplace pairs old→new (ADR-0004): old must be a current member, new must
// not be one yet (it joins fresh), and no other layout operation may be running.
// The pairing commits on old's record; reconcile then swaps new in for old once
// new joins. Leader-only.
func (n *Node) proposeReplace(oldNode, newNode string) error {
	if oldNode == "" || newNode == "" {
		return fmt.Errorf("both the old and new node IDs are required")
	}
	if oldNode == newNode {
		return fmt.Errorf("a node cannot replace itself")
	}
	err, ok := onLoopAsync(n, 30*time.Second, func(done func(error)) {
		if lead, _ := n.raft.Leader(); lead != n.cfg.RaftID {
			done(raftnode.ErrNotLeader)
			return
		}
		oldMember, newMember := false, false
		for _, m := range n.raft.Members() {
			switch string(m.Addr) {
			case oldNode:
				oldMember = true
			case newNode:
				newMember = true
			}
		}
		if !oldMember {
			done(fmt.Errorf("node %s is not a cluster member", oldNode))
			return
		}
		if newMember {
			done(fmt.Errorf("node %s is already a cluster member; the replacement must be a fresh node — declare the replacement before joining it", newNode))
			return
		}
		if reason := n.layoutOpInProgress(""); reason != "" {
			done(fmt.Errorf("%s; only one layout change at a time", reason))
			return
		}
		n.raft.Propose(meta.SetNodeReplacedBy{
			ProposedAtUnixMS: n.clock.Now().UnixMilli(), NodeID: oldNode, ReplacedBy: newNode,
		}, func(_ any, e error) { done(e) })
	})
	if !ok {
		return fmt.Errorf("cluster: replace timed out")
	}
	return err
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
		return removeResponse{Error: notLeaderMsg, Leader: n.leaderDial()}
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
	err, ok := onLoopAsync(n, 10*time.Second, func(done func(error)) {
		if lead, _ := n.raft.Leader(); lead != n.cfg.RaftID {
			done(raftnode.ErrNotLeader)
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
			done(nil) // already removed (a retry that raced a commit)
			return
		}
		if raftID == n.cfg.RaftID {
			done(fmt.Errorf("a node cannot remove itself; issue remove from another node"))
			return
		}
		if reason := n.removeBlockedReason(nodeID); reason != "" {
			done(errors.New(reason))
			return
		}
		if err := n.raft.RemoveNode(raftID); err != nil {
			done(err)
			return
		}
		log.Printf("cluster: removing node %s (raft id %d)", nodeID, raftID)
		done(nil)
	})
	if !ok {
		return fmt.Errorf("cluster: remove timed out")
	}
	return err
}

// handleOptimize serves an optimize request (ADR-0004, ADR-0031): a leader-only
// sweep that re-encodes existing data up to the active-count storage profile, so
// objects written when the cluster was smaller spread across the nodes added
// since. Authenticated by a cluster certificate like drain; a non-leader answers
// with the leader's dial address so the client can retry there.
// encryptionLabel reads the cluster's encryption-at-rest posture (ADR-0021)
// for `cluster status`: the algorithm name, or "" when the cluster does not
// encrypt. Runs on the loop.
func (n *Node) encryptionLabel() string {
	label, _ := onLoop(n, 5*time.Second, func() string {
		if n.raft.Store().EncryptionAlgorithm() == meta.EncAES256GCM {
			return "AES256GCM"
		}
		return ""
	})
	return label
}

// handleEncrypt serves `cluster encrypt`: a leader-only proposal that turns on
// the cluster's encryption posture (ADR-0021), authenticated by a cluster
// certificate. Enable-only — there is no disable. A non-leader redirects.
func (n *Node) handleEncrypt(conn *tls.Conn) encryptResponse {
	if len(conn.ConnectionState().PeerCertificates) == 0 {
		return encryptResponse{Error: "encrypt requires a cluster certificate"}
	}
	switch label, err := n.proposeEnableEncryption(); {
	case errors.Is(err, raftnode.ErrNotLeader):
		return encryptResponse{Error: notLeaderMsg, Leader: n.leaderDial()}
	case err != nil:
		return encryptResponse{Error: err.Error()}
	default:
		return encryptResponse{Encryption: label}
	}
}

// proposeEnableEncryption commits the encryption posture (AES256GCM) through
// Raft. Leader-only. It guards against the obvious footgun: a leader with no
// KEK loaded refuses, since enabling there would make every encrypted write
// fail — the operator must provision the key on every node first.
func (n *Node) proposeEnableEncryption() (string, error) {
	var label string
	err, ok := onLoopAsync(n, 30*time.Second, func(done func(error)) {
		if lead, _ := n.raft.Leader(); lead != n.cfg.RaftID {
			done(raftnode.ErrNotLeader)
			return
		}
		if !n.masterKey.Loaded() {
			done(errors.New("this node has no master key loaded; pass -master-key-file on every node before enabling encryption"))
			return
		}
		n.raft.Propose(meta.SetEncryptionPosture{
			ProposedAtUnixMS: n.clock.Now().UnixMilli(), Algorithm: meta.EncAES256GCM,
		}, func(_ any, e error) {
			if e == nil {
				label = "AES256GCM"
			}
			done(e)
		})
	})
	if !ok {
		return "", fmt.Errorf("cluster: encryption proposal timed out")
	}
	return label, err
}

func (n *Node) handleOptimize(conn *tls.Conn) optimizeResponse {
	if len(conn.ConnectionState().PeerCertificates) == 0 {
		return optimizeResponse{Error: "optimize requires a cluster certificate"}
	}
	// An optimize sweep re-encodes every under-width object and can run far longer
	// than the default control deadline; extend it for the duration of the sweep.
	conn.SetDeadline(time.Now().Add(30 * time.Minute))
	switch rep, retry, err := n.runOptimize(); {
	case errors.Is(err, raftnode.ErrNotLeader):
		return optimizeResponse{Error: notLeaderMsg, Leader: n.leaderDial()}
	case err != nil:
		return optimizeResponse{Error: err.Error(), Retry: retry}
	default:
		resp := optimizeResponse{Objects: uint64(rep.Objects), ReEncoded: uint64(rep.ReEncoded)}
		if len(rep.Failed) > 0 {
			resp.Error = fmt.Sprintf("%d of %d objects could not be re-encoded: %s",
				len(rep.Failed), rep.Objects, strings.Join(rep.Failed, "; "))
		}
		return resp
	}
}

// runOptimize starts one optimize sweep on the loop and waits for it to finish.
// Leader-only (re-encode proposes through Raft). It refuses until the cluster is
// ready, distinguishing two cases: an operator-initiated op the caller must
// resolve (a drain or replace in flight — not retryable), versus the cluster
// still converging a membership change (a layout transition open, or a join not
// yet reconciled — retryable, the caller should wait and re-ask). The retryable
// flag is what lets `cluster optimize` wait out a fresh join instead of silently
// no-op'ing against a stale node count. Runs off the loop, so it may wait.
func (n *Node) runOptimize() (rep coord.RepairReport, retry bool, err error) {
	if n.coord == nil {
		return coord.RepairReport{}, false, errors.New("this node has no data-path coordinator")
	}
	type result struct {
		rep   coord.RepairReport
		retry bool
		err   error
	}
	// No timeout: an optimize sweep re-encodes every under-width object and may run
	// minutes; only the refusals below are synchronous.
	r, _ := onLoopAsync(n, 0, func(done func(result)) {
		if lead, _ := n.raft.Leader(); lead != n.cfg.RaftID {
			done(result{err: raftnode.ErrNotLeader})
			return
		}
		// An operator-initiated layout op (drain, replace) must be resolved first
		// — not something to wait out.
		for _, r := range n.raft.Store().Nodes() {
			if r.Draining {
				done(result{err: fmt.Errorf("node %s is draining; finish or undo it before optimizing", r.NodeID)})
				return
			}
			if r.ReplacedBy != "" {
				done(result{err: fmt.Errorf("node %s is being replaced; let that finish before optimizing", r.NodeID)})
				return
			}
		}
		// The cluster is still absorbing a membership change (a transition is open,
		// or a recent join has not reconciled into the layout). Optimizing now would
		// target a node count that is about to change — wait and re-ask.
		if !n.layoutSettled() {
			done(result{retry: true, err: errors.New("the cluster layout is still reconciling a recent membership change")})
			return
		}
		if n.sweeping {
			done(result{retry: true, err: errors.New("a repair or optimize sweep is already running")})
			return
		}
		n.sweeping = true
		n.coord.Optimize(func(rep coord.RepairReport, err error) {
			n.sweeping = false
			done(result{rep: rep, err: err})
		})
	})
	return r.rep, r.retry, r.err
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

// isLeader reports, on the loop, whether this node is the current Raft leader.
func (n *Node) isLeader() bool {
	v, _ := onLoop(n, 5*time.Second, func() bool {
		lead, _ := n.raft.Leader()
		return lead == n.cfg.RaftID
	})
	return v // a wedged loop yields false — not the leader, the safe assumption
}

// isClusterMember reports, on the loop, whether nodeID is in the current Raft
// membership.
func (n *Node) isClusterMember(nodeID string) bool {
	v, ok := onLoop(n, 5*time.Second, func() bool {
		for _, m := range n.raft.Members() {
			if string(m.Addr) == nodeID {
				return true
			}
		}
		return false
	})
	if !ok {
		return true // loop wedged; do not falsely report removed
	}
	return v
}
