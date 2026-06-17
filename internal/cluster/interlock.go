package cluster

import (
	"crypto/tls"
	"fmt"
	"time"
)

// The health interlock (ADR-0034). `cluster can-stop <node>` answers whether
// taking a node down for maintenance or upgrade is safe — the rolling-upgrade
// discipline made checkable: proceed only from full health, one node at a time.
// It is **advisory**: it informs the operator's (or, in v0.10, the automated
// roll's) decision; it never refuses SIGTERM. A node must always be stoppable in
// a genuine emergency.
//
// Three conditions, all required:
//
//  1. Quorum survives. If the target is a voter, the remaining voters must still
//     form a Raft majority without it ([ADR-0017]). A 3-voter cluster can give up
//     one; a 2-voter cluster cannot (the standard "two voters tolerate zero
//     failures"). A learner is free to stop — it is not counted toward quorum.
//  2. The cluster is not already degraded. No *other* node may be down: stopping
//     one healthy node from an otherwise-healthy cluster never drops an object
//     below its read floor (shards are node-distinct and m >= 1), but stopping a
//     second concurrent node might. This is the "one node at a time" rule.
//  3. No layout transition is open ([ADR-0004]). A node leaving mid-migration —
//     while shards are moving between placements — is unsafe; wait for the
//     transition to converge first.

// handleCanStop serves a can-stop request: the interlock verdict for a target
// node, authenticated by a cluster certificate like status. Answered from this
// node's view (committed membership and layout, plus its local liveness view) —
// advisory and read-only, so any member can answer; no leader redirect.
func (n *Node) handleCanStop(conn *tls.Conn, payload []byte) canStopResponse {
	if len(conn.ConnectionState().PeerCertificates) == 0 {
		return canStopResponse{Error: "can-stop requires a cluster certificate"}
	}
	req, err := decodeCanStopRequest(payload)
	if err != nil {
		return canStopResponse{Error: "malformed can-stop request"}
	}
	safe, reason := n.canStop(req.NodeID)
	return canStopResponse{Safe: safe, Reason: reason}
}

// canStop evaluates the interlock for nodeID (ADR-0034), returning the verdict
// and a human reason either way. Reads the committed membership and layout plus
// this node's local liveness view.
func (n *Node) canStop(nodeID string) (safe bool, reason string) {
	members := n.members()
	var target *Member
	for i := range members {
		if members[i].NodeID == nodeID {
			target = &members[i]
		}
	}
	if target == nil {
		return false, fmt.Sprintf("node %q is not a cluster member", nodeID)
	}

	// (2) Proceed only from full health: no other node already down.
	for _, m := range members {
		if m.NodeID != nodeID && m.Down {
			return false, fmt.Sprintf("another node (%s) is currently down — stop one node at a time, only from full health", m.NodeID)
		}
	}

	// (3) No layout transition open (a node leaving mid-migration is unsafe).
	if n.transitionOpen() {
		return false, "a layout transition is in progress — wait for it to converge before stopping a node"
	}

	// (1) Quorum survives if the target is a voter.
	if !target.Learner {
		voters := 0
		for _, m := range members {
			if !m.Learner {
				voters++
			}
		}
		remaining := voters - 1
		majority := voters/2 + 1
		if remaining < majority {
			return false, fmt.Sprintf("stopping this voter would lose Raft quorum: %d of %d voters would remain, need %d", remaining, voters, majority)
		}
	}

	return true, "safe to stop: Raft quorum holds, no other node is down, and no layout transition is open"
}

// transitionOpen reports whether a layout transition is in flight (ADR-0004):
// the committed layout carries a Previous placement. Read on the loop.
func (n *Node) transitionOpen() bool {
	open, _ := onLoop(n, 5*time.Second, func() bool {
		cl, ok := n.raft.Store().ClusterLayout()
		return ok && len(cl.Previous) > 0
	})
	return open
}
