package cluster

import (
	"fmt"
	"time"

	"github.com/hamster-storage/hamster/internal/certs"
)

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
	if req.Replaces == req.NodeID {
		return refuse("a node cannot replace itself")
	}
	tok, err := decodeToken(req.Token)
	if err != nil {
		return refuse("%v", err)
	}
	if err := consumeToken(n.dir, tok.ID, tok.Secret, time.Now()); err != nil {
		return refuse("%v", err)
	}

	// Snapshot membership before taking issueMu: members() round-trips the loop,
	// and reconcileLayout (on the loop) also takes issueMu — holding it across
	// this call would deadlock the loop until members() times out, handing the
	// joiner an empty member list and stranding it with no peers to reach.
	members := n.members()
	if req.Replaces != "" {
		// Validate the replacement target before allocating an identity, so a
		// typo'd or stale name fails fast rather than stranding a wasted ID.
		found := false
		for _, m := range members {
			if m.NodeID == req.Replaces {
				found = true
				break
			}
		}
		if !found {
			return refuse("cannot replace %q: not a cluster member", req.Replaces)
		}
	}
	outcome := n.issueIdentity(req, members)
	if outcome.Error != "" {
		return outcome
	}

	// A replacing join (ADR-0004): now that the identity is issued and issueMu is
	// released (proposeReplace round-trips the loop, which also takes issueMu),
	// pair old→new. reconcile then swaps the new node in for the old at constant
	// size once it joins, so the storage profile never changes. Leader-only and
	// mutually exclusive with other layout ops, like every layout change.
	if req.Replaces != "" {
		if err := n.proposeReplace(req.Replaces, req.NodeID); err != nil {
			return refuse("declaring replacement of %s: %v", req.Replaces, err)
		}
	}
	return outcome
}

// issueIdentity allocates the joining node's Raft ID and certificate under
// issueMu (serializing concurrent joins), recording its failure-domain labels in
// the durable registry the layout reconcile reads (ADR-0016). Allocates the ID
// durably before handing it out — a crash between must waste an ID, never reuse
// one.
func (n *Node) issueIdentity(req joinRequest, members []Member) joinOutcome {
	n.issueMu.Lock()
	defer n.issueMu.Unlock()
	for _, m := range members {
		if m.NodeID == req.NodeID {
			return refuse("node ID %q is already a cluster member", req.NodeID)
		}
	}
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
