package cluster

import "github.com/hamster-storage/hamster/internal/meta"

// Version advertisement and the cluster's effective generation (ADR-0034).
//
// Each node owns its binary version and declared protocol generation (set at
// startup via WithVersion, never persisted — read fresh each boot). The leader
// runs a version monitor that polls every peer's *own* advertised version over
// the existing status channel and replicates it into that member's NodeRecord
// (SetNodeVersion). The cluster's effective generation is then the minimum of
// the recorded generations across live members — etcd's cluster-version model:
// it rolls forward only once the last node has upgraded, so a mixed-version
// cluster never claims a generation some member has not confirmed.
//
// The monitor is the answer to "no proposal forwarding": a follower upgraded in
// place cannot write its own record, so the leader learns the new version by
// polling and writes it. That keeps the advertisement current across an
// in-place upgrade without a re-join.

// versionMonitorEvery is how many peer-sync ticks pass between version polls.
// The poll dials every peer's status, and a roll takes seconds to minutes per
// node, so detecting a version change within a few seconds is ample.
const versionMonitorEvery = 5

// advVersion is a member's self-advertised build: its release string and
// declared protocol generation.
type advVersion struct {
	binaryVersion string
	generation    uint32
}

// effectiveGeneration is the cluster's effective protocol generation (ADR-0034):
// the minimum recorded generation across the given members (the current Raft
// membership). A member whose generation is unrecorded (zero) pins the result
// low, so the effective generation never claims a roll a member has not
// confirmed. Empty membership is zero. Pure over the member list members()
// already labels from the replicated registry.
func effectiveGeneration(members []Member) uint32 {
	if len(members) == 0 {
		return 0
	}
	eff := members[0].Generation
	for _, m := range members[1:] {
		if m.Generation < eff {
			eff = m.Generation
		}
	}
	return eff
}

// versionMonitor is the leader-only step that keeps the replicated registry's
// version fields current (ADR-0034). It runs off-loop from syncPeers: it
// snapshots membership, reads each peer's own advertised version over the status
// channel (its own runtime values for itself, no round trip), and proposes a
// SetNodeVersion for any member whose record lags. A peer that is unreachable is
// skipped — its record stays as last recorded until it answers again, which is
// exactly right: the effective generation must not advance past a node that
// cannot confirm it. Proposals are leader-gated (benign on a non-leader), so a
// leadership change mid-poll is harmless.
func (n *Node) versionMonitor() {
	if n.raft == nil || !n.isLeader() {
		return
	}
	members := n.members()
	adv := make(map[string]advVersion, len(members))
	adv[n.cfg.NodeID] = advVersion{n.binaryVersion, n.generation}
	for _, m := range members {
		if m.NodeID == n.cfg.NodeID || m.Dial == "" {
			continue
		}
		st, err := n.memberStatus(m.Dial)
		if err != nil {
			continue // unreachable — leave its record as last recorded
		}
		adv[m.NodeID] = advVersion{st.LocalBinaryVersion, st.LocalGeneration}
	}
	n.loop.Post(func() {
		store := n.raft.Store()
		for id, a := range adv {
			rec, ok := store.Node(id)
			if !ok {
				continue // not registered yet — RegisterNode lands first (reconcile)
			}
			if rec.BinaryVersion == a.binaryVersion && rec.Generation == a.generation {
				continue
			}
			n.raft.Propose(meta.SetNodeVersion{
				ProposedAtUnixMS: n.clock.Now().UnixMilli(),
				NodeID:           id,
				BinaryVersion:    a.binaryVersion,
				Generation:       a.generation,
			}, func(any, error) {}) // stale / not-leader outcomes are benign
		}
	})
}
