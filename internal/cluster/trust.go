package cluster

import (
	"log"

	"github.com/hamster-storage/hamster/internal/certs"
	"github.com/hamster-storage/hamster/internal/meta"
)

// CA trust and rotation (ADR-0033). A node's inter-node mTLS trust is the
// replicated trust bundle, not a single static CA: every node builds its trust
// pool from the bundle the leader seeds at formation, and a CA rotation widens
// it (dual trust) before reissuing leaves and narrows it after. The transport
// reads the resulting pool — and this node's current leaf — per handshake
// (internal/sys), so a rotation needs no restart.

// refreshTrust rebuilds this node's mTLS trust pool from the replicated trust
// bundle when its generation advances — the bundle's first install, or a CA
// rotation widening or narrowing trust. Loop-owned; the transport reads the
// resulting pool per handshake. Before any bundle exists the boot CA pool, set
// at construction, stands. Runs on every node so followers track trust changes,
// not only the leader that proposes them.
func (n *Node) refreshTrust() {
	if n.raft == nil {
		return
	}
	bundle, ok := n.raft.Store().TrustBundle()
	if !ok || bundle.Version == n.trustVersion {
		return
	}
	pool, err := certs.PoolFromCAs(bundle.CertPEMs())
	if err != nil {
		log.Printf("cluster: trust bundle v%d is unusable (%v); keeping current trust", bundle.Version, err)
		return
	}
	n.trust.Store(pool)
	n.trustVersion = bundle.Version
}

// seedTrustBundle installs the first trust-bundle generation from the founding
// CA (ADR-0033) once a leader exists, so every node builds trust from the
// replicated bundle rather than only its boot ca.pem — the foundation a CA
// rotation extends. Leader-only in effect (a follower's proposal is a benign
// no-op, and the compare-and-set on Version 1 makes a race idempotent);
// skipped once a bundle is committed. Every node's ca.pem is the same cluster
// CA, so whichever node seeds, the bundle is identical.
func (n *Node) seedTrustBundle() {
	if n.raft == nil || len(n.bootCAPEM) == 0 {
		return
	}
	if _, ok := n.raft.Store().TrustBundle(); ok {
		return // already seeded
	}
	fp, err := certs.CAFingerprint(n.bootCAPEM)
	if err != nil {
		return
	}
	n.raft.Propose(meta.SetTrustBundle{
		ProposedAtUnixMS:  n.clock.Now().UnixMilli(),
		Version:           1,
		CAs:               []meta.TrustedCA{{Fingerprint: fp, CertPEM: append([]byte(nil), n.bootCAPEM...)}},
		IssuerFingerprint: fp,
	}, func(_ any, err error) {
		if err == nil {
			n.loop.Post(n.refreshTrust)
		}
	})
}

// caRotationOpen reports whether a CA rotation is in flight: the trust bundle
// holds more than one CA (dual trust), the window between adding the new CA and
// dropping the old. Loop-owned.
func (n *Node) caRotationOpen() (bundle meta.TrustBundle, open bool) {
	b, ok := n.raft.Store().TrustBundle()
	if !ok {
		return meta.TrustBundle{}, false
	}
	return b, len(b.CAs) > 1
}

// caStragglerCount counts members whose current leaf is not signed by the
// bundle's issuing CA (ADR-0033) — the CA rotation's remaining work, the
// analogue of the KEK-rotation straggler count. Loop-owned.
func (n *Node) caStragglerCount(issuer uint64) uint64 {
	var c uint64
	for _, r := range n.raft.Store().Nodes() {
		if r.LeafCAFingerprint != issuer {
			c++
		}
	}
	return c
}
