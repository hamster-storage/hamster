package cluster

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/hamster-storage/hamster/internal/certs"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/raftnode"
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
	// Persist the bundle's CA certificates to ca.pem so a restart boots trusting
	// the current set (ADR-0033) — during a rotation that is the old and new CA,
	// after it the new CA alone. Written only on a generation change, so it is
	// rare. The write is atomic (temp + rename): a concurrent reader — a join
	// minting a token, a restart loading TLS — never sees a torn file. Best-
	// effort: a failed write only risks a restart booting on stale on-disk trust,
	// which a fresh bundle refresh corrects once it rejoins.
	if err := atomicWriteFile(filepath.Join(n.dir, "ca.pem"), bytes.Join(bundle.CertPEMs(), nil)); err != nil {
		log.Printf("cluster: persisting trust bundle to ca.pem: %v", err)
	}
}

// atomicWriteFile writes data to path via a temp file and a rename, so a
// concurrent reader sees either the old file or the new one, never a partial
// write. The rename is atomic on the same filesystem (POSIX).
func atomicWriteFile(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// applyReissue adopts a new node certificate signed by the bundle's issuing CA
// during a CA rotation (ADR-0033): the leaf the rotation driver pushed. It is
// validated before adoption — the certificate must be for this node's identity
// and chain to the issuing CA — so only the holder of the new CA key (the
// driver) can rotate this node's leaf. On success the new leaf goes live (the
// transport reads it per handshake), is persisted (node.pem/node.key) for
// restarts, and this node's leaf-CA fingerprint is recorded so the rotation can
// see it converge. Loop-owned.
func (n *Node) applyReissue(certPEM, keyPEM []byte) error {
	bundle, ok := n.raft.Store().TrustBundle()
	if !ok {
		return fmt.Errorf("no trust bundle: cannot validate a reissued certificate")
	}
	var issuerPEM []byte
	for _, c := range bundle.CAs {
		if c.Fingerprint == bundle.IssuerFingerprint {
			issuerPEM = c.CertPEM
		}
	}
	if issuerPEM == nil {
		return fmt.Errorf("trust bundle has no issuing CA certificate")
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM) // also checks the key matches the cert
	if err != nil {
		return fmt.Errorf("reissued certificate/key invalid: %w", err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("parsing reissued certificate: %w", err)
	}
	if leaf.Subject.CommonName != n.cfg.NodeID {
		return fmt.Errorf("reissued certificate is for %q, not this node %q", leaf.Subject.CommonName, n.cfg.NodeID)
	}
	pool, err := certs.PoolFromCAs([][]byte{issuerPEM})
	if err != nil {
		return err
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}); err != nil {
		return fmt.Errorf("reissued certificate does not chain to the issuing CA: %w", err)
	}
	// Present the issuing CA in the chain too, as the boot path does.
	if der, err := certs.LoadCertDER(issuerPEM); err == nil {
		cert.Certificate = append(cert.Certificate, der)
	}
	for name, data := range map[string][]byte{"node.pem": certPEM, "node.key": keyPEM} {
		if err := atomicWriteFile(filepath.Join(n.dir, name), data); err != nil {
			return fmt.Errorf("persisting reissued %s: %w", name, err)
		}
	}
	n.leaf.Store(&cert)
	// The leaf-CA fingerprint in the replicated registry is committed by the
	// rotation driver (the leader), not here: a follower cannot propose (no
	// proposal forwarding yet, ADR-0027), and the driver knows the reissue
	// succeeded.
	return nil
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

// caPropagationWait / caConvergeWait bound the cross-node waits in a rotation:
// for every member to pick up a new trust-bundle generation, and for the
// registry to show every member reissued.
const (
	caPropagationWait = 60 * time.Second
	caConvergeWait    = 60 * time.Second
)

// proposeAsLeader proposes p and waits for it to commit, tolerating a transient
// loss of leadership: a CA rotation runs for several seconds, and a brief
// election blip (common under load) should not abort it. It retries on
// ErrNotLeader for up to leaderSettle, so a blip that returns leadership here is
// absorbed; a leadership move that sticks elsewhere surfaces as a clean failure
// the operator re-runs. Loop-driven proposals, so it runs off the loop.
func (n *Node) proposeAsLeader(p any) error {
	deadline := time.Now().Add(leaderSettle)
	for {
		e, ok := onLoopAsync(n, 30*time.Second, func(done func(error)) {
			if lead, _ := n.raft.Leader(); lead != n.cfg.RaftID {
				done(raftnode.ErrNotLeader)
				return
			}
			n.raft.Propose(p, func(_ any, err error) { done(err) })
		})
		err := e
		if !ok {
			err = errors.New("proposal timed out")
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, raftnode.ErrNotLeader) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(200 * time.Millisecond) // wait for leadership to settle, then retry
	}
}

// leaderSettle bounds how long a rotation waits out a leadership blip before
// giving up (and asking the operator to re-run).
const leaderSettle = 30 * time.Second

// runRotateCA drives a CA rotation to completion (ADR-0033): mint a new CA,
// widen trust to it (dual-trust bundle), reissue every member onto it, then drop
// the old CA. Leader-only (it installs bundle generations and reaches every
// member). The new CA key lives only on this node and never travels the wire;
// each member's new leaf is pushed over the established mTLS channel, the same
// as a join. On success the old CA is retired and this node is the issuer for
// future joins.
func (n *Node) runRotateCA() (reissued uint64, err error) {
	// Phase 1: mint the new CA and install the dual-trust bundle generation.
	var newCA *certs.CA
	var newFP, targetVer uint64
	var dualCAs []meta.TrustedCA
	prep, _ := onLoop(n, 10*time.Second, func() error {
		if lead, _ := n.raft.Leader(); lead != n.cfg.RaftID {
			return raftnode.ErrNotLeader
		}
		cur, ok := n.raft.Store().TrustBundle()
		if !ok {
			return errors.New("no trust bundle yet; the cluster is still forming")
		}
		ca, e := certs.NewCA(n.cfg.Cluster, n.clock.Now())
		if e != nil {
			return e
		}
		newCA, newFP, targetVer = ca, ca.Fingerprint(), cur.Version+1
		// Trust everything currently trusted plus the fresh CA. In the normal
		// case that is {old, new}; if a prior rotation was interrupted (the bundle
		// already holds more than one CA), this resumes by adding a fresh CA and,
		// in phase 5, dropping all the others — so a re-run always converges to a
		// single CA, the one whose key this driver holds (ADR-0033).
		dualCAs = append(append([]meta.TrustedCA(nil), cur.CAs...), meta.TrustedCA{Fingerprint: newFP, CertPEM: ca.CertPEM()})
		return nil
	})
	if prep != nil {
		return 0, prep
	}
	if err := n.proposeAsLeader(meta.SetTrustBundle{
		ProposedAtUnixMS: n.clock.Now().UnixMilli(), Version: targetVer, CAs: dualCAs, IssuerFingerprint: newFP,
	}); err != nil {
		return 0, err
	}

	// Phase 2: wait for every member to trust the new CA before reissuing any
	// leaf, so a reissued leaf is never presented to a peer that would reject it.
	members := n.members()
	if err := n.waitMembersTrust(members, targetVer); err != nil {
		return 0, err
	}

	// Phase 3: reissue every member onto the new CA — the leader itself locally,
	// the rest by pushing a freshly signed leaf over the control channel.
	for _, m := range members {
		leaf, e := newCA.Issue(m.NodeID, n.clock.Now())
		if e != nil {
			return reissued, fmt.Errorf("issuing for %s: %w", m.NodeID, e)
		}
		certPEM, keyPEM, e := certs.CertPEMs(leaf)
		if e != nil {
			return reissued, fmt.Errorf("encoding for %s: %w", m.NodeID, e)
		}
		if m.NodeID == n.cfg.NodeID {
			if e, _ := onLoop(n, 10*time.Second, func() error { return n.applyReissue(certPEM, keyPEM) }); e != nil {
				return reissued, fmt.Errorf("reissuing self: %w", e)
			}
		} else {
			buf, e := controlRoundTrip(m.Dial, *n.leaf.Load(), n.trust.Load(),
				encodeRequest(reqReissue, encodeReissueRequest(reissueRequest{CertPEM: certPEM, KeyPEM: keyPEM})))
			if e != nil {
				return reissued, fmt.Errorf("reissuing %s: %w", m.NodeID, e)
			}
			rr, e := decodeReissueResponse(buf)
			if e != nil {
				return reissued, fmt.Errorf("reissuing %s: %w", m.NodeID, e)
			}
			if rr.Error != "" {
				return reissued, fmt.Errorf("reissuing %s: %s", m.NodeID, rr.Error)
			}
		}
		// Record the member's new leaf CA in the replicated registry (the driver
		// is the leader, so it proposes; ADR-0033). This is what waitCAConverged
		// counts down. proposeAsLeader rides out a transient election blip.
		if err := n.proposeAsLeader(meta.SetNodeLeafCA{
			ProposedAtUnixMS: n.clock.Now().UnixMilli(), NodeID: m.NodeID, LeafCAFingerprint: newFP,
		}); err != nil {
			return reissued, fmt.Errorf("recording %s's new CA: %w", m.NodeID, err)
		}
		reissued++
	}

	// Phase 4: wait for the registry to show every member on the new CA.
	if err := n.waitCAConverged(newFP); err != nil {
		return reissued, err
	}

	// Phase 5: drop the old CA — trust narrows to the new one alone. Safe now
	// that no member presents an old-CA leaf.
	if err := n.proposeAsLeader(meta.SetTrustBundle{
		ProposedAtUnixMS: n.clock.Now().UnixMilli(), Version: targetVer + 1,
		CAs: []meta.TrustedCA{{Fingerprint: newFP, CertPEM: newCA.CertPEM()}}, IssuerFingerprint: newFP,
	}); err != nil {
		return reissued, err
	}

	// Phase 6: become the issuer under the new CA, so future joins are signed by
	// the now-current CA. The new key lands on this node's disk only.
	onLoop(n, 10*time.Second, func() struct{} {
		if keyPEM, e := newCA.KeyPEM(); e == nil {
			if e := atomicWriteFile(filepath.Join(n.dir, "ca.key"), keyPEM); e != nil {
				log.Printf("cluster: persisting new CA key: %v", e)
			} else {
				n.ca = newCA
			}
		}
		return struct{}{}
	})
	return reissued, nil
}

// waitMembersTrust polls every member's status until it is on at least trust
// bundle generation ver — proof it trusts the CA just added (ADR-0033).
func (n *Node) waitMembersTrust(members []Member, ver uint64) error {
	deadline := time.Now().Add(caPropagationWait)
	for _, m := range members {
		for {
			if m.NodeID == n.cfg.NodeID {
				if got, _ := onLoop(n, 5*time.Second, func() uint64 { return n.trustVersion }); got >= ver {
					break
				}
			} else if st, err := n.memberStatus(m.Dial); err == nil && st.TrustVersion >= ver {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("cluster: member %s did not pick up the new CA in time", m.NodeID)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	return nil
}

// waitCAConverged polls this node's replicated registry until no member's leaf
// remains on the old CA — the rotation's provable convergence (ADR-0033).
func (n *Node) waitCAConverged(issuer uint64) error {
	deadline := time.Now().Add(caConvergeWait)
	for {
		if got, _ := onLoop(n, 5*time.Second, func() uint64 { return n.caStragglerCount(issuer) }); got == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("cluster: CA rotation did not converge (members still on the old CA)")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// memberStatus fetches a member's status over the control channel using this
// node's live certificate and trust pool (ADR-0033) — used during a rotation,
// when on-disk material may be mid-swap.
func (n *Node) memberStatus(addr string) (statusResponse, error) {
	buf, err := controlRoundTrip(addr, *n.leaf.Load(), n.trust.Load(), encodeRequest(reqStatus, nil))
	if err != nil {
		return statusResponse{}, err
	}
	return decodeStatusResponse(buf)
}
