# ADR-0033: CA rotation via a replicated multi-CA trust bundle

## Status

Proposed

## Context

[ADR-0022](0022-cluster-mtls.md) put all inter-node traffic on mutual TLS under a
cluster CA minted at `cluster init`, and closed by naming the debt this ADR pays:
"CA rotation is owed before v1, designed alongside KEK rotation." [ADR-0029](0029-ca-custody-and-issuance.md)
then settled CA *custody*: the self-managed CA is the default, an external/operator
PKI is a first-class option, and the CA **private key never enters the Raft log**
(nor any replicated store, snapshot, or BadgerDB) — issuance HA comes from custody,
never from spreading the signing key across serving nodes. It also narrowed
*renewal* (a new cert for an already-admitted identity, proving possession of the
current key) below *issuance* (minting a new identity), and observed that losing
the self-managed CA key is "a cluster-growth-ending event, not a data-losing one":
existing node certs keep validating, because validation needs only the CA
*certificate*, which is already in every node's trust store.

This ADR builds the rotation on those foundations. The forces:

- **No moment of broken trust.** A naïve "swap the CA" reissues nodes under a new
  root while peers still trust only the old one — every connection fails until the
  last node flips. A rotation must never have a window where a current member is
  untrusted.
- **The private key stays off Raft.** Whatever the rotation replicates, it is CA
  *certificates* (public), never keys — [ADR-0029](0029-ca-custody-and-issuance.md)'s
  hard line holds.
- **Zero downtime is the v0.10 target.** Rotation should not require a fleet
  restart; a node should pick up new trust and a new leaf certificate without
  going down.
- **Self-managed and external PKI both.** The flow must work whether the cluster
  mints its own new CA or an operator's PKI provides it.
- **Lost-key recovery is the same problem.** Restoring issuance after the old CA
  key is lost should not need a separate mechanism.

The same value [ADR-0032](0032-master-key-rotation.md) made central for the KEK —
a rotation must be **observable and provably complete** before the old trust root
is retired — applies here, and shapes the design the same way.

## Decision

**CA rotation is a dual-trust rollover over a replicated, generational multi-CA
trust bundle.** Trust the old and new CA at once, reissue every node from the new
CA, then drop the old — the rollover etcd, CockroachDB, and Vault use, with no
instant where a current member is untrusted.

1. **A replicated trust bundle, not a single CA cert.** A `meta` singleton (the
   `TrustBundle`, alongside the cluster layout and encryption posture) holds the
   *set* of trusted CA **certificates** (PEM) and a generation counter, installed
   compare-and-set like the layout ([ADR-0028](0028-stored-cluster-layout.md)).
   Every node builds its TLS `ClientCAs`/`RootCAs` pool from the bundle. Only
   public certificates are replicated; the CA private key is never in it
   ([ADR-0029](0029-ca-custody-and-issuance.md)). The on-disk `ca.pem` becomes the
   boot-time bootstrap trust (and may hold more than one CA mid-rotation); the
   replicated bundle is the live set, and a node persists it so a restart boots
   with correct trust.

2. **The transport consults the live bundle and leaf per handshake.** The `sys`
   mTLS transport selects its certificate and verifier from current state via
   stdlib `crypto/tls` callbacks (`GetConfigForClient`, `GetCertificate`), so a
   bundle change or a reissued leaf takes effect on the next handshake **without a
   node restart** — the zero-downtime path. No new dependency; the adapter stays
   logic-free below the seam ([ADR-0022](0022-cluster-mtls.md) decision 5), reading
   state the core hands it.

3. **Reissuance is the renewal path, self-service over the existing channel.** With
   both CAs trusted, every node still connects (its current leaf validates against
   the old CA, still in the bundle). Each node proves possession of its current key
   and receives a **new leaf for the same node identity** signed by the new CA —
   the narrowed renewal of [ADR-0029](0029-ca-custody-and-issuance.md), which need
   not carry the power to mint *new* identities. Joins (new identities) stay rare
   and root-gated; reissuance is a local exchange, not an operator chore.

4. **A pluggable issuer seam.** "Sign a CSR for node identity X" sits behind an
   interface with two implementations: the self-managed CA (the cluster mints the
   new CA and holds its key on the issuer node) and an external PKI (Vault, an
   offline/HSM root, a corporate CA) per [ADR-0029](0029-ca-custody-and-issuance.md).
   The rotation orchestration is issuer-agnostic; only where the new CA cert and
   the signatures come from differs.

5. **The rotation is a small leader-driven state machine, observable and provably
   complete.** Open it by installing a bundle generation that adds the new CA
   (every node now trusts old+new); drive reissuance of every member to a new-CA
   leaf; close it by installing a generation that drops the old CA. The **count of
   members still presenting an old-CA leaf is the convergence signal** — exact and
   cheap, the CA analogue of [ADR-0032](0032-master-key-rotation.md)'s straggler
   count — and the old CA is dropped only when that count is zero, so retiring the
   old root is provably safe, not hoped. One CA rotation at a time. `cluster
   rotate-ca` drives it; `cluster status` shows the rotation and the count still on
   the old CA.

6. **Planned rotation and lost-CA-key recovery are one flow.** Rotation never needs
   the *old* CA key: leaves are reissued from the *new* key, and validation needs
   only the two CA *certs*. So recovering from a lost self-managed CA key is just a
   rotation — generate a new CA, run the rollover — and the design that enables
   planned rotation is exactly the design that restores issuance after key loss.
   The only thing the lost old key costs is minting more old-CA certs and revoking
   via it, neither of which the rollover uses.

## Consequences

- The mTLS trust root becomes rotatable with no downtime and no trust gap, on both
  the self-managed and external-PKI paths — the rotation [ADR-0022](0022-cluster-mtls.md)
  owed before v1.
- Retiring the old CA is gated on observable convergence, mirroring KEK rotation:
  the old root drops only once every member presents a new-CA leaf, so a stale-but-
  current node is never locked out by a premature drop. Like `cluster remove`'s
  drained-and-empty gate, a member that is *down* during a rotation blocks closure
  (or is removed first): the rotation cannot prove it safe to drop the old CA while
  a member that still needs reissuing cannot be reached. This is operational
  ordering, not a new hazard.
- The replicated bundle carries only public certificates; [ADR-0029](0029-ca-custody-and-issuance.md)'s
  no-private-key-on-Raft invariant is preserved, and the new CA key lives wherever
  the old one did (issuer node or external PKI).
- A node restarting mid-rotation boots from its persisted bundle and leaf, so it
  resumes with correct trust; a node that missed the whole window (down past
  closure) needs a fresh reissue to rejoin — the renewal path, or, failing that, a
  re-join, the same as any node whose certificate has lapsed.
- New surface to test below the simulator ([ADR-0022](0022-cluster-mtls.md) keeps
  TLS in the `sys` adapter): the e2e suite must cover a full rollover, a reissue
  mid-rotation, a node down across the window, and the lost-key recovery path.
- Certificate **revocation** (CRL/OCSP) stays orthogonal and out of scope here:
  membership-bound peer-identity matching ([ADR-0022](0022-cluster-mtls.md) decision
  3) remains how a removed node loses access. A rotation can also be used as a blunt
  cluster-wide revocation (reissue everyone, drop the old root) when a key is feared
  compromised.

## Alternatives considered

- **Replicate the CA private key so any leader can reissue.** Rejected for exactly
  the reasons [ADR-0029](0029-ca-custody-and-issuance.md) rejected it: the root
  signing key would land on every voter's disk and in every snapshot, trading a
  bounded availability problem for an unbounded confidentiality one. The bundle
  replicates certs, never the key.
- **Single-CA "big bang" swap** (reissue all, then flip trust). There is no
  ordering of "reissue" and "flip trust" without a window where some peers are
  untrusted — flip first and old leaves fail, reissue first and new leaves are
  untrusted. The dual-trust bundle exists precisely to remove that window.
  Rejected.
- **Restart-based rollover** (write new material to disk, restart the fleet). Works,
  but forces downtime and an operator-choreographed restart for a security routine
  that should be self-service and continuous. Rejected in favor of per-handshake
  live trust; restart remains a fallback a node already handles (it boots from its
  persisted bundle).
- **A separate lost-key recovery mechanism.** Unnecessary once rotation never needs
  the old key: recovery *is* rotation. Building a second path would be more surface
  for the rarer case, and more to keep correct. Rejected — unify on the rollover.
- **CRL/OCSP for revocation instead of membership binding.** Real PKI machinery, but
  it adds a distribution and freshness problem the cluster does not need: membership
  is already the authoritative, replicated source of who is allowed, and a rotation
  covers the cluster-wide-revocation case. Out of scope, not rejected on merit.
