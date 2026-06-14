# ADR-0029: CA custody and issuance — self-managed default, external PKI supported, key never in Raft

## Status

Accepted

## Context

[ADR-0022](0022-cluster-mtls.md) established that all inter-node traffic runs
over mutual TLS, with a cluster CA minted by `cluster init` and held on the first
node, which issues node certificates as members join. It deliberately left two
questions open, and this ADR settles them:

1. **What happens to issuance when the init node — the sole holder of the CA
   private key by default — is lost?** Cluster growth and certificate renewal
   depend on that one node.
2. **Should the CA private key be made highly available by replicating it through
   Raft, so any leader can issue?**

A third point needs stating plainly because ADR-0022 under-sold it: many of the
operators Hamster targets — small enterprises and startups self-hosting an S3
service — **already run a PKI** (HashiCorp Vault, a corporate CA, an offline or
HSM root). For them, "Hamster mints and hides its own CA" is not the feature; the
feature is being able to anchor trust where their auditors already look.

The governing constraint from [ADR-0002](0002-single-binary-no-external-dependencies.md)
is **no _required_ external services** — not "zero configuration" and not "no
external services permitted." Hamster ships sensible defaults (the auto storage
profile ladder is the model: a user need not understand Reed–Solomon to get a
durable cluster) and does not force an advanced operator into them. CA custody is
exactly this kind of decision: a default that just works, and an override for
those who want it.

## Decision

**1. Self-managed cluster CA is the default.** `cluster init` mints the CA
automatically (Ed25519, as ADR-0022) with no PKI step asked of the operator. This
serves the user who wants a working, encrypted, authenticated cluster without
learning X.509 — the no-platform-team case.

**2. External / operator-supplied PKI is a first-class, supported path — not a
fallback.** An operator may bring their own CA material or delegate issuance to an
external PKI (Vault, an offline/HSM root, a corporate CA). Issuance goes behind a
pluggable issuer interface so the source of signed certificates is a
configuration choice. This does not break the single-binary promise: the promise
is no _required_ external services, and the self-managed default still requires
none — opting into an external PKI is the advanced operator's prerogative, and the
natural fit for compliance-shaped users who already centralize trust.

**3. The CA private key is never replicated through the Raft log** — nor through
any cluster-replicated store, snapshot, or BadgerDB. Issuance HA is achieved by
custody (external PKI, or operator backup of the self-managed key), never by
spreading the signing key across the serving nodes.

**4. Loss of the issuing node degrades issuance, not the cluster.** Certificate
*validation* needs only the CA *certificate*, which is already distributed to
every node; existing members keep talking and keep serving data. Only *new joins
and renewals* need the signing key. With the self-managed default, the operator
backs the key up out of band; with external PKI, there is no single-node custody
to lose in the first place.

## Consequences

- The default path is unchanged for the casual operator: init mints, join hands
  over a token, done — zero PKI knowledge required.
- An operator who runs Vault (or any PKI) can make issuance highly available and
  audited by pointing Hamster at it; the signing key never touches a Hamster disk.
- With the self-managed CA, backing up the key is an operational obligation:
  losing it without a backup or an external PKI is a **cluster-growth-ending
  event, not a data-losing one** — existing nodes serve indefinitely, but no new
  member can join and no certificate can be renewed. Everything committed through
  Raft (the stored cluster layout, its failure-domain labels per
  [ADR-0016](0016-failure-domain-hierarchy.md), all object metadata) survives, so
  durability and placement are untouched; only issuance is lost. This ties to the
  [ADR-0025](0025-force-new-cluster-recovery.md) recovery story.
- Renewal can be narrowed below issuance: renewing an already-admitted node's
  certificate (prove possession of the current key, receive a new cert for the
  *same* identity) need not carry the full power to mint *new* identities. Joins
  stay rare and root-gated; renewals become self-service. Designed alongside the
  CA rotation still owed before v1 (ADR-0022).
- CA rotation, owed before v1, must work for both the self-managed and the
  external-PKI cases.

## Alternatives considered

- **Replicating the CA private key through Raft so any leader can issue.** This
  trades a bounded availability problem for an unbounded confidentiality one. The
  root signing key would land on every voter's disk and in every snapshot —
  including the streamed install a brand-new learner receives the moment it joins,
  the least-vetted member at the worst possible time — so a single node compromise
  or one leaked snapshot would expose the power to mint *any* identity in the
  cluster, permanently and irreversibly, rather than exposing only that node's own
  certificate, shards, and metadata. The trust root must have the *smallest*
  footprint in the system; replication gives it the largest. At-rest encryption
  does not save it, because the decryption key lives on those same nodes. Rejected
  — the root key is the one secret the replicated log must never hold.
- **Requiring an external PKI for everyone.** Would break the no-platform-team
  promise and the no-_required_-external-services constraint of ADR-0002 for the
  casual self-hoster. Rejected as a *requirement*; adopted as a first-class
  *option* (decision 2).
- **Threshold-signing the CA** (quorum issuance, no single holder) and
  **replicating the key only wrapped under an operator KEK held outside the
  cluster.** Both keep the root off any single serving node, but are materially
  heavier than custody-plus-external-PKI and buy little the chosen directions do
  not. Named for completeness, not chosen for v0.
