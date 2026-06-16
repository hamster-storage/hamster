# Execution plan

What is being worked **now and next**, in priority order — the middle altitude
between [ROADMAP.md](ROADMAP.md) (high-level milestones per version) and the
ADRs (the reasoning behind each decision). A specific behavioral gap is tracked
by a named test, not retyped here; this file records *order and priority*, and
points at the test or ADR that holds the detail.

This is the **front line only**. Phases are pruned the moment they land — a
completed item's record survives in git history, in its now-green test, and in
the shipped ADR/doc. This file is not an archive and not a TODO graveyard: if a
line here is done, delete it.

## Now / next — v0.8 key and CA rotation

v0.7 encryption at rest shipped (v0.7.0): envelope encryption, encrypt-then-EC,
the `--master-key-file` KEK source, the enable-only posture singleton, the SSE-S3
surface. What it deliberately left out is **rotation** — both the master key that
wraps every object's DEK and the cluster CA that anchors inter-node trust. That is
v0.8, and [ADR-0022](adr/0022-cluster-mtls.md) pairs the two: they are the same
keys problem, so they travel as one release.

Two largely independent tracks under one headline. **Track A (KEK rotation) has
landed**; Track B (CA) is the front line.

### Track A — KEK rotation (master-key rewrap) — shipped, [ADR-0032](adr/0032-master-key-rotation.md)

A **metadata-only** rewrap sweep: object bytes and shards are never touched, only
the small wrapped DEK alongside each version changes. Made observable by a
**per-version KEK fingerprint** (an additive `VersionEntry` field — a 64-bit
content hash of the wrapping key) plus a *current fingerprint* on the
`EncryptionPosture` singleton; the count of versions still on the old fingerprint
is the exact progress signal, so completion is *provable* (zero stragglers) and
retiring the old key is safe, not hoped. A node whose loaded KEK fingerprint ≠ the
posture's current fingerprint refuses encrypted writes (the split-key guard). One
rotation at a time; never more than two KEKs loaded. `cluster rotate-key
-new-master-key-file` drives the leader-only sweep to convergence; `cluster
status` shows the rotation and its straggler count. Proven by sim schedules
(rotate, resume, COMPLIANCE-safe), in-process cluster tests, and an e2e that reads
every object after restarting a node with only the new key.

### Track B — CA custody, issuance, and rotation

[ADR-0029](adr/0029-ca-custody-and-issuance.md) settled custody: self-managed CA
is the default, external/operator PKI is a first-class supported path, and the CA
private key is **never** replicated through Raft. v0.8 builds the rotation on top:

- **The design enabler: a multi-CA trust bundle.** Validate against the old and
  the new CA at once (the dual-trust rollover etcd/CockroachDB/Vault use) so there
  is no moment where nothing is trusted. This is the concrete thing to build first.
- **Pluggable issuer interface** — the self-managed default and an external PKI
  (Vault, an offline/HSM root, a corporate CA) behind one seam, so issuance source
  is a configuration choice ([ADR-0029](adr/0029-ca-custody-and-issuance.md)
  decision 2).
- **Narrowed renewal vs issuance** ([ADR-0029](adr/0029-ca-custody-and-issuance.md)
  consequences): renewing an already-admitted node's cert (prove possession, get a
  new cert for the *same* identity) is self-service and need not carry the power to
  mint *new* identities; joins stay rare and root-gated.
- **Lost-CA-key recovery:** regenerate a new CA, migrate trust through the bundle
  (trust old+new, reissue every node cert from new, drop old) — issuance restored
  with no data at risk, since validation only ever needed the CA *certificate*.

The rotation flow and its lost-key case are designed in
**[ADR-0033](adr/0033-ca-rotation.md)** (Proposed): a dual-trust rollover over a
replicated multi-CA trust **bundle** (CA *certs* only, never the key), the
transport reading live trust + leaf per handshake (zero-downtime), reissuance as
the self-service renewal path, a pluggable issuer (self-managed / external PKI),
and the count of members still on the old CA as the provable convergence signal —
planned rotation and lost-key recovery being one flow, since rotation never needs
the *old* CA key. `cluster rotate-ca` drives it; `cluster status` shows progress.

The KEK-rotation half of v0.8 is done; the release is not cut until Track B lands
(or the operator decides to split it). Track B passes are not yet broken out —
the ADR is the next thing to settle before code.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.9
upgrade machinery (feature gates, health interlock, the upgrade test suite), v0.10
zero-downtime rolling upgrades, v0.11 observability. They are pulled into the
section above as they become the front line.
