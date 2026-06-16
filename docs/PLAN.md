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

Two largely independent tracks under one headline.

### Track A — KEK rotation (master-key rewrap)

A **metadata-only** rewrap sweep: object bytes and shards are never touched —
only the small wrapped DEK alongside each version changes. Load the old and the
new KEK, walk the keyspace, unwrap each version's DEK under the old KEK and rewrap
it under the new one (the wrap nonce stays the version ID), and commit the new
`WrappedDEK` through a meta proposal. A leader-only sweep in the shape of
`Optimize`/the scrubber (under the shared single-flight guard), proven under the
simulator: rotate, every object still decrypts; crash mid-rotation resumes and
converges; COMPLIANCE-safe by construction (the lock and the bytes are untouched,
only the wrapped key is rewritten).

**Open design question to settle first (new ADR).** During a rotation the cluster
holds a *mix* — some DEKs wrapped under the old KEK, some under the new — so a
reader must know which KEK unwraps a given version. Two candidates:
- **Try-both:** each node holds old+new KEK during the rotation window and tries
  the new KEK, falling back to the old. Simplest; no record change; the window is
  the only place two keys are loaded.
- **Per-version KEK id:** record a small KEK-generation marker on each version
  (additive field) so unwrap is unambiguous and a stalled rotation is self-
  describing. More metadata, but no ordering assumptions.

This is the load-bearing decision for the track and wants the user's call before
code (the same way the v0.7 KEK *source* was decided up front).

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

The rotation flow and its lost-key case get **their own ADR** when the work starts
([ROADMAP.md](ROADMAP.md) records this).

### Sequencing / open questions for the user

- **Which track first?** Track A (KEK) is self-contained and rides machinery that
  already exists (the sweep shape, the meta proposal pattern); Track B is the
  larger, more security-sensitive design (a new seam + a new ADR). KEK-first gives
  an early, shippable win and a working sweep template.
- **The KEK-mix resolution** (try-both vs per-version id) above — settle before
  Track A code.
- **Where the *new* key/CA material is supplied** (a second `-master-key-file`-
  shaped flag, a `cluster rotate-key` command reading it) — a CLI-surface call.

v0.8 is design-first: unlike v0.5–v0.7 (which mostly exposed pre-built metadata),
the rotation tracks have genuine open decisions, so the first step is the ADR, not
the code.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.9
upgrade machinery (feature gates, health interlock, the upgrade test suite), v0.10
zero-downtime rolling upgrades, v0.11 observability. They are pulled into the
section above as they become the front line.
