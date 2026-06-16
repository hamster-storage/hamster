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

Two largely independent tracks under one headline. **Track A is the front line**
(self-contained, rides existing sweep machinery); Track B follows.

### Track A — KEK rotation (master-key rewrap) — designed, [ADR-0032](adr/0032-master-key-rotation.md)

A **metadata-only** rewrap sweep: object bytes and shards are never touched —
only the small wrapped DEK alongside each version changes. Load the old and the
new KEK, walk the keyspace, unwrap each version's DEK under the old KEK and rewrap
it under the new one (the wrap nonce stays the version ID), and commit the new
`WrappedDEK` through a meta proposal. A leader-only sweep in the shape of
`Optimize`/the scrubber (under the shared single-flight guard), proven under the
simulator: rotate, every object still decrypts; crash mid-rotation resumes and
converges; COMPLIANCE-safe by construction (the lock and the bytes are untouched,
only the wrapped key is rewritten).

**Design settled in [ADR-0032](adr/0032-master-key-rotation.md):** the rotation is
made observable by a **per-version KEK fingerprint** (an additive `VersionEntry`
field — a 64-bit content hash of the wrapping key) plus a *current fingerprint* on
the `EncryptionPosture` singleton. The count of versions still on the old
fingerprint is the exact, cheap progress signal — completion is *provable* (zero
stragglers) so retiring the old key is safe, not hoped. A node whose loaded KEK
fingerprint ≠ the posture's current fingerprint refuses encrypted writes (a
split-key misconfig guard v0.7 lacks). One rotation at a time; never more than two
KEKs loaded. CLI: `cluster rotate-key -new-master-key-file`.

Passes (data-path/metadata-first, CLI last), to be detailed as each starts:
1. `internal/keys` — KEK fingerprint at load (domain-separated hash prefix, cached
   on the loaded KEK).
2. `internal/meta` — additive `VersionEntry` KEK-fingerprint field + posture
   current/rotating-to fingerprints; the rewrap proposal; back-compat (absent
   fingerprint = founding KEK).
3. `internal/coord` — the leader-only rewrap sweep (single-flight guard, resumable
   by straggler scan, yields to a layout transition); write-time fingerprint guard;
   sim schedules.
4. `internal/cluster` + `cmd/hamster` — two-key load, `cluster rotate-key`, the
   straggler count + rotation state in `cluster status`; cluster/e2e proof.

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

### Settled

- **KEK-first** (Track A before Track B): self-contained, rides existing sweep
  machinery, ships an early win and a working rewrap template.
- **KEK-mix resolution: per-version KEK fingerprint** (not try-both), for the
  observability and audit it buys — [ADR-0032](adr/0032-master-key-rotation.md).
- **New key reaches the cluster via** `cluster rotate-key -new-master-key-file`,
  reusing the `--master-key-file` source shape.

Track B (CA) still owes its own ADR (the multi-CA trust bundle, pluggable issuance,
lost-key recovery) when it becomes the front line.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.9
upgrade machinery (feature gates, health interlock, the upgrade test suite), v0.10
zero-downtime rolling upgrades, v0.11 observability. They are pulled into the
section above as they become the front line.
