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

### Track B — CA rotation: SHIPPED, [ADR-0033](adr/0033-ca-rotation.md)

A dual-trust rollover over a replicated multi-CA trust **bundle** (CA *certs*
only, never the key — [ADR-0029](adr/0029-ca-custody-and-issuance.md)'s line
holds). Built across three passes, each gated + pushed:

1. `internal/certs` + `internal/meta` — `CAFingerprint`/`PoolFromCAs`; the
   replicated `TrustBundle` singleton (generational, compare-and-set), `NodeRecord`
   `LeafCAFingerprint`, the `SetTrustBundle`/`SetNodeLeafCA` proposals.
2. `internal/sys` — the transport reads this node's leaf and trust pool per
   handshake (`GetConfigForClient` + per-dial config, a stable session-ticket key
   preserving resumption), so a reissued leaf and a widened bundle take effect with
   **no restart** — the zero-downtime enabler.
3. `internal/cluster` + `cmd/hamster` — every node builds trust from the bundle
   (seeded at formation, refreshed on each generation, persisted to ca.pem
   atomically for restarts); `cluster rotate-ca` drives the rollover (mint new CA →
   widen to dual trust → reissue every member onto it → drop the old), reissuance
   being a leader-driven push of a new-CA leaf each member validates; `cluster
   status` shows the trust generation and, mid-rotation, the count still on the old
   CA. Distinct CA serials + Subject Key IDs keep same-named CAs unambiguous; the
   driver rides out a leadership blip (`proposeAsLeader`) and a re-run converges
   from any partial state.

The convergence signal mirrors KEK rotation — the count of members still on the
old CA reaching zero, so the old CA is dropped only when provably safe. Proven by
in-process cluster tests and an e2e that **restarts a node trusting only the new
CA** and still serves every object. Planned rotation and lost-CA-key recovery are
one flow (rotation never needs the *old* CA key).

**Both v0.8 tracks are done.** The release is held until Nick signs off.

### Deliberately deferred (noted, not built)

- **External-PKI issuer** ([ADR-0029](adr/0029-ca-custody-and-issuance.md) decision
  2): the self-managed CA is implemented; the pluggable-issuer seam for an operator
  PKI is the additive next step, like the KEK's `--master-key-command`.
- **Issuer-node addressing after rotation:** the rotation driver becomes the issuer
  (holds the new CA key); a cluster whose join issuer is a different node would need
  the new key provisioned there for future joins. Reissuance also transmits the
  member's new key over mTLS, matching join — a CSR flow that keeps the key local is
  the future refinement for both.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.9
upgrade machinery (feature gates, health interlock, the upgrade test suite), v0.10
zero-downtime rolling upgrades, v0.11 observability. They are pulled into the
section above as they become the front line.
