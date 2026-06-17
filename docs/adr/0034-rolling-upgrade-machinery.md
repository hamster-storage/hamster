# ADR-0034: Rolling-upgrade machinery — etcd-style auto-rolled cluster version, the health interlock, the upgrade test suite

## Status

Proposed

Partially supersedes [ADR-0008](0008-versioned-formats-rolling-upgrades.md)
decision 6 (a manually-finalized cluster version): the cluster version auto-rolls
etcd-style instead. Everything else in ADR-0008 — additive versioned formats,
expand-then-contract, one major version at a time, the health interlock, the
upgrade test suite — stands and is what this ADR builds.

## Context

[ADR-0008](0008-versioned-formats-rolling-upgrades.md) set the upgrade
*discipline*: every persistent and networked structure is additively versioned
protobuf (old readers skip unknown fields), breaking changes expand-then-contract
across releases, and a cluster upgrades one node at a time within fault tolerance.
It named three pieces of *machinery* a zero-downtime rolling upgrade needs but did
not build: a cluster version with feature gates, a health-check interlock, and the
end-to-end upgrade test suite. v0.9 builds them; v0.10 automates the roll on top.

Two facts shape the design, and the second revises an ADR-0008 decision:

- **Hamster's metadata plane is architecturally an etcd**: a Raft cluster that
  upgrades by rolling one member at a time, quorum preserved throughout. The
  reference model is etcd's own rolling upgrade, not a bespoke one. Hamster adds a
  *data plane* etcd lacks — erasure-coded shards — so its interlock must check EC
  durability as well as Raft quorum (the analogue of a Kubernetes PodDisruption
  Budget, which etcd has no equivalent of).
- **Hamster's formats are additive.** A new field a new node writes is simply
  ignored by an old node. So *most* changes are already mixed-version-safe and
  rollback-safe with no gate at all. The narrow exception is a change old nodes
  cannot safely ignore — chiefly a **new Raft command type** (an old node refuses a
  command it cannot apply, by design: *"newer node? upgrade first"*) or an
  all-or-nothing behavior. Only those need a gate.

ADR-0008 decision 6 specified a *manually finalized* cluster version (the MongoDB
`featureCompatibilityVersion` model): features stay dormant until an administrator
commits the bump, buying a rollback window. That window matters when an upgrade
makes irreversible on-disk changes. Hamster's additive discipline means it rarely
does — so Hamster sits in etcd's camp (auto-roll is safe), and a manual gate would
mostly be ceremony. This ADR adopts the etcd model and keeps the manual hold in
reserve for the day a genuinely irreversible change needs it.

## Decision

**1. The cluster version auto-rolls etcd-style.** Each node advertises its binary
version in its replicated `NodeRecord` (the reserved `binary_version` field). The
cluster's effective version is the **minimum across live members**, recomputed as
membership and versions change — exactly etcd's cluster version. There is no
manual finalize step in the common path. A feature that requires every node
(should one ever exist) activates when the effective version reaches it; until
then it is dormant, and because the version only rolls forward once the last node
is upgraded, mixed-version operation never half-enables it.

**2. Feature-gate *enforcement* is deferred to first need.** v0.9 ships the
version *advertisement* and its display, not a gate framework, because v0.9 adds
no change that needs one — the additive discipline already covers field additions,
and there is no new command type this release. When the first non-additive change
lands (a new Raft command, say), it registers a gate against the effective version
(don't propose the command until every node can apply it) — a small, local
addition then, on the foundation laid here. A manual hold ("don't roll the version
past N yet") can be added at that point if a future change is ever irreversible
enough to want a rollback window; it is not built speculatively now.

**3. The health interlock is a conservative full-health gate.** `cluster can-stop
<node>` answers whether taking a node down for upgrade is safe. It refuses unless:
the remaining voters keep Raft quorum without it ([ADR-0017](0017-raft-voter-cap-learners.md));
no *other* node is currently down (so the cluster is not already degraded —
stopping one healthy node from a healthy cluster never drops an object below its
read floor, since shards are node-distinct and `m ≥ 1`); and no layout transition
is open ([ADR-0004](0004-partitioned-placement.md): a node leaving mid-migration
is unsafe). This is the rolling-upgrade discipline made checkable — proceed only
from full health, one node at a time. It is **advisory** in v0.9: the operator (or
v0.10's automation) consults it before each step; a node does not refuse to stop.

**4. The end-to-end upgrade test suite** ([ADR-0009](0009-deterministic-simulation-testing.md),
[SIMULATION.md](../SIMULATION.md)) is the proof. It obtains the binary for version
N (the last release) and N+1 (the current build), starts a cluster at N, writes a
known workload including versioned and object-locked data, and rolls node by node
to N+1 under a live read/write workload — honoring the interlock between steps —
asserting continuous availability, zero data loss, and that reads come back
intact. It also asserts the effective cluster version rolls forward as the last
node upgrades. (The "finalize → gated feature activates" step from ADR-0008's
sketch is N/A until a gated feature exists; the suite gains it with the first
gate.)

## Consequences

- The metadata plane's upgrade story is etcd's, which is well-trodden: roll one at
  a time, quorum preserved, version auto-rolls when the last node lands. No new
  admin verb in the common path.
- The interlock makes "is it safe to take this node down" a question the system
  answers, not the operator guesses — and it is the exact check v0.10's automated
  roll will drive, so that release is orchestration over machinery that already
  exists and is tested.
- Deferring the gate framework keeps v0.9 honest: it builds what has immediate
  value (observability of the roll, the interlock, the upgrade proof) and does not
  ship a feature-gate mechanism with nothing to gate. The cost is that the first
  non-additive change must add its gate as part of its own work — which is the
  right place for it anyway.
- Rollback safety in v0.9 rests on the additive discipline, not a manual gate: a
  node rolled back to N reads everything N+1 wrote, because N+1 only added fields N
  ignores. The day a change breaks that assumption, it must either stay additive or
  introduce the manual hold this ADR keeps in reserve — and the upgrade suite is
  where that obligation is enforced.
- `binary_version` becoming replicated state means a node stamps it at
  registration and on version change; `cluster status` surfaces the per-node
  version and the effective cluster version, so a half-finished roll is visible.

## Alternatives considered

- **The manual finalize (MongoDB FCV) of ADR-0008 decision 6.** Buys a rollback
  window across irreversible changes, but Hamster's additive formats make such
  changes rare, so the gate would mostly be ceremony an operator must remember to
  perform — and a forgotten finalize silently withholds features, a confusing
  failure mode. Adopted in reserve (decision 2), not as the default. This is the
  decision this ADR revises.
- **Building the full feature-gate framework now.** Symmetric and "complete," but
  there is nothing to gate in v0.9, so it would be untested-by-real-use scaffolding
  shipped ahead of need — exactly the speculative generality the project avoids.
  Deferred to first need.
- **An enforced stop-refusal (a node won't shut down when unsafe).** A stronger
  guarantee, but a process refusing SIGTERM is a sharp edge — operators expect stop
  to stop, and an automated roll or a genuine emergency must always be able to take
  a node down. The interlock informs that decision; it does not override it.
  Rejected for v0.9; the coordinated roll (v0.10) owns enforcement by *not asking*
  to stop a node the interlock refuses.
- **Cluster version as the release string (vX.Y) rather than a declared version.**
  Couples gating to marketing numbers and needs semver comparison in the hot path;
  a declared, monotonic version the binary owns is what etcd and MongoDB both use.
  The `binary_version` string is kept for *display*; any gate compares declared
  versions, not parses release tags.
