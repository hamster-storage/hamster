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

## Now / next — v0.9 upgrade machinery

v0.8 shipped (v0.8.0): master-key rotation (`cluster rotate-key`, [ADR-0032](adr/0032-master-key-rotation.md))
and CA rotation (`cluster rotate-ca`, [ADR-0033](adr/0033-ca-rotation.md)). The
front line is now the machinery a zero-downtime rolling upgrade needs — built so
v0.10 can automate the roll on top. Designed in **[ADR-0034](adr/0034-rolling-upgrade-machinery.md)**
(Accepted), which partially supersedes [ADR-0008](adr/0008-versioned-formats-rolling-upgrades.md)
decision 6: the cluster version auto-rolls etcd-style rather than via a manual
admin finalize, because Hamster's additive formats make most changes auto-safe
(etcd-like), so a manual gate only earns its keep for the rare non-additive change.

Three pieces, in roughly this order:

1. **Per-node version advertisement.** Two facts in each node's replicated
   `NodeRecord`: a `binary_version` *string* (display only — `v0.11.0-rc.1`) and a
   declared protocol **generation** *integer* (what gating and the skew check
   compare). The cluster's effective generation is the min across live members
   (etcd-style, auto-rolled); `cluster status` shows each node's build string, its
   generation, and the effective generation, so a half-finished roll is visible.
   The generation advances at most one per release and only for a *coordinated*
   change (a purely additive release does not tick it; dev iteration is throwaway),
   which keeps the sequence dense — the basis of the **one-generation skew rule** (a
   node at most one generation ahead of the cluster, upgrade-one-step-at-a-time made
   checkable; advisory in v0.9, enforced by v0.10's roll). No gate *consumes* the
   generation yet — the additive discipline covers field additions, and v0.9 adds no
   new Raft command type — so gate *enforcement* and the first real gate are deferred
   to first need (the next non-additive change registers its own gate; a manual hold
   stays in reserve for the day an irreversible change wants a rollback window).
2. **The health interlock.** `cluster can-stop <node>`: safe only when the
   remaining voters keep Raft quorum ([ADR-0017](adr/0017-raft-voter-cap-learners.md)),
   no *other* node is currently down, and no layout transition is open
   ([ADR-0004](adr/0004-partitioned-placement.md)) — the rolling discipline made
   checkable (proceed only from full health, one node at a time). Advisory in v0.9;
   v0.10's automated roll drives the same check. The data-plane dimension (EC
   tolerance, not just quorum) is what etcd's interlock lacks and Hamster's adds.
3. **The end-to-end upgrade suite** ([SIMULATION.md](SIMULATION.md) "the upgrade
   suite", [ADR-0009](adr/0009-deterministic-simulation-testing.md)): obtain the
   binary for version N (last release) and N+1 (this build), start at N, write
   versioned + object-locked data under live load, roll node-by-node to N+1
   honoring the interlock, and assert continuous availability, zero data loss, and
   that the effective generation rolls forward as the last node lands.

**Pre-step:** fix two stale comments that say feature gates are "v0.8"
(`internal/meta/proposal_codec.go`, `docs/SIMULATION.md`) — they predate the
v0.7/v0.8 split and the etcd-style decision; correct them to reflect [ADR-0034](adr/0034-rolling-upgrade-machinery.md)
(additive discipline + a gate at first need, not a v0.8 feature-gate framework).

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.10
zero-downtime rolling upgrades (orchestration over v0.9's interlock), v0.11
observability. They are pulled into the section above as they become the front line.
