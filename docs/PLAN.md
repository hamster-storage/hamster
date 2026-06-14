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

## Now / next

1. **Persist cluster metadata to BadgerDB under Raft** — the clustered metadata
   plane recovers from the Raft log plus snapshots and writes no BadgerDB, contrary
   to the "BadgerDB on each replica" decision in
   [ADR-0005](adr/0005-metadata-badgerdb-raft.md). Wire BadgerDB as the per-replica
   apply target with the applied index reconciled across restarts so recovery
   replays only the un-applied log tail exactly once. Drive it red→green from
   `TestClusterMetadataPersistsToBadgerDB` (today `t.Skip`-ped). Durability/correctness
   gap — ahead of the v0.4 feature work below.

2. **CA custody and issuance** — implement the
   [ADR-0029](adr/0029-ca-custody-and-issuance.md) directions: a pluggable issuer so
   an operator can point the CA at an external PKI / bring their own key, the
   self-managed CA staying the default; init-node-loss degradation made explicit
   (existing nodes keep serving; only joins/renewals pause).

## v0.4 — partitioned placement (remaining passes)

Pass 1 (the stored, versioned cluster layout) and pass 2 (failure-domain spread,
[ADR-0016](adr/0016-failure-domain-hierarchy.md)) have landed. Remaining passes,
in order — each its own focused change, all building on the labeled layout:

- **Capacity weighting** — balance load *within* the failure-domain spread
  ([ADR-0004](adr/0004-partitioned-placement.md)).
- **Node liveness / status registry** — the full `NodeRecord` with DOWN detection
  and draining; PUT skips down nodes instead of paying their write timeout.
- **Transition tracking + manual rebalance** — migrate partitions between nodes
  without re-encoding objects ([ADR-0004](adr/0004-partitioned-placement.md), the
  fixed-partition invariant).
- **Repair re-encode** — existing data climbs to the active storage profile as the
  cluster grows.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.5
versioning API, v0.6 object lock, v0.7 encryption at rest, v0.8 upgrade
machinery. They are pulled into the sections above as they become the front line.
