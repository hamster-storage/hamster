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

## Now / next — v0.4 partitioned placement

The current version. Pass 1 (the stored, versioned cluster layout) and pass 2
(failure-domain spread, [ADR-0016](adr/0016-failure-domain-hierarchy.md)) have
landed. Remaining passes, in order — each its own focused change, all building on
the labeled layout:

- **Node liveness / DOWN detection** — PUT skips a known-down node instead of
  opening a stream to it and paying the datapath retransmit timeout (GET already
  abandons stragglers). Runtime liveness surfaced in `cluster status`; the
  replicated `NodeRecord` (ADR-0016, ADR-0004) the registry now lives in is the
  place a committed status/`DRAINING` flag will hang off.
- **Draining** — an operator-set drain flag on `NodeRecord`: placement excludes a
  draining node from new writes; repair/rebalance migrate its shards off.
- **Transition tracking + manual rebalance** — migrate partitions between nodes
  without re-encoding objects ([ADR-0004](adr/0004-partitioned-placement.md), the
  fixed-partition invariant).
- **Repair re-encode** — existing data climbs to the active storage profile as the
  cluster grows.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.5
versioning API, v0.6 object lock, v0.7 encryption at rest, v0.8 upgrade
machinery. They are pulled into the sections above as they become the front line.

CA custody, external/operator PKI, and CA rotation — including recovery from a
lost CA key via a multi-CA trust bundle ([ADR-0029](adr/0029-ca-custody-and-issuance.md),
[ADR-0022](adr/0022-cluster-mtls.md)) — ride **v0.7**, not the placement work:
[ADR-0022](adr/0022-cluster-mtls.md) pairs CA rotation with KEK rotation, so it is
a keys problem. See the v0.7 note in [ROADMAP.md](ROADMAP.md).
