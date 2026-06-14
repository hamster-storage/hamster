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

The current version. The stored, versioned cluster layout; failure-domain
spread ([ADR-0016](adr/0016-failure-domain-hierarchy.md)); capacity weighting
([ADR-0004](adr/0004-partitioned-placement.md)); the replicated node registry
(`meta.NodeRecord`); and node liveness (the passive detector fed by PUT/GET/
repair outcomes, the PUT skip, and the `cluster status` STATE column) have all
landed. Remaining passes, in order — each its own focused change, all building on
the labeled layout:

- **Transition tracking + manual rebalance** — the prerequisite that makes any
  layout change (drain, capacity change, growth) safe for existing data. Shard
  addressing is positional and derived from the member set, so a layout change
  relocates shards and would make existing objects unreadable without a
  transition. Landed so far: the dual-read primitive (`place.Layout.Previous` +
  `Layout.Locate`) and GET consuming it (`coord/get.go` fetches each shard from
  its new home, falling back to its old one), proven by a coord test that drains a
  node out from under populated data and still reads everything (and a control
  showing the hazard without dual-read). Remaining, in order:
  1. `meta.ClusterLayout` carries `Previous` (additive) — the committed transition.
  2. the leader's reconcile *opens* a transition (sets `Previous = prior member
     set`) on a membership change (a node drains out, joins, or weight shifts)
     instead of swapping the layout outright.
  3. repair migrates shards old→new during a transition (copy to the new home,
     reclaim the old), and *closes* the transition (drops `Previous`) once a full
     sweep finds nothing left to migrate — after which the drained node holds no
     live shards and can be removed.
  Only with these does draining (already wired through `cluster drain`, the
  `NodeRecord.Draining` flag, placement demotion, and the `draining` status) become
  safe on a populated cluster ([ADR-0004](adr/0004-partitioned-placement.md), the
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
