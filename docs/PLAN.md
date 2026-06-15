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
  transition. Landed so far:
  - the dual-read primitive (`place.Layout.Previous` + `Layout.Locate`) and GET
    consuming it (`coord/get.go` fetches each shard from its new home, falling
    back to its old one), proven by a coord test that drains a node out from under
    populated data and still reads everything (and a control showing the hazard);
  - the committed transition: `meta.ClusterLayout.Previous` (additive field 6) +
    `SetClusterLayout` carrying it, mapped through the coord layout getter to
    `place.Layout.Previous`, so a committed transition flows straight to dual-read.
  - repair *migrates* shards old→new during a transition (`coord/repair.go`):
    `RepairSweep` resolves `Locate` (new + old), scrubs the new home, then for
    each shard not yet there carries it across. **Migration is a copy, not an EC
    reconstruct** — during a transition a shard sits at its *old* home, intact,
    so repair fetches it whole from `old[i]` and writes it to `new[i]`; EC
    reconstruction stays the path only for a shard *damaged* mid-move (clean at
    neither home), rebuilt from any k survivors across old+new. The steady-state
    sweep (no `Previous`) is untouched. Proven by `TestRepairMigratesAcrossTransition`:
    a transition drains a node out from under populated data, a sweep moves every
    shard, a second sweep moves nothing (the convergence signal), and after
    crashing the drained node every object still reads from its new placement
    alone. Crash-mid-migration convergence rides the same staged-then-marker
    commit as a fresh write, so the existing crash schedules cover it.

  Remaining — **the open/close pair, coupled** (per the single-transition model:
  one (old,new) pair, not a stack):
  1. the leader's reconcile *opens* a transition (sets `Previous = prior member
     set`) on a placement-affecting change instead of swapping the layout
     outright, and *closes* it (proposes a layout with `Previous` dropped) once a
     repair sweep over the transition migrates nothing and reports nothing
     unrepairable — the zero `MigratedShards`/all-`Healthy` convergence the sweep
     already returns. The two must land together: never open a transition that
     cannot be closed.
     - **One drain / one transition at a time.** Refuse to open a second
       transition while one is in flight, at two layers: the structural floor
       (reconcile never opens while `Previous != nil` — the durability guard) and
       the operator UX (`cluster drain` refuses up front if a transition is open
       or another node is already draining, naming the in-flight node, to avoid a
       silent no-op). Multi-drain (a *set* of draining nodes folded into one
       transition — the overnight big-cluster case) is additive future work: the
       single-pair format already supports the shape; only the *open* logic would
       change.
     - **Scope:** open transitions for *subtractive* changes (drain/removal),
       which is where positional addressing strands data. Pure additions (join,
       weight-up) continue to ride the existing rebuild-from-k repair as shipped
       capacity weighting already does — so cluster formation, which is all
       additions, does not serialize behind transition closes. Record this
       decision in ADR-0004 when the slice lands.
  Only with this does draining (already wired through `cluster drain`, the
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
