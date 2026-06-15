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

- **Repair re-encode + downsize** — existing data re-encoded to a different
  storage profile, in place. Two directions: climbing *up* as a cluster grows
  (better efficiency, optional — data is already safe), and stepping *down* so a
  cluster can shrink *below* an existing object's width. The latter is the
  operator-facing "downsize": the auto-ladder picks the profile for the target
  node count (the operator thinks in node counts, not k+m), a re-encode sweep
  rewrites every object to it (repair-driven, staged-then-marker, COMPLIANCE-safe
  — same bytes, never deletes/shortens lock), then `cluster drain --reencode`
  removes the node. Until it lands `cluster remove`/`drain` refuse a shrink that
  would strand data (k is never downgraded in place). Note the ladder isn't
  uniform: 6→5 (4+2→3+2) keeps 2-failure tolerance and only costs efficiency, but
  5→4 (3+2→2+1) drops tolerance to 1 — the confirmation must state the real
  per-step trade. *Replacing* a node at constant size already works (join with
  `-replaces <old>`), and needs no re-encode — it's the size-changing case that
  does.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.5
versioning API, v0.6 object lock, v0.7 encryption at rest, v0.8 upgrade
machinery. They are pulled into the sections above as they become the front line.

CA custody, external/operator PKI, and CA rotation — including recovery from a
lost CA key via a multi-CA trust bundle ([ADR-0029](adr/0029-ca-custody-and-issuance.md),
[ADR-0022](adr/0022-cluster-mtls.md)) — ride **v0.7**, not the placement work:
[ADR-0022](adr/0022-cluster-mtls.md) pairs CA rotation with KEK rotation, so it is
a keys problem. See the v0.7 note in [ROADMAP.md](ROADMAP.md).
