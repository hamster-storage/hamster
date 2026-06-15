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

v0.4's headline work — drain/replace/remove, transition tracking, and downsize
re-encode ([ADR-0031](adr/0031-reencode-across-profiles.md)) — has landed. What
remains is the optional other direction:

- **Upsize re-encode** — climbing existing data *up* to a larger profile as a
  cluster grows, for storage efficiency. The same machine as downsize
  (`coord.ReEncode`), but optional: data is already safe at the smaller profile,
  so it must never fire automatically on a join (a join can't trigger a full
  re-encode) — a background, throttled, opt-in pass. Downsize is the safety
  feature; upsize is the tidy-up.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.5
versioning API, v0.6 object lock, v0.7 encryption at rest, v0.8 upgrade
machinery. They are pulled into the sections above as they become the front line.

CA custody, external/operator PKI, and CA rotation — including recovery from a
lost CA key via a multi-CA trust bundle ([ADR-0029](adr/0029-ca-custody-and-issuance.md),
[ADR-0022](adr/0022-cluster-mtls.md)) — ride **v0.7**, not the placement work:
[ADR-0022](adr/0022-cluster-mtls.md) pairs CA rotation with KEK rotation, so it is
a keys problem. See the v0.7 note in [ROADMAP.md](ROADMAP.md).
