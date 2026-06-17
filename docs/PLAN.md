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

Two of three pieces have landed: per-node version advertisement (`SetNodeVersion`,
the leader's `versionMonitor`, `cluster status` showing per-node version +
effective generation + skew note; `TestClusterVersionAdvertisement` proves a
three-node roll), and the health interlock (`cluster can-stop <node>`, the quorum
+ no-other-down + no-open-transition check; `TestClusterCanStopInterlock` proves
the quorum math at 1/2/3 voters). One piece remains:

1. **The end-to-end upgrade suite** ([SIMULATION.md](SIMULATION.md) "the upgrade
   suite", [ADR-0009](adr/0009-deterministic-simulation-testing.md)): obtain the
   binary for version N (last release) and N+1 (this build), start at N, write
   versioned + object-locked data under live load, roll node-by-node to N+1
   honoring the interlock, and assert continuous availability, zero data loss, and
   that the effective generation rolls forward as the last node lands.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.10
zero-downtime rolling upgrades (orchestration over v0.9's interlock), v0.11
observability. They are pulled into the section above as they become the front line.
