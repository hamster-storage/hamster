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

## Now / next — v0.10 zero-downtime rolling upgrades

v0.9 upgrade machinery has landed (all three pieces of [ADR-0034](adr/0034-rolling-upgrade-machinery.md)):
per-node version advertisement (`SetNodeVersion`, the leader's `versionMonitor`,
`cluster status` showing per-node version + effective generation + skew note), the
health interlock (`cluster can-stop <node>`), and the end-to-end upgrade suite
(`TestClusterRollingUpgrade`: two generations from one source, a three-node roll
under live load with versioned + COMPLIANCE-locked data, proving availability,
zero loss, and the effective generation auto-rolling only after the last node).

The front line is now **v0.10: zero-downtime rolling upgrades** — the orchestration
*over* v0.9's machinery. v0.9 left stop/swap/start to the operator (advisory
`can-stop`, out-of-band binary swap); v0.10 drives the loop: an operator command
(or controller) that, given a target version, rolls the cluster node by node on
its own — consult `can-stop`, drain/stop, wait for the replacement to rejoin and
re-advertise, repeat — and *enforces* what v0.9 only surfaces (the one-generation
skew rule, the interlock as a hard gate by never asking to stop a refused node).
Design owes its own ADR before the passes break out; the interlock and
advertisement it builds on are tested and shipped.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.11
observability/telemetry, v0.12 web console. They are pulled into the section above
as they become the front line.
