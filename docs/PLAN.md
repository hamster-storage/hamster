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

## Now / next — v0.10 observability and telemetry

**Zero-downtime rolling upgrades shipped at v0.9.0** ([ADR-0034](adr/0034-rolling-upgrade-machinery.md)):
version advertisement (`SetNodeVersion`, the leader's `versionMonitor`, etcd-style
auto-roll), the health interlock (`cluster can-stop`), the end-to-end upgrade suite
(`TestClusterRollingUpgrade`), and the supported operator-driven per-node procedure
([UPGRADES.md](UPGRADES.md)). The binary swap is the deployment system's job, per
node — Hamster owns the safety machinery and the proof, not the swap — so there is
no in-cluster "upgrade driver" to build. The roadmap pulled forward a version: what
was v0.11 is now the front line.

The front line is **v0.10: observability and telemetry** — making a running cluster
legible to an operator and their monitoring stack. The shape is still to be
designed (it owes its own ADR before passes break out); the open questions are
roughly: a metrics surface (Prometheus `/metrics` on the admin port? OpenTelemetry?
both?), which signals matter first (durability/EC health, repair/scrub progress,
Raft and data-plane latency, capacity, request rates and errors), structured logs
and request tracing, and how this lands without a platform team — useful out of the
box, integrating with what an operator already runs. Design next, then break out
the passes.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.11
web console (over v0.10's signals), then hardening toward v1.0. They are pulled into
the section above as they become the front line.
