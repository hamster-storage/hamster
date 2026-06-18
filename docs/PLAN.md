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

The front line is **v0.10: observability and telemetry**, designed in
**[ADR-0035](adr/0035-observability-metrics.md)** (Accepted): one internal metrics
registry as the single source of truth, rendered many ways — a hand-rolled
Prometheus text exposition on the admin port, and a typed snapshot over the control
channel that the CLI and the v0.11 web console both render. Durability/EC health is
the headline signal; tracing deferred.

Passes, in order:

1. **The registry + Prometheus `/metrics`.** A hand-rolled `internal/metrics`
   (counters, gauges, histograms, scrape-time collector callbacks for derived
   signals) with a golden-pinned Prometheus text exposition; an admin HTTP listener
   (`-admin <addr>` on `cluster run` and `serve`) serving `GET /metrics`; a first
   signal set proving it end to end — `build_info{version,generation}`, uptime, and
   on a cluster node a few cheap cluster-wide-derived gauges (members, voters,
   is-leader, effective generation). Recording stays deterministic (no ambient time
   — durations are computed at the call site through the seam clock and observed as
   values; metrics never feed replicated state).
2. **The typed snapshot + CLI.** A versioned protobuf metrics snapshot over a new
   `reqMetrics` control request (the encoding the v0.11 console will also consume),
   a `cluster metrics` command that renders it, and a durability/health summary
   line added to `cluster status`.
3. **The real signals.** Wire the meaty ones through: durability/EC health (objects
   at/below their redundancy floor, shards needing repair), repair/scrub coverage
   and backlog, Raft health (leader/term/commit lag), data-plane latency and
   request rates/errors, capacity. Each proven under the simulation harness where it
   lives on the deterministic path.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.11
web console (over v0.10's signals), then hardening toward v1.0. They are pulled into
the section above as they become the front line.
