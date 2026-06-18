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

All three passes have landed and are gated/pushed:

- **The registry + Prometheus `/metrics`** — `internal/metrics` (counters, gauges,
  scrape-time collectors, golden-pinned exposition), the `-admin <addr>` listener on
  `cluster run` and `serve`.
- **The typed snapshot + CLI** — `metrics.Snapshot`/`Family` and the
  hand-written-protobuf `MarshalSnapshot` codec, served over `reqMetrics`, rendered
  by `cluster metrics` through the shared `RenderText` (the encoding the v0.11
  console will consume).
- **The real signals** — the durability posture any node derives from its replica
  (object-version and bucket counts, the active auto profile's k/m,
  layout-transition-open), the `hamster_s3_requests_total{method,code}` counter the
  `ServeS3` middleware increments, and a durability summary line on `cluster status`.
  `TestClusterMetricsEndpoint` proves the gauges, the counter (after an S3 request),
  and the status line end to end.

v0.10 is ready for a **release decision** (held until Nick signs off). The one
additive increment left is **request-latency histograms** — the registry gains a
Histogram type (Prometheus `_bucket`/`_sum`/`_count` exposition + snapshot encoding)
and the `ServeS3` middleware observes request durations through the gateway clock.
It is deliberately its own increment (a histogram with no latency signal is unused
scaffolding); it can ship in v0.10 before release or as a fast-follow. Deeper
durability signals sourced from the repair/scrub sweeps (objects below their floor,
shards needing repair, scrub coverage) and Raft term/commit-lag are the natural
next instrumentation, additive over this foundation.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.11
web console (over v0.10's signals), then hardening toward v1.0. They are pulled into
the section above as they become the front line.
