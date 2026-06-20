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

## Now / next — v0.11: Hamster is one clustered path

**Observability shipped at v0.10.0** ([ADR-0035](adr/0035-observability-metrics.md)):
one internal metrics registry rendered many ways — `internal/metrics` with a
golden-pinned Prometheus `/metrics` exposition on the `-admin` listener, a
hand-written-protobuf typed snapshot over `reqMetrics` that `cluster metrics` and the
coming web console render, the durability posture each node derives from its replica,
the `hamster_s3_requests_total{method,code}` counter, and the `cluster status`
durability line (`TestClusterMetricsEndpoint`). Request-latency histograms remain the
open additive increment (a Histogram type + `ServeS3` duration observation through the
gateway clock); they can ride v0.11 or a later release.

The front line is **v0.11: one clustered path**, designed in
**[ADR-0036](adr/0036-one-clustered-path.md)** (the keystone) with two technical
enablers, **[ADR-0037](adr/0037-proposal-forwarding.md)** (proposal forwarding) and
**[ADR-0038](adr/0038-ec-multipart-and-data-path-parity.md)** (EC multipart + parity).
Hamster stops being two data paths and two command namespaces and becomes one product:
a node is a one-node cluster, the CLI is flat, and S3 serves on every node by default.
**This is one release** — dropping `serve` is only safe once the cluster path is a
strict superset of it, so parity and forwarding land with the flatten, not after.

ADRs 0036/0037/0038 are accepted and committed. **Data-path parity is done**
([ADR-0038](adr/0038-ec-multipart-and-data-path-parity.md)): Range-efficient + streaming
GET; streaming PUT end to end (the fed-with-backpressure coordinator, a tunable window,
and the in-flight/throughput/backpressure-stall load metrics); server-side `CopyObject`
and `UploadPartCopy`; and erasure-coded multipart — each part EC'd independently and
durable on upload, `Complete` assembling the part list, GET stitching the parts with
Range mapping to the covering parts (no whole-object re-encode). The cluster S3 path is
now a strict superset of the single-node surface. Remaining passes, in dependency order:

1. **Proposal forwarding** ([ADR-0037](adr/0037-proposal-forwarding.md)) — a non-leader
   does the leadership-independent data work locally, then forwards only the small
   metadata commit to the leader and awaits it, replacing today's `503 SlowDown`. Only
   the commit crosses the leader hop; object bytes never do (invariant 1). Proven under
   simulated cluster schedules (leadership change mid-write, follower-coordinated PUT).
2. **S3 on by default** — `hamster run` serves S3 on `127.0.0.1:9000` by default,
   credentials required (refuse to boot without them, as `serve` did); `-no-s3` for a
   headless storage node, `-s3 <addr>` to override the address.
3. **Flatten the CLI + drop `serve`** — the fifteen `cluster <sub>` commands move to
   top-level verbs; the `cluster` namespace and `serve`/`internal/blob` retire (hard
   break, no aliases). README, CLAUDE.md, GLOSSARY, the demo Taskfile, and every
   e2e/compat invocation move to the flat commands and the one-path model in the same
   release.
4. **One complete help + a drift guard** (the tail item) — a single top-level help that
   lists every command (folding in the good descriptions from today's `clusterUsage`),
   plus a test that iterates the dispatch table and asserts every verb appears in the
   usage string, so help can never again drift from the implemented surface.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.12
adaptive load shedding ([ADR-0039](adr/0039-adaptive-load-shedding.md): latency-gradient
concurrency limiting with 429, request-latency histograms, and degradation detection —
all from in-flight depth and per-op latency), then v0.13 the web console, then hardening
toward v1.0. They are pulled into the section above as they become the front line.
