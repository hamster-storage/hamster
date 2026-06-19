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

All three ADRs are written (Proposed — an ADR ahead of code is normal). Execution
passes, in dependency order:

1. **Data-path parity** ([ADR-0038](adr/0038-ec-multipart-and-data-path-parity.md)) —
   the prerequisite that lets `serve` retire without regressing the S3 surface:
   - **Range-efficient GET** — plumb the HTTP Range through the gateway
     `ObjectBackend.Get` into the existing `coord.Get(off,length)` (the engine already
     prefetches only the covering shards; the gateway interface currently discards it).
   - **Streaming PUT** — change `coord.Put`/`ObjectBackend.Put` to take `io.Reader`+size
     end to end so large objects do not sit whole in RAM, restoring the bounded-memory
     promise on the cluster path.
   - **Server-side `CopyObject` / `UploadPartCopy`** — read-and-re-encode through the
     coordinator, streaming.
   - **Erasure-coded multipart** — each part EC'd independently and durable on upload
     (keyed by `uploadId`+part under the existing `u/` metadata prefix), `Complete`
     assembling the part list into a version entry, GET stitching parts and Range
     mapping to the covering parts. No whole-object re-encode.
2. **Proposal forwarding** ([ADR-0037](adr/0037-proposal-forwarding.md)) — a non-leader
   does the leadership-independent data work locally, then forwards only the small
   metadata commit to the leader and awaits it, replacing today's `503 SlowDown`. Only
   the commit crosses the leader hop; object bytes never do (invariant 1). Proven under
   simulated cluster schedules (leadership change mid-write, follower-coordinated PUT).
3. **S3 on by default** — `hamster run` serves S3 on `127.0.0.1:9000` by default,
   credentials required (refuse to boot without them, as `serve` did); `-no-s3` for a
   headless storage node, `-s3 <addr>` to override the address.
4. **Flatten the CLI + drop `serve`** — the fifteen `cluster <sub>` commands move to
   top-level verbs; the `cluster` namespace and `serve`/`internal/blob` retire (hard
   break, no aliases). README, CLAUDE.md, GLOSSARY, the demo Taskfile, and every
   e2e/compat invocation move to the flat commands and the one-path model in the same
   release.
5. **One complete help + a drift guard** (the tail item) — a single top-level help that
   lists every command (folding in the good descriptions from today's `clusterUsage`),
   plus a test that iterates the dispatch table and asserts every verb appears in the
   usage string, so help can never again drift from the implemented surface.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.12 web
console (over v0.10's signals), then hardening toward v1.0. They are pulled into the
section above as they become the front line.
