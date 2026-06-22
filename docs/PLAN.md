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
hand-written-protobuf typed snapshot over `reqMetrics` that `metrics` and the
coming web console render, the durability posture each node derives from its replica,
the `hamster_s3_requests_total{method,code}` counter, and the `status`
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
now a strict superset of the single-node surface.

**Proposal forwarding is done** ([ADR-0037](adr/0037-proposal-forwarding.md)): any node
accepts writes. A non-leader runs the data plane locally and forwards only the small
metadata commit to the leader over the control channel (`reqForward`) — object bytes
never cross the leader hop or the Raft log (invariant 1), and apply errors keep their
identity across the hop. The coordinator's PutObject/UploadPart commits go through a
forwarding `coord.Proposer` (`forwardingProposer`, the hop off-loop with the callback
posted back); the gateway's bucket/delete/multipart-metadata commits go through
`Node.propose`→`forward`. A non-leader no longer answers `503` for being a non-leader
(`503` stays for genuine backpressure and the below-floor durability refusal). Proven by
a real-cluster e2e (PUT, multipart, and an apply error all driven through a follower).

**S3 on by default is done**: `hamster serve` serves S3 on `127.0.0.1:9000` without
being asked, credentials required; `-no-s3` runs a headless storage node, `-s3 <addr>`
overrides the address.

**The CLI is flat**: the fifteen `cluster <sub>` commands are top-level verbs and the
node command is `hamster serve` (renamed from `cluster run`); the old single-node `serve`
command is gone. Dispatch and the generated `-h` help are one table (`commandGroups`), so
help cannot drift from what is dispatched (`TestCommandHelpInSync`). README, CLAUDE.md, the
demo Taskfile, the docs, and every e2e invocation moved to the flat commands and the
one-path model.

**`internal/blob` stays as the gateway's test backend** — a deliberate narrowing of
ADR-0036's "retire `internal/blob`" to "retire it as a *production* path." The single-node
`serve` command is gone, so no user can reach the blob path; but `blob` is the simple,
synchronous backend the 41 gateway unit tests use to exercise the backend-agnostic S3
surface without standing up the whole data plane. The production object path
(`internal/coord`) has its own 41 simulation tests proving durability by decoding shards
off disk, and the gateway↔coord seam is covered by the cluster e2e — so keeping `blob` as a
test seam leaves no production-path coverage gap. Collapsing the gateway's `Objects == nil`
branches and migrating all 41 tests + compat onto an in-process EC backend was weighed and
declined: large, risky, and it makes those tests heavier for no production benefit.

That completes v0.11. The web console (v0.13) and adaptive load shedding (v0.12) are the
next front lines.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.12
adaptive load shedding ([ADR-0039](adr/0039-adaptive-load-shedding.md): latency-gradient
concurrency limiting with 429, request-latency histograms, and degradation detection —
all from in-flight depth and per-op latency), then v0.13 the web console, then hardening
toward v1.0. They are pulled into the section above as they become the front line.
