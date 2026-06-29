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

## Now / next — v0.13: the web console

**v0.12 adaptive load shedding is done** ([ADR-0039](adr/0039-adaptive-load-shedding.md)),
built as five loop-owned, simulator-driven increments on the data-plane coordinator, all
from in-flight depth and per-operation latency — no OS primitives:
- a pure `metrics.Histogram` (cumulative buckets, sum, count; additive Prometheus + snapshot
  codec), closing the open ADR-0035 request-latency-histogram follow-on;
- per-op PUT/GET latency timed on the loop through the seam clock into
  `hamster_s3_request_duration_seconds{method}` (decoupled via a `coord.Config` recorder
  hook — the coordinator never imports `internal/metrics`);
- the RTT gradient tracker: `minRTT` (a re-probing long-window minimum that can rise) and
  `curRTT` (short-window EWMA), `gradient = clamp(minRTT/curRTT, 0..1)`;
- the adaptive limiter: a per-op AIMD concurrency limit gated by the gradient, floored so it
  can never reach zero, that **sheds at admission with `429` + `Retry-After`** (`coord.ErrShed`,
  kept distinct from the `503 SlowDown` durability-floor/non-leader refusal) — shedding only
  ever refuses a *new* request, never touching an admitted or committed object;
- degradation detection: a sustained rise of the `minRTT` floor (not `curRTT`, so pure load
  cannot trip it) surfaced as `hamster_node_degraded` and a candidate `degraded` state in
  `cluster status` — detection only, no auto-eviction.

The front line is **v0.13: the web console**
([ADR-0020](adr/0020-embedded-htmx-web-console.md)): embedded in the binary, served on the
admin port, server-rendered with htmx, decoding the same typed metrics snapshot the CLI does.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.13 the web
console, then hardening toward v1.0. They are pulled into the section above as they become
the front line.
