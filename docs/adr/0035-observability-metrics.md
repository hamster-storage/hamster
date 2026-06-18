# ADR-0035: Observability — one internal metrics registry, hand-rolled Prometheus exposition, a typed snapshot for the CLI and console

## Status

Accepted

## Context

A cluster an operator can run without a platform team still has to be *legible*:
when a disk rots, a node falls behind, a repair backlog grows, or latency climbs,
the operator needs to see it — ideally before it becomes an incident. v0.10 is the
observability release. Today the only window into a running cluster is `cluster
status` (membership, topology, encryption/CA/version state) and the structured
logs (`slog`, JSON mode); there are no quantitative signals.

Three different consumers want those signals, and that is the load-bearing fact:

1. **An external monitoring stack** — Prometheus or compatible — scraping each node.
2. **Hamster's own web console** (v0.11, [ADR-0020](0020-embedded-htmx-web-console.md)),
   which renders the cluster's health in a browser.
3. **The CLI** — `cluster status` and a richer `cluster metrics`/`cluster health`.

If metrics were built as a Prometheus-only feature, the console and the CLI would
each re-derive the same numbers by scraping and re-parsing text, or by a second
parallel code path. That is the trap this ADR avoids. The constraint shaping the
*how* is the project's dependency discipline ([ADR-0002](0002-single-binary-no-external-dependencies.md),
[ADR-0011](0011-permissive-only-dependencies.md)): a small module graph, prefer
the standard library, every dependency justifies itself. And the determinism rule
([ADR-0009](0009-deterministic-simulation-testing.md)): the code that *records*
metrics runs inside the simulated core, so recording must not read ambient time,
randomness, or I/O.

## Decision

**1. One internal metrics registry is the single source of truth.** A hand-rolled
in-process registry (`internal/metrics`) holds the cluster's signals as counters,
gauges, and histograms. Every subsystem records into it — the coordinator
(durability, repair/scrub progress), raftnode (leader, term, commit lag),
datapath/gateway (request rates, errors, latency), placement (capacity). A number
is produced in exactly one place. The registry is **collect once, render many**:
the renderings below are views over it, never independent re-derivations.

**2. Hand-rolled Prometheus text exposition on the admin port.** `GET /metrics`
on the admin port ([ADR-0020](0020-embedded-htmx-web-console.md)) serves the
Prometheus text exposition format, written in-repo — the format is simple and
stable, and pulling `prometheus/client_golang` (Apache-2.0, so license-clean, but
several transitive dependencies) is not worth the module-graph cost when the
exposition is a few hundred lines. This matches the hand-written-protobuf
precedent ([ADR-0023](0023-handwritten-protowire-codecs.md)). Each node exposes
its own `/metrics`; cross-node aggregation is the scraper's job, the standard
Prometheus model — no central collector to run, fitting the single-binary promise.

**3. A typed metrics snapshot over the cluster control channel.** A new control
request (alongside `reqStatus`) returns the registry as a versioned protobuf
record — a list of metric families (name, type, labels, samples). This is what
the **CLI** (`cluster status`'s new health summary, and a fuller `cluster
metrics`) and the **web console** (v0.11) both consume. The console is a *renderer*
over this snapshot, not a second metrics path — so v0.10 builds the model and the
snapshot, and the console release is mostly a view. The Prometheus text and the
typed snapshot are two encodings of one registry.

**4. Two classes of signal, by where the number comes from.**

- **Cluster-wide, derived from replicated state.** Durability/EC health (objects
  at or below their redundancy floor, degraded/missing shards), object and bucket
  counts, open layout transitions, rotation progress, the effective generation.
  Any node computes these from its own replica — consistent regardless of which
  node is asked, exactly as `cluster status` already resolves membership.
- **Per-node operational.** This node's request rates, latencies, disk usage,
  repair work in flight, liveness view. Local; the CLI and console show them
  per-node or summed.

`cluster status` gains a short **durability/health summary** (objects at risk,
repair backlog) from the first class; the full set lives in `cluster
metrics`/the console.

**5. Durability is the headline signal; tracing is deferred.** For a
compliance-shaped store the first question is always *is my data safe* — so the
EC/durability signals (objects below their floor, shards needing repair, scrub
coverage and backlog) are the priority surface, then Raft health, data-plane
latency, capacity, and S3 request/error rates. Structured JSON logs already exist
(`slog`); request-id'd access logs are in scope. **Distributed tracing is deferred**
— it is a larger surface with its own propagation and exporter questions, and
metrics plus structured logs answer the operator's first questions.

**6. Metrics are observability-only and deterministic to record.** A metric never
feeds replicated state or control flow — it is a side effect, never an input, so
it stays outside the Raft/state-machine path entirely. Time-based metrics
(latency histograms, rates) take their timestamp from the injected seam clock, not
an ambient read, so recording is deterministic under the simulator. The registry
is concurrency-safe (recorded from handler goroutines and the loop alike) but
carries no cross-node consensus — each node's operational counters are its own.

## Consequences

- The console (v0.11) and the CLI read the same typed snapshot, so there is one
  metrics model to keep correct, not three. Adding a signal means recording it
  once; all three surfaces get it.
- No new runtime dependency: the exposition and the snapshot codec are in-repo,
  keeping the module graph small and the binary static.
- Each node is an independent Prometheus scrape target. Cluster-wide rollups in an
  external stack are the operator's PromQL; Hamster does not run a collector. The
  cluster-wide *derived* signals (durability) are still consistent because any node
  computes them from replicated metadata.
- Recording through the seam clock keeps the simulation harness deterministic, and
  because metrics never feed replicated state, they cannot perturb correctness —
  an observability bug stays an observability bug.
- The admin port now serves both `/metrics` (this release) and the console
  (v0.11), so its surface and access model are settled here rather than later.
- Deferring tracing keeps v0.10 focused on what answers the operator's first
  questions; it can be added additively when request-level causality across nodes
  becomes the need.

## Alternatives considered

- **`prometheus/client_golang` for the registry and exposition.** The obvious
  default and license-clean (Apache-2.0), but it pulls several transitive
  dependencies for a text format that is a few hundred lines to emit, against the
  small-module-graph rule ([ADR-0002], [ADR-0011]). Hand-rolling also keeps the
  internal model ours to shape for the snapshot. Rejected for the dependency cost.
- **OpenTelemetry / OTLP push as the primary surface.** More modern and
  vendor-neutral, but push needs a collector to run — an external service the
  single-binary promise avoids — and the SDK is a large dependency. Kept as a
  possible *additive* exporter later (another rendering over the same registry),
  not the v0.10 default.
- **Prometheus-only, no internal model.** Simplest to start, but the console and
  `cluster status` would scrape and re-parse text or grow a parallel path —
  re-deriving the same numbers, exactly the duplication this ADR exists to prevent.
- **Distributed tracing in v0.10.** Valuable for cross-node request causality, but
  a larger surface (context propagation, an exporter, sampling) than the first
  observability cut needs. Deferred to first real need.
- **Metrics as replicated/consensus state.** Aggregating counters through Raft
  would make a cluster-wide total a single committed number, but it would put
  high-frequency observability traffic through the consensus path and couple a
  metric bug to correctness. Rejected: per-node counters plus scraper-side
  aggregation is how Prometheus is meant to be used, and it keeps metrics off the
  critical path.
