# ADR-0039: Adaptive load shedding — latency-gradient concurrency limiting and degradation detection

## Status

Accepted. Scheduled for v0.12.

## Context

A node has finite capacity, and the limit is not a number we can know ahead of
time. The real ceiling depends on the deployment: a container CPU/IO cgroup, the
drive's actual IOPS, the NIC, a noisy neighbour, the object-size mix. Under more
load than it can serve, a node that keeps accepting work does not get faster — it
queues. Latency climbs, memory and open shard-streams pile up, clients time out
and retry, and the retries deepen the queue: a metastable collapse. The store
should instead **refuse new work it cannot serve and say so**, so a client backs
off and the node stays healthy.

Two things make a naive answer wrong:

- **A flat threshold is brittle and blind.** "Max N concurrent" or "shed above X%
  CPU" needs per-deployment tuning and still misses the true bottleneck — the
  ceiling moves with the object-size mix and the environment, and CPU% says
  nothing about a saturated disk or NIC.
- **Sampling OS resources is the wrong primitive.** Measuring true CPU/disk
  utilisation cross-platform without cgo is hard (Linux `/proc` is one shape,
  other OSes another), it is not deterministic for the simulation harness
  ([ADR-0009](0009-deterministic-simulation-testing.md)), and it still cannot see
  a cgroup limit, a network ceiling, or a neighbour.

The signal we *can* observe, in-process and cheaply, is the system's own response.
By **Little's Law** (`in-flight = arrival_rate × latency`), a saturated system
cannot raise throughput, so added load only inflates latency and in-flight depth.
Every cause — cgroup, drive, NIC, neighbour — produces the *same* signature:
throughput plateaus while latency rises. This is the basis of adaptive concurrency
limiting, the technique TCP congestion control, Netflix's `concurrency-limits`,
and Envoy's adaptive concurrency all use. Hamster already measures the inputs: the
streaming-PUT in-flight gauge ([ADR-0035](0035-observability-metrics.md)/[ADR-0038](0038-ec-multipart-and-data-path-parity.md))
and a seam clock that makes per-operation latency deterministic.

A second, related question the same signal answers: a **degrading resource**. If
latency rises while load is *steady*, the cause is not the workload — the floor
got slower (a failing drive, a throttled volume). That is a fault to surface, not
just load to shed.

## Decision

Add a per-node **adaptive load shedder** over the data-plane S3 operations,
deriving everything from in-flight depth and per-operation latency — no OS
primitives, no cgo, deterministic under the simulator.

1. **Measure per-operation latency on the loop.** Each PUT and GET is timed from
   admission to completion through the seam clock. The recorded durations feed
   request-latency histograms (closing the open observability follow-on from
   ADR-0035) and the limiter below.

2. **Maintain a no-load baseline and a current estimate.** Track `minRTT` (the
   best-case latency, a long-window minimum — the service time with no queuing)
   and `curRTT` (a short-window estimate of recent latency). Their ratio, the
   **gradient** `clamp(minRTT / curRTT, 0..1)`, is ≈1 when healthy and falls
   toward 0 as queuing grows.

3. **Hold a dynamic concurrency limit, not a fixed one.** The limit grows while
   the gradient stays near 1 (the node has headroom) and shrinks as latency rises
   (a gradient/AIMD update; exact constants are an implementation detail). It is
   bounded below by a small floor, so a single slow operation can never drive the
   limit to zero and lock the node out, and the floor guarantees forward progress.

4. **Shed at admission with 429.** When in-flight ≥ the current limit, a *new*
   request is refused immediately with **429 Too Many Requests** (a cheap check
   before any coordinator work), with `Retry-After`. 429 is the correct HTTP
   overload semantic and S3 SDKs, rclone, and restic retry it as throttling. This
   is distinct from the existing **503 SlowDown**, which is kept for the
   durability-floor refusal ([ADR-0015](0015-storage-profiles.md)/[ADR-0027](0027-v03-distributed-data-path.md))
   and the non-leader write — "cannot write *safely* right now," a different
   condition from "at capacity." Shedding is always safe: a 429 is retryable and
   never touches durability or a committed object.

5. **Flag degradation separately from load.** Distinguish the two signatures: load
   saturation raises latency *together with* arrival rate, while degradation raises
   the *best-case* `minRTT` at *steady* load. A sustained rise in `minRTT` is
   surfaced as a node health signal (a metric and a candidate `degraded` state in
   `cluster status`) — detection and observability, not automatic action: Hamster
   does not auto-evict on latency, since that could cascade; the operator decides.

The shedder is loop-owned (like the coordinator and the liveness detector), so its
state is deterministic and the simulator drives it directly. PUT and GET may carry
separate limits, since their cost profiles differ (a PUT writes `k+m` shards, a GET
reads `k`); the granularity is an implementation choice the ADR leaves open.

This composes with, and is distinct from, the per-request **backpressure** of
ADR-0038: backpressure *paces an admitted* operation (never an error); admission
control decides *whether to admit* a new one. Backpressure bounds one request's
memory; the shedder bounds the aggregate across all concurrent requests.

## Consequences

- **The node finds its own ceiling and holds there** under overload instead of
  collapsing — whatever the actual bottleneck is, named or not. Clients get a
  clean, retryable 429 and back off; latency stays bounded; memory and open
  shard-streams stay bounded.
- **Early bad-drive detection falls out for free** — the same latency machinery
  flags a degrading resource, a compliance-grade operability signal.
- **The request-latency histograms ship** (the ADR-0035 follow-on), plus the
  adaptive limit, the shed count (by operation and reason), and the degradation
  signal — the load-test dashboard the streaming work was building toward.
- **Fully deterministic and dependency-free** (the no-cgo, permissive-only rules
  hold): everything derives from in-flight count, seam-clock latency, and
  completion events on the loop. The harness proves both behaviours — drive arrival
  rate up and assert the limit shrinks and 429s appear; inject a slow disk and
  assert the degradation signal fires — per invariant 5.
- **A new tunable surface**, kept minimal: the limiter's floor/headroom and the
  degradation sensitivity are configurable (defaulting sensibly), in the spirit of
  the ADR-0038 backpressure window.
- It is **advisory and per-node**: each node sheds on its own observed load, so the
  cluster degrades gracefully without a global coordinator.

## Alternatives considered

- **A flat concurrency or resource threshold.** Rejected: needs per-deployment
  tuning, does not track the moving real ceiling (object-size mix, environment),
  and a CPU/memory threshold is blind to disk and network saturation.
- **Sample OS resources (`/proc`, cgroup limits) behind a seam.** Rejected as the
  primary mechanism: cross-platform cgo-free sampling is hard and
  non-deterministic, and it still cannot see a network ceiling or a noisy
  neighbour. The latency signal captures all of them. OS hints could be an optional
  *additional* input later, never the basis.
- **No admission control — rely on backpressure and timeouts alone.** Rejected:
  backpressure paces each request but does not cap how many run at once, so
  overload still queues unboundedly into a latency explosion and a retry storm.
  Admission control is what prevents the metastable collapse.
- **Token-bucket request-rate limiting (fixed req/s).** Rejected: a rate that is
  fine for small objects overwhelms the node for large ones; a concurrency- and
  latency-based limit is agnostic to load shape and self-calibrating.
