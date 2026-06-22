# ADR-0037: Proposal forwarding — any node accepts writes

## Status

Accepted. A technical enabler of [ADR-0036](0036-one-clustered-path.md) (one
clustered path); ships in the same release (v0.11).

## Context

On the v0.3 cluster S3 path, writes are **leader-only**: a non-leader node answers
a mutating request with `503 SlowDown` and the client retries elsewhere
([`internal/cluster/serve.go`](../../internal/cluster/serve.go)). This was a
deliberate v0.3 *preview* simplification — "v0.3 does not forward proposals" — not
a formal decision to never forward.

[ADR-0036](0036-one-clustered-path.md) makes S3 serve on **every** node by default.
A write that lands on a non-leader should then succeed, not bounce: vanilla S3 SDKs
have no idea which node holds Raft leadership, so a `503`-and-retry model leaks
cluster topology into every client and breaks the "any node is an S3 endpoint"
promise.

A Hamster write is **not** a single Raft proposal. It is data-plane work —
placement, erasure coding, shard transfer to `k+m` nodes — followed by **one small
metadata commit** (the `PutObject` proposal). Object bytes must never cross the
leader hop or enter the Raft log (invariant 1). So "forwarding" here cannot mean
proxying the request to the leader.

## Decision

1. **The receiving node coordinates the data plane locally.** Placement, erasure
   coding, and shard transfer need no leadership — any node runs the existing
   `coord` PUT through shard durability exactly as a leader would, fanning shards
   out to `k+m` nodes and enforcing the ack rule.

2. **Only the metadata commit forwards.** Once the shards are durable, a non-leader
   sends the prepared `PutObject` proposal (carrying every apply input, including
   the client-minted version ID) to the leader over the existing mTLS control
   channel. The leader proposes it through Raft and returns the committed result
   (final version ID, ETag, replaced data IDs); the receiving node completes the S3
   response from that. The bytes stay where they were written; only the small
   commit crosses the hop.

3. **The forward is idempotent and leadership-blip-tolerant.** The node already
   tracks the leader (status redirects use it). A leadership change mid-forward is a
   retry: re-resolve the leader, re-send the commit. The proposal is keyed on the
   minted version ID and the metadata apply is deterministic, so a duplicate forward
   applies once.

4. **Reads are unchanged** — still served from the local replica; linearizable
   `ReadIndex` reads remain separate, later work. This ADR is the write path only.

`503 SlowDown` stays for genuine backpressure and the below-floor durability refusal
([ADR-0027](0027-v03-distributed-data-path.md) decision 5) — those are distinct from
"this node is not the leader," which no longer produces a `503`.

## Consequences

- **Every node is a full S3 endpoint for reads and writes** — the fleet is uniformly
  load-balanceable behind any round-robin, which is exactly what ADR-0036's
  S3-on-every-node default requires.
- **The leader still serializes every commit** — the linearization point is
  unchanged. Forwarding moves only the tiny commit there, never the bytes, so leader
  CPU and bandwidth stay negligible.
- A non-leader PUT pays **one extra network round-trip for the commit** (not the
  body, which already fanned out to `k+m` nodes). Acceptable.
- **New crash/leadership coverage** under the simulation harness: a forward in flight
  when leadership changes, duplicate forwards, and a receiver crash after the shards
  are durable but before the commit (the next client retry re-drives; orphan shards
  are GC'd). These schedules merge per invariant 5.

## Alternatives considered

- **HTTP-proxy the whole S3 request to the leader.** Rejected: streams the object
  body twice (client → follower → leader), couples front-end throughput to leader
  bandwidth, and routes bytes through the leader hop for no benefit. Forwarding only
  the commit keeps bytes off the leader and off Raft.
- **Use etcd-raft's built-in proposal forwarding.** Rejected: that forwards the Raft
  message, but a Hamster write is data-plane-then-commit — the data plane must run on
  the receiving node regardless, and we want explicit control over the commit's
  idempotency and the S3 response plumbing.
- **Keep leader-only writes; put a leader-aware proxy or smart client in front.**
  Rejected: pushes Raft-topology awareness onto every client and breaks the
  any-node-is-an-endpoint promise. Standard S3 SDKs cannot know the leader.
