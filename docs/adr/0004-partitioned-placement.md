# ADR-0004: Partitioned placement with a versioned layout, not fixed pools

## Status

Accepted. First implementation step landed in v0.4 pass 1: the layout is now a
stored, versioned, Raft-replicated `ClusterLayout` record that placement reads
from ([ADR-0028](0028-stored-cluster-layout.md)). That pass stores the member
set and derives per-partition assignments by rendezvous; the explicit
old→new transition tracking and incremental rebalance this ADR describes are
the remaining v0.4 passes.

## Context

An object store needs a placement function: given an object, which nodes hold its shards? The choice determines what happens when capacity is added — the moment where many designs hurt.

MinIO's model used fixed **server pools**: capacity is added in pre-sized pool units, and a pool's geometry is frozen at creation. That makes growth lumpy and planning-heavy — the opposite of "grows smoothly." At the other extreme, pure consistent hashing over nodes reshuffles large fractions of data on every membership change.

The middle path, used by Ceph (placement groups) and many Dynamo-style systems (vnodes), is an indirection layer: hash objects to a fixed set of partitions, then assign partitions to nodes.

## Decision

Placement is **consistent hashing over a fixed, generously overprovisioned partition count** — a few thousand — that is **never resized**. The assignment of partitions to nodes is the **cluster layout**, which is versioned state:

- Adding a node creates a new layout version that reassigns some partitions to it. Rebalancing migrates those partitions' shards incrementally. **Objects are never re-encoded** during rebalance; shards only move.
- The layout carries old→new transition tracking, so reads can find data that is mid migration by consulting both assignments.
- **v0 starts simple**: equal weight nodes and manually triggered rebalance. Weighted heterogeneous capacity and automatic rebalancing come later as additive features within the same model — only the assignment policy changes, never the partition abstraction.

## Consequences

- Growth is one node at a time, and the amount of data moved is proportional to the capacity added — no lumpy pool-sized increments, no mass reshuffle.
- The partition count is a permanent constant; choosing it generously up front (a few thousand) is a one-time decision that bounds maximum useful cluster size. Overprovisioning is cheap because empty partitions cost almost nothing.
- "Never resized" removes an entire class of bugs (split/merge of partitions) at the cost of that fixed ceiling. Accepted.
- The layout becomes consistency-critical metadata: it lives in the Raft-replicated store ([ADR-0005](0005-metadata-badgerdb-raft.md)) and its versioning rules follow the format-evolution rules ([ADR-0008](0008-versioned-formats-rolling-upgrades.md)).
- Mid-migration reads are a permanent code path, not an edge case — the simulator must exercise rebalance under failure from the start.

## Alternatives considered

- **Fixed pools (MinIO style).** Operationally predictable but rigid: capacity arrives in pool-shaped chunks and geometry mistakes are forever. Contradicts "add a node, data redistributes." Rejected.
- **Consistent hashing directly over nodes.** No indirection layer to maintain, but membership changes move data immediately and uncontrollably, and there is no natural unit for tracking migration state. Rejected.
- **Resizable partitions (split/merge).** Removes the fixed ceiling, but split/merge during failures is among the hardest distributed-systems code to get right. Overprovisioning buys the same headroom for free. Rejected.
- **Weighted placement and auto-rebalance in v0.** Desirable, but each multiplies the failure schedules the simulator must cover before the core is trusted. Deferred — the model accommodates both additively.
