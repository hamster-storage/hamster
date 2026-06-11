# ADR-0003: Erasure coding over replication for object durability

## Status

Accepted

## Context

An object store must survive disk and node loss. The two standard mechanisms:

- **Replication**: store N full copies. Simple, fast to read and repair, but storage overhead is N× — 3× for the common triple replication.
- **Erasure coding**: split an object into `k` data shards plus `m` parity shards (Reed Solomon). Any `k` shards reconstruct the object; overhead is `(k+m)/k`. A 4+2 scheme costs 1.5× and tolerates two simultaneous losses — cheaper than 3× replication with comparable or better protection.

Hamster targets self hosters, for whom storage cost is often the deciding factor. "Durable by default" only matters if people can afford to turn it on. Garage chooses replication for simplicity; that is part of the gap Hamster aims at.

## Decision

Object durability comes from **Reed Solomon erasure coding**: `k` data shards plus `m` parity shards per object, with overhead `(k+m)/k`.

- Shards spread across **independent failure domains**, and **the failure domain is the node, not the disk** — no two shards of one object on the same node, so a node loss costs at most one shard per object.
- **Self healing reconstruction** rebuilds lost shards onto healthy capacity as a background process, from v0.
- **Background scrubbing** against bit rot is a later sophistication — planned, not in the initial release.

## Consequences

- Storage stays cheap without giving up safety, which is the README's core durability promise.
- Reads need any `k` shards, so the cluster serves reads while degraded, up to `m` losses per object.
- EC is more complex than replication: encode/decode on the data path, shard placement constraints, and reconstruction logic. This complexity is exactly what the deterministic simulation harness ([ADR-0009](0009-deterministic-simulation-testing.md)) exists to keep honest.
- A minimum cluster size follows from the math: you need at least `k+m` nodes to spread shards across node-level failure domains. Small/single-node deployments need an EC profile sized to fit.
- Repair traffic on node loss reads `k` shards to rebuild each lost one — more network than copying a replica. Acceptable for the target scale.

## Alternatives considered

- **Triple replication.** Simpler in every way, and the right call for Garage's goals. But 3× overhead contradicts cheap-but-safe, and replication-only is precisely the feature set already available. Rejected as the primary mechanism.
- **Replication for small objects, EC for large.** A real optimization (shard-per-tiny-object overhead is real), but two durability paths from day one doubles the surface the simulator must cover. Possible later as an additive optimization; rejected for v0.
- **Disk-level failure domains.** Spreading shards across disks within fewer nodes allows smaller clusters, but a single node loss can then take multiple shards of one object — quietly weakening the durability promise. Rejected: the domain is the node.
