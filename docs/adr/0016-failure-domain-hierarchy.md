# ADR-0016: Failure domains above the node: hosts and zones

## Status

Accepted

## Context

The failure domain is the node ([ADR-0004](0004-partitioned-placement.md)), and the recommended deployment pattern for multi-disk machines is one node process per volume mount. Those two facts collide: a cluster of three servers with five disks each is fifteen nodes, and node-distinct placement is satisfied by layouts that stack five shards of one object on a single server. A server reboot would take objects offline; a server loss would destroy data — while the profile table claims "tolerates 2 nodes." Dense deployments need placement to know which nodes share a machine, and often which machines share a larger blast radius (an availability zone, a rack).

## Decision

Every node carries two labels, stored in its `NodeRecord` (additive fields):

- **`host`** — the machine identity, detected automatically at `node init` (hostname/machine-id). Processes on one machine share it with zero configuration.
- **`zone`** — an operator-supplied label for the failure domain above the machine, defaulting to the host. Free-form: an AWS availability zone (`us-east-1a` — an AZ, not a region), a rack, a room.

Placement rules, in order of force:

1. **Hard invariant, unchanged:** never two shards of one object on the same node. Always enforceable, enforced absolutely.
2. **Spread objective:** distribute each object's shards as evenly as possible across zones, then hosts, then nodes.
3. **Visibility:** `cluster status` reports achieved tolerance at every level (disks, hosts, zones) and states plainly when a level has none — a single-box cluster has one host, and that is reported, not hidden.

One cluster is one region: Raft commits and shard writes are synchronous, so a cluster spans AZs within a region, not regions. Multi-region is a future replication feature between clusters, not a stretched cluster.

## Consequences

- Three servers × five disks at `4+2` places two shards per server: a whole-server loss costs exactly `m=2` shards per object — everything stays readable and repair rebuilds. Without zones this deployment was quietly unsafe; with them it is the design's sweet spot.
- Zero configuration for the common case: host detection alone makes same-machine processes spread correctly. The `zone` flag exists for operators whose blast radius is bigger than a machine.
- Spreading is an objective, not a constraint, above the node level — small or lopsided clusters still place writes, with the shortfall reported rather than refused.
- The layout assignment logic gains a packing objective (even spread across two levels), and the simulation harness gains an invariant: placement never exceeds the achievable spread reported in status. Capacity weighting (planned for the placement release) must balance *within* the spread objective.
- Two levels, not an arbitrary hierarchy. If real deployments want disk < host < rack < AZ depth, levels can be added additively.

## Alternatives considered

- **Node-only placement (the status quo).** Correct for one-node-per-machine clusters, silently dangerous for node-per-disk ones — which the deployment guidance actively recommends. Rejected.
- **Arbitrary CRUSH-style hierarchy (Ceph).** Maximum generality, real complexity in placement, weighting, and the harness's invariant checking. Two levels cover every deployment Hamster targets in v0; depth is additive later. Rejected for v0.
- **Hard constraint at zone level (refuse writes that can't spread).** A single-host homelab could never write at `2+1`. Honest reporting beats refusal where the user's hardware is what it is. Rejected.
- **Zone label only, no automatic host detection.** Makes the most common dense deployment (several processes, one box) depend on the operator remembering a flag per node — and forgetting it recreates the silent stacking this ADR exists to kill. Rejected.
