# ADR-0015: Storage profiles: auto-by-default profile policy, per-object parameters, replication as k=1

## Status

Accepted (amended: the profile policy was originally explicit-only; design review made `auto` the default — see Alternatives)

## Context

ADR-0003 chose erasure coding and ADR-0013 chose the library, but neither said which `k+m` values exist, who picks them, or what they mean for the deployments Hamster actually targets — which start at one node and grow. Five questions needed answers before the write path can be coded:

1. Are EC parameters cluster-wide, per-bucket, or per-object?
2. Who chooses them — the operator, or the cluster from its own shape?
3. What happens to existing data when a cluster grows and the profile improves?
4. What do small objects do, where per-fragment overhead makes EC counterproductive?
5. How many shards must be durable before a PUT is acknowledged?

The full design is in [ERASURE-CODING.md](../ERASURE-CODING.md); this ADR records the decisions and the rejected alternatives.

## Decision

- **A small set of named, tested profiles** (`1+0`, `1+1`, `2+1`, `3+2`, `4+2`), each shipped with its own simulation failure schedules. The cluster has **one active profile**, stored in `ClusterConfig`.
- **The profile policy defaults to `auto`**: the cluster follows the node-count ladder (1 → `1+0`, 2 → `1+1`, 3–4 → `2+1`, 5 → `3+2`, 6+ → `4+2`), re-deriving **only when an explicit membership command commits** (`cluster join` / `cluster remove-node`) — never on health changes — and announcing every transition. Operators who want immovable parameters pin (`set-profile 4+2`); under a pin, a removal that would make the profile unplaceable is refused.
- **Every object records its own `k`/`m`** in its `VersionEntry` ([ADR-0014](0014-metadata-keyspace-design.md)). A profile change is a layout transition: new writes use the new profile; existing objects keep their recorded parameters and their shards move without re-encoding. **Bringing existing data up to the active profile is a repair task** (re-encode), part of the one repair system rather than a separate operator command; until it ships, `cluster status` reports data sitting below the active profile.
- **Small objects (below a threshold, 128 KiB to start) are written as `1+m`** — Reed-Solomon with one data shard, which is full replication. Same placement, repair, metadata, and invariants as every other object; no second storage path.
- **Acknowledgment rule:** all `k+m` shards durable on the healthy path; shards targeted at known-down nodes may be skipped with a hard floor of `k+1` durable, with repair queued at ack; below the floor the write is refused. Every acknowledged write tolerates at least one node loss immediately and `m` after repair.

## Consequences

- Growth requires no remembered commands: join nodes, the ladder steps up, repair (once re-encode ships) brings old data along. The bolt-on-replication story — one node today, mirror tomorrow — works as users expect.
- Nothing is silent: profile transitions only follow operator-issued membership commands, are announced at the command, and persist in `cluster status`.
- Single-node and small clusters are first-class, honestly labeled modes (`1+0` has zero redundancy; `1+1` mirrors data but halts all API calls when either node is down).
- Until re-encode ships, data written under a weaker profile stays at that durability — visible operator-facing state, not a silent promise gap.
- The ack floor means small profiles (`2+1`, `1+1`) need every node up to accept writes — the truthful cost of small clusters.
- The simulation harness must track each object's budget at ack (`durable − k`) instead of assuming `m` — reflected in SIMULATION.md invariant 1.

## Alternatives considered

- **Explicit `set-profile` as the only mode (this ADR's original decision).** Rejected on review: every growth step requires a second, forgettable command, and forgetting it leaves new data silently at weak durability — precisely the silent gap explicitness was meant to prevent. The original rejection of automation conflated "automatic" with "silent": membership only changes by operator command, so a ladder step following `cluster join` is announced, attributable, and operator-initiated. Explicit pinning is retained for operators who want it.
- **Fully implicit parameters with no pinning (pure MinIO-style).** The operator can never state "these parameters, exactly, do not move." Rejected; `auto` plus pinning covers both temperaments.
- **Per-bucket profiles (storage classes) in v0.** The metadata schema already supports it — parameters are per-object — but it multiplies the tested configuration matrix for no v0 user need. Deferred, not foreclosed.
- **Inlining small objects into metadata.** The classic small-object fix, and categorically unavailable here: object data would ride the Raft log, violating the first critical invariant. Rejected without appeal.
- **A separate replication code path for small objects.** Doubles the surface the simulation harness must cover; `k=1` erasure coding is byte-for-byte the same thing through one path. Rejected.
- **A standalone "re-protect" command for upgrading existing data.** A second maintenance verb for operators to learn, forget, and schedule. Re-encode is repair work by any reasonable definition of repair ("make reality match the target"), so it lives in repair's queue and runs itself. Rejected.
- **Strict write-all acknowledgment (no degraded floor).** One down node in a 6-node `4+2` cluster would block every write in the cluster. Rejected for availability.
- **Acknowledging at `k` durable shards.** An acked object with zero loss tolerance until repair runs is a durability lie at the API boundary. Rejected.
