# ADR-0005: Metadata in BadgerDB replicated by Raft, with object data outside the log

## Status

Accepted

## Context

An object store handles two very different kinds of state:

- **Metadata** — bucket configuration, the key namespace, version lists, and the object→shard mapping. Small records, but consistency critical: losing or mis-ordering them loses objects.
- **Object data** — the bytes themselves, erasure coded into shards ([ADR-0003](0003-erasure-coding-over-replication.md)). Large, but immutable once written.

A tempting simplification is to push everything through one replicated log. But consensus replicates by copying: every byte through Raft is written to a quorum of logs and then to state machines — bandwidth-expensive triple-writing of data that erasure coding already protects at `(k+m)/k` overhead. Log-shipping gigabytes also turns the consensus group into the throughput ceiling for every upload.

The single-binary decision ([ADR-0002](0002-single-binary-no-external-dependencies.md)) rules out external databases and coordination services, so both stores must be embedded.

## Decision

Metadata and object data take **separate paths**:

- **Metadata** is stored in **BadgerDB** (embedded, pure Go, key-value) on each participating node and replicated via **Raft** for strong consistency. **v0 uses a single Raft group** for all metadata to prove the system. Later, metadata shards across many partitions, each with its own Raft group (**multi-raft**), for scale — an additive evolution.
- **Object data never passes through the Raft log.** Shards are written directly to the chosen nodes' data directories. Durability comes from the EC spread, not from consensus.
- **A PUT** encodes the object into shards, writes the shards directly, and once a quorum of shards is durable, **atomically commits one small metadata record** (key, version, size, checksums, shard locations) through Raft. That commit is the linearization point: until it lands, the shards are invisible garbage; after it, the object durably exists.

Consistency model: metadata is strongly consistent (Raft + quorum). Objects are immutable blobs with no in-place edits, which removes most conflict cases; same-key overwrites and the current-version pointer resolve inside the metadata transaction.

## Consequences

- Upload bandwidth scales with the data path (parallel direct shard writes), not with consensus throughput. The Raft log stays small and fast.
- The commit-after-quorum protocol yields clean failure semantics: a crashed PUT leaves orphaned shards (garbage to collect), never a dangling metadata record pointing at missing data.
- We need shard garbage collection for uncommitted writes — a permanent background process.
- The single v0 Raft group caps metadata throughput; accepted to prove the system first. The multi-raft evolution is planned, not improvised.
- BadgerDB and Raft become load bearing embedded dependencies whose failure behavior we own — the simulation harness ([ADR-0009](0009-deterministic-simulation-testing.md)) must exercise both under fault injection.

## Alternatives considered

- **All data through the Raft log.** Maximum simplicity of reasoning, unacceptable write amplification and a consensus-shaped bottleneck on every upload. Rejected.
- **External metadata store.** Violates ADR-0002. Rejected.
- **No consensus — eventually consistent metadata with CRDTs** (Garage's direction). Operationally simple and AP-friendly, but versioning, object lock, and a current-version pointer want linearizable metadata; bolting strong semantics onto eventual consistency is harder than starting strong. Rejected.
- **Other embedded stores (bbolt, Pebble, SQLite).** bbolt's single-writer model and SQLite's relational layer fit less well than an LSM key-value store for write-heavy metadata; Pebble is strong but BadgerDB is pure Go with an API that matches the need. Revisitable behind the storage interface without changing this ADR's substance.
- **Multi-raft from day one.** Sharded metadata is where the design ends up, but starting there multiplies coordination bugs before a single PUT works. Rejected for v0.
