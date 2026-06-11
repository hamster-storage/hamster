# ADR-0014: Metadata keyspace: version-list truth table, derived current index, partition-indirect shard addressing

## Status

Accepted

## Context

ADR-0005 placed metadata in BadgerDB replicated by Raft, and ADR-0006 required every key to be modeled as an ordered version list. Neither said what the records and keys actually look like. BadgerDB is a flat sorted key-value store, so the schema *is* the key encoding: listing performance, version resolution, and what rebalance must rewrite all fall out of how rows are laid out.

Three questions needed answers before any code:

1. How is the version list physically stored — one row per version, or one record per key holding a list?
2. How does `ListObjects` avoid paying for version history and delete markers?
3. Does a version entry name the nodes its shards live on, or something more indirect?

The full schema is in [METADATA.md](../METADATA.md); this ADR records the three load-bearing choices and what was rejected.

## Decision

- **One BadgerDB row per version** under `v/<bucket>\x00<key>\x00<~version-id>`, where `~version-id` is the bitwise complement of the 16-byte UUIDv7 so versions sort newest-first. The `v/` table is the only truth about objects.
- **A derived current-version index** under `c/<bucket>\x00<key>`, holding denormalized listing fields, present exactly when the key's newest version is a real object. It is maintained in the same transaction as every `v/` change and is mechanically rebuildable from `v/` — the simulation harness checks the equivalence.
- **Shards are addressed by partition, not by node.** A version entry stores its partition ID and EC parameters; the versioned cluster layout (ADR-0004) resolves partition → nodes. Object records are immutable after commit (lock fields excepted, and those only strengthen) and are never touched by repair or rebalance.

## Consequences

- `ListObjects` is a pure scan of `c/`, `ListObjectVersions` a pure scan of `v/`, both already in S3 order — no skip-scans, no sorting, no penalty for version churn.
- Rebalance and repair are layout changes plus data movement; they rewrite zero per-object records. "Shards move, never re-encoded" stays true at the metadata layer too.
- Every read pays one indirection through the layout — small, hot, replicated state on every node. Cheap, and accepted.
- The `c/` index is denormalized state that must be maintained by every transaction touching a key's newest version. The discipline is mechanical (same Badger transaction) and verified by the harness, but it is a standing obligation.
- Object keys may not contain `0x00` (the key-component delimiter) — a documented deviation from AWS, which technically accepts NUL in keys.
- Keys order by bucket then object key, so the future multi-raft split is a range split this layout already accommodates.

## Alternatives considered

- **One record per key with an embedded version array.** Still satisfies the version-list invariant, but the record grows unboundedly with version count, every PUT rewrites the whole history (write amplification on the hottest path), and listing a million keys' current versions means deserializing a million histories. Rejected.
- **No current index — resolve current by seeking the first `v/` row per key.** Correct and simpler, but `ListObjects` becomes a skip-scan whose cost scales with version and delete-marker churn rather than with live keys. Listing is the operation S3 clients hammer. Rejected.
- **Explicit shard locations (node IDs) in each version entry.** Self-contained reads with no layout lookup, but every repair and every rebalance must rewrite the metadata of every affected object — millions of Raft proposals for one node replacement, and a contradiction of the immutable-record principle. Rejected.
- **Timestamp-based version sort keys instead of complemented UUIDv7.** Redundant (UUIDv7 already encodes time, ADR-0007) and introduces a second ordering authority that can disagree with version IDs. Rejected.
