# ADR-0007: UUIDv7 for version IDs

## Status

Accepted

## Context

Every object version needs a unique ID ([ADR-0006](0006-versioning-and-object-lock.md)). The choice affects more than uniqueness:

- Version lists are read in chronological order (`ListObjectVersions`, resolving "latest"). If IDs sort by creation time, the metadata index gives chronological order for free.
- Random IDs (UUIDv4) scatter inserts across the keyspace; time-ordered IDs cluster writes, which is kinder to an LSM store like BadgerDB ([ADR-0005](0005-metadata-badgerdb-raft.md)).
- RFC 9562 standardized **UUIDv7**: a 48-bit Unix millisecond timestamp followed by random bits — time sortable, standard, and collision resistant.

One sharp edge: the timestamp has millisecond precision, so two versions created in the same millisecond need a tiebreaker or their order is random.

## Decision

**Version IDs are UUIDv7**, generated via `google/uuid` (`uuid.NewV7()`).

- The ID is kept as a **`[16]byte`** value internally, so when the Go standard library ships its uuid type, adopting it is a one line swap.
- **Intra-millisecond monotonicity is handled explicitly** — same-millisecond writes on a node must still produce strictly increasing IDs (RFC 9562 anticipates this with its monotonic counter methods), so ordering within the index never depends on luck.

## Consequences

- Chronological ordering in the version index comes free from byte order — no separate timestamp column or secondary sort.
- Write locality improves in BadgerDB: new versions land near each other in the keyspace.
- Ordering across nodes is only as good as clock sync; the authoritative order of concurrent same-key writes is decided by the metadata transaction in Raft ([ADR-0005](0005-metadata-badgerdb-raft.md)), not by comparing IDs. UUIDv7 ordering is an index property, not a distributed-consistency mechanism.
- Version IDs leak coarse creation timestamps. Acceptable for an object store, where last-modified is exposed anyway.
- The `google/uuid` dependency is small, maintained, and quarantined behind the `[16]byte` representation.

## Alternatives considered

- **UUIDv4 (random).** Maximum simplicity, but random index placement (poor locality) and no chronological ordering — every version list needs an explicit timestamp sort. Rejected.
- **ULID.** Same shape as UUIDv7 (48-bit time + randomness) but predates the RFC; UUIDv7 wins on standardization and the forthcoming standard library support. Rejected.
- **Snowflake-style IDs.** Compact (64-bit) and sortable, but requires coordinated worker-ID assignment — operational machinery for little gain. Rejected.
- **Monotonic per-key counters (v1, v2, …).** Trivially ordered but globally coordinated per key, and guessable IDs deviate from S3-style opaque version IDs. Rejected.
