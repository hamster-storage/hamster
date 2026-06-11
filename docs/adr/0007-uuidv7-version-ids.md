# ADR-0007: UUIDv7 for version IDs

## Status

Accepted (amended 2026-06-11: IDs are minted in-repo from explicit inputs; the `google/uuid` dependency is dropped)

## Context

Every object version needs a unique ID ([ADR-0006](0006-versioning-and-object-lock.md)). The choice affects more than uniqueness:

- Version lists are read in chronological order (`ListObjectVersions`, resolving "latest"). If IDs sort by creation time, the metadata index gives chronological order for free.
- Random IDs (UUIDv4) scatter inserts across the keyspace; time-ordered IDs cluster writes, which is kinder to an LSM store like BadgerDB ([ADR-0005](0005-metadata-badgerdb-raft.md)).
- RFC 9562 standardized **UUIDv7**: a 48-bit Unix millisecond timestamp followed by random bits — time sortable, standard, and collision resistant.

One sharp edge: the timestamp has millisecond precision, so two versions created in the same millisecond need a tiebreaker or their order is random.

## Decision

**Version IDs are UUIDv7**, minted in-repo (`meta.NewVersionID`) from explicit inputs: a clock reading and a PRNG, supplied by the caller. The RFC 9562 layout is about twenty lines of standard library code.

- The original decision named `google/uuid`'s `NewV7()`. It was dropped before any code shipped: `NewV7` reads the ambient wall clock and global `crypto/rand`, and the determinism convention (CLAUDE.md, [ADR-0009](0009-deterministic-simulation-testing.md)) requires that anything touching time or randomness take them as inputs the simulator controls. Wrapping the library to inject both is more code than the generator itself.
- The ID is kept as a **`[16]byte`** value (`meta.VersionID`) internally, so a future standard library uuid type remains a small swap if ever wanted.
- **Monotonicity is handled at apply time**: if a proposal's ID does not sort after the key's newest version, apply increments it as a 128-bit value ([METADATA.md](../METADATA.md), "commit order beats clock order"). This subsumes the intra-millisecond tiebreak concern — same-millisecond collisions, skewed clocks, and duplicate mints all resolve to strict per-key ordering by Raft commit. A bumped ID may no longer decode as a valid UUIDv7 timestamp; version IDs are opaque ordered values that start life as UUIDv7, which is why `created_unix_ms` is stored explicitly.

## Consequences

- Chronological ordering in the version index comes free from byte order — no separate timestamp column or secondary sort.
- Write locality improves in BadgerDB: new versions land near each other in the keyspace.
- Ordering across nodes is only as good as clock sync; the authoritative order of concurrent same-key writes is decided by the metadata transaction in Raft ([ADR-0005](0005-metadata-badgerdb-raft.md)), not by comparing IDs. UUIDv7 ordering is an index property, not a distributed-consistency mechanism.
- Version IDs leak coarse creation timestamps. Acceptable for an object store, where last-modified is exposed anyway.
- No dependency at all: the generator is pure standard library, and determinism under simulation comes for free because the inputs are explicit.

## Alternatives considered

- **UUIDv4 (random).** Maximum simplicity, but random index placement (poor locality) and no chronological ordering — every version list needs an explicit timestamp sort. Rejected.
- **ULID.** Same shape as UUIDv7 (48-bit time + randomness) but predates the RFC; UUIDv7 wins on standardization and the forthcoming standard library support. Rejected.
- **Snowflake-style IDs.** Compact (64-bit) and sortable, but requires coordinated worker-ID assignment — operational machinery for little gain. Rejected.
- **Monotonic per-key counters (v1, v2, …).** Trivially ordered but globally coordinated per key, and guessable IDs deviate from S3-style opaque version IDs. Rejected.
