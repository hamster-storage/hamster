# ADR-0019: MD5 ETags for compatibility, with integrity carried by internal checksums

## Status

Accepted

## Context

S3's `ETag` has de-facto semantics that tooling depends on: for a single-part upload it is the MD5 of the object body; for a multipart upload it is the MD5 of the concatenated part MD5s, suffixed with `-<part count>`. rclone uses it to verify transfers and detect changes; restic and other sync tools rely on it. An S3-compatible store that returns opaque or differently-computed ETags breaks those tools *silently* — they fall back to re-transferring or, worse, mis-detect equality.

MD5 is cryptographically broken, which is irrelevant to one use and disqualifying for another: fine as a change-detection fingerprint for cooperating clients, unacceptable as the store's integrity mechanism.

This was the ETag question parked in [METADATA.md](../METADATA.md); this ADR resolves it.

## Decision

- **ETags follow S3's de-facto semantics exactly**: MD5 of the body for single-part PUTs (computed during the write-buffer pass, no extra read), composite `MD5(MD5(part1)‖…)-N` for multipart. Stored in the `VersionEntry.etag` field.
- **Integrity is internal and separate**: the whole-object and per-shard checksums in the metadata record (a modern algorithm, chosen with the v0.1 code) are what reads, repair, and scrub verify against. MD5 is a wire-format obligation, never trusted for correctness.
- **`Content-MD5`**, when a client supplies it, is validated and the PUT rejected on mismatch — free end-to-end integrity on the upload hop.
- The **`x-amz-checksum-*` family** (SHA-256, CRC32C) arrives later, additively, for clients that want verifiable strong checksums over the wire.

## Consequences

- rclone, restic, the CLI, and SDK transfer managers verify uploads and sync correctly with no special configuration — the practical definition of S3 compatibility.
- Hamster computes two hashes on the upload path (MD5 for the ETag, the internal checksum for integrity). Both stream through the write buffer in one pass; the cost is CPU cycles, not I/O.
- MD5's weakness never touches durability: a corrupted or tampered shard is caught by the internal checksums regardless of any MD5 collision.
- Multipart ETags are not content hashes (they depend on part boundaries) — true on AWS too; tools already know.

## Alternatives considered

- **Opaque ETags (random or version-ID derived).** Legal by the letter of HTTP, breaks the sync-tool ecosystem in practice. Rejected.
- **A modern hash (SHA-256, BLAKE3) as the ETag.** Honest cryptography, wrong layer: clients that compare ETags against locally computed MD5s see permanent mismatch. Rejected.
- **MD5 as the internal integrity mechanism too.** One hash instead of two, but the store's own correctness would rest on a broken function. Rejected — compatibility and integrity are different jobs and get different tools.
