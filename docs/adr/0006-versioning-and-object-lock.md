# ADR-0006: First class versioning and object lock

## Status

Accepted

## Context

Versioning and object lock (WORM retention) are the features that separate "compatible enough for dev" from "usable for backups and compliance." They are also exactly what the lightweight alternatives skip — Garage supports neither — which makes them part of Hamster's reason to exist.

The dangerous way to build versioning is to start with one metadata record per key and graft version history on later. That retrofit is a schema migration through the most consistency-critical data in the system, performed on live clusters. The S3 semantics make the trap concrete: versioning interacts with delete markers, `ListObjectVersions`, and the null-version behavior of unversioned buckets, and object lock attaches retention to *specific versions* — none of which fit a single-record model.

## Decision

**Versioning is first class in the metadata model from day one.** Every key is an ordered list of versions — even when versioning is disabled and the list holds a single entry. Enabling versioning on a bucket is a state change, never a schema migration. The model includes delete markers, version IDs ([ADR-0007](0007-uuidv7-version-ids.md)), and `ListObjectVersions`.

**Object lock and WORM are first class**: retention in **GOVERNANCE** and **COMPLIANCE** modes, plus legal holds. Object lock requires versioning, because a lock applies to a specific version.

The hard rule: **COMPLIANCE mode is enforced even against administrators, with no override path.** No flag, no root credential, no support escape hatch. GOVERNANCE mode is the bypassable tier (with the appropriate permission); COMPLIANCE is not bypassable by anyone. Any code path that can delete or shorten retention on a COMPLIANCE-locked version is a bug, full stop.

## Consequences

- The unversioned fast path pays a small constant tax (a one-element list instead of a bare record). Cheap insurance against a live migration later.
- Versioned buckets accumulate versions; lifecycle expiration of noncurrent versions becomes necessary follow-on work.
- COMPLIANCE-with-no-override is a one-way door by design — a mis-set retention date means the data stays until it expires. That is the feature working as specified, and the docs must say so loudly.
- "No override path" is a property of the whole codebase, not one check: every delete/overwrite path must consult retention state inside the metadata transaction. This is one of the invariants in CLAUDE.md, and the simulator should include lock semantics in its failure schedules.
- Immutable shards ([ADR-0005](0005-metadata-badgerdb-raft.md)) make WORM natural: locking a version means refusing metadata transitions, since the bytes were never mutable anyway.

## Alternatives considered

- **Single record per key, versioning later.** Less metadata machinery in v0, but the retrofit is a live schema migration through consistency-critical state — the exact class of risk this project exists to avoid. Rejected.
- **GOVERNANCE mode only.** Avoids the scary one-way door, but COMPLIANCE is what regulated backup use cases actually require, and an "admin can always fix it" lock is not WORM. Rejected.
- **An administrative break-glass for COMPLIANCE.** Operationally tempting, legally self-defeating: an override path means the retention guarantee cannot be attested. S3's own semantics allow no such path. Rejected without reservation.
