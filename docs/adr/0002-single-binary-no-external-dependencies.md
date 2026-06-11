# ADR-0002: Single binary with no external service dependencies

## Status

Accepted

## Context

Hamster's thesis is that object storage should be simple to run. The audience is self hosters and small teams without a platform group: the operating cost of every additional moving part — a coordination service, a metadata database, a sidecar — is paid by the person running the system, forever. SeaweedFS demonstrates the capable-but-many-parts end of the spectrum; Hamster wants the other end without giving up durability features.

The components a distributed object store typically outsources are consensus/coordination (ZooKeeper, etcd) and metadata storage (an external database). Both can instead be embedded in the process: Go has mature embedded options for each.

## Decision

Hamster ships as a **single static Go binary with no external service dependencies**. No ZooKeeper, no etcd, no separate database. Consensus is embedded Raft; metadata storage is embedded BadgerDB ([ADR-0005](0005-metadata-badgerdb-raft.md)). Every node runs the same binary; a cluster is just nodes pointed at each other.

A corollary that is policy, not preference: **no cgo, anywhere**. Hamster builds with `CGO_ENABLED=0`, and every dependency must be pure Go (Go assembly is fine). cgo is what quietly turns a "static" Go binary into one with libc and toolchain entanglements, complicates cross-compilation, and adds a class of crashes the Go runtime cannot see into. The library choices already reflect this — BadgerDB and `etcd-io/raft` are pure Go, and `klauspost/reedsolomon` gets its SIMD performance from Go assembly rather than C.

## Consequences

- Operating Hamster is operating one process per node. Install, upgrade, and backup stories stay simple, and the quick start can honestly be two commands.
- Go is effectively mandated by this decision: static binaries, easy cross compilation, and a mature ecosystem of embeddable storage and Raft libraries.
- We own the embedded consensus and storage layers' operational behavior (compaction, snapshots, recovery) instead of delegating them to a battle tested external service. That raises our testing burden — one reason the simulation harness ([ADR-0009](0009-deterministic-simulation-testing.md)) is foundational rather than optional.
- The dependency budget is small on principle: every library we embed is something users implicitly operate.

## Alternatives considered

- **External coordination (ZooKeeper/etcd).** Battle tested, but it doubles the operational footprint and contradicts the project's reason to exist. The MinIO-shaped gap is for software that is simple to run. Rejected.
- **External metadata database (PostgreSQL etc.).** Strong consistency for free, but now the object store's durability depends on a database the user must separately deploy, replicate, and back up. Rejected.
- **Multiple cooperating binaries** (separate gateway, metadata, and storage daemons, as SeaweedFS does). More flexible to scale per role, but the complexity lands on the operator. A single binary can still run with roles enabled or disabled later if scale demands it — that door stays open without shipping three daemons on day one. Rejected for v0.
