# CLAUDE.md

Hamster is a self hosted, S3 compatible object store in a single Go binary, built around erasure coded durability and first class versioning and object lock.

**Positioning:** the target user runs real workloads with compliance-shaped needs — retention, object lock, audits (HIPAA, SEC 17a-4 territory) — without a platform team. Design and performance work should serve that user: use the hardware well and be plenty fast, but never trade durability, compliance correctness, or operational simplicity for benchmark wins. Small-file microbenchmark supremacy is explicitly not a goal. In public-facing docs, name MinIO's community-edition archive as the factual origin story, but do not name or disparage other open source stores — generalize.

> **Status: early development (v0). Not production ready.** Nothing described in the docs is a guarantee yet; it is the design being built. Keep all documentation honest about this — features are goals until they ship and survive the simulation harness.

## Critical invariants — never violate these

These are the load bearing design decisions. Code or docs that break them are wrong even if tests pass.

1. **Object data never passes through the Raft log.** Erasure coded shards are written directly to storage nodes. Only the small metadata commit (key, version, size, checksums, shard locations) goes through Raft. Durability comes from the EC spread, not from consensus. See [ADR-0005](docs/adr/0005-metadata-badgerdb-raft.md).
2. **All on disk and on wire formats are additively versioned protobuf.** Every persistent or networked structure carries a version field. Fields are only ever added — never removed, never repurposed. New code must always read old formats. See [ADR-0008](docs/adr/0008-versioned-formats-rolling-upgrades.md).
3. **Metadata models every key as an ordered list of versions**, never a single record per key — even when versioning is disabled and the list holds one entry. Enabling versioning must never require a schema migration. See [ADR-0006](docs/adr/0006-versioning-and-object-lock.md).
4. **COMPLIANCE mode object lock has no override path.** Not for administrators, not for operators, not behind a flag. If a code path can delete or shorten retention on a COMPLIANCE locked version, it is a bug. GOVERNANCE mode is the bypassable one; COMPLIANCE is not.
5. **Durability and consistency changes must pass the deterministic simulation harness.** Any change touching the write path, repair, placement, Raft, or formats must survive the simulated failure schedules before it merges. See [ADR-0009](docs/adr/0009-deterministic-simulation-testing.md).
6. **Objects are immutable blobs.** Written once, never edited in place. Overwrites create a new version; the current version pointer resolves inside the metadata transaction.
7. **The partition count is fixed and never resized.** Rebalancing migrates partitions between nodes; it never re-encodes objects. See [ADR-0004](docs/adr/0004-partitioned-placement.md).
8. **The failure domain is the node, not the disk.** Shard placement must never put two shards of one object on the same node. Hosts and zones group nodes above that ([ADR-0016](docs/adr/0016-failure-domain-hierarchy.md)); spreading across them is a placement objective — the node rule is the hard floor.

## Go conventions

- Standard Go style: `gofmt`, `go vet`, idiomatic naming. Exported identifiers get doc comments.
- Errors are wrapped with context (`fmt.Errorf("...: %w", err)`) and checked, not ignored.
- No global mutable state; pass dependencies explicitly so the simulation harness can substitute clocks, networks, and disks.
- Anything that touches time, randomness, the network, or the filesystem goes behind an interface that the simulator can control. Determinism is a feature, not a test affordance bolted on later.
- Version IDs are UUIDv7 via `google/uuid` (`NewV7`), kept as `[16]byte` so the future standard library uuid type is a one line swap. See [ADR-0007](docs/adr/0007-uuidv7-version-ids.md).
- Prefer the standard library. Every dependency must justify itself — the single binary, no external services promise extends to keeping the module graph small.
- **No cgo, anywhere.** Hamster builds with `CGO_ENABLED=0`, always. Dependencies must be pure Go — a library that requires cgo is disqualified no matter how good it is. (Go assembly is fine: that is how `klauspost/reedsolomon` gets its SIMD speed.) This is what keeps the binary truly static and cross-compilation trivial; see [ADR-0002](docs/adr/0002-single-binary-no-external-dependencies.md).
- **Dependency licensing: permissive only.** Any imported package must be Apache 2.0 or similarly permissive (MIT, BSD, ISC), including transitive dependencies. No copyleft (GPL, LGPL, MPL) and no source-available licenses (BUSL, SSPL). Exceptions require an ADR. See [ADR-0011](docs/adr/0011-permissive-only-dependencies.md).

## Build, test, and lint

Nothing is built yet. Placeholders to fill in when the Go module lands:

```sh
# build:  TBD (will be `go build ./...` or a thin Makefile wrapper)
# test:   TBD (unit tests plus the simulation harness)
# lint:   TBD (gofmt, go vet, likely golangci-lint)
```

Until then, this repository is documentation only. Do not add Go files, `go.mod`, or build scaffolding unless an issue asks for it.

## Where the design lives

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — the system design narrative: request paths, metadata/data separation, erasure coding, placement, upgrades, testing.
- [`docs/SIMULATION.md`](docs/SIMULATION.md) — the testing strategy design: the deterministic simulation harness, the interface seam, fault model, invariants, and the end to end upgrade suite.
- [`docs/METADATA.md`](docs/METADATA.md) — the metadata schema design: protobuf records, the BadgerDB keyspace, and how S3 operations map to transactions.
- [`docs/ERASURE-CODING.md`](docs/ERASURE-CODING.md) — storage profiles: the k+m set, profile changes, small objects, the write-ack rule, and the single-node-to-cluster growth story.
- [`docs/S3-API.md`](docs/S3-API.md) — the S3 compatibility surface: operations by release, SigV4 authentication, ETag semantics, limits, and non-goals.
- [`docs/adr/`](docs/adr/) — Architecture Decision Records. One decision per file, with the reasoning and the rejected alternatives. Start at the [index](docs/adr/README.md).
- [`docs/ROADMAP.md`](docs/ROADMAP.md) — the v0.x and v1.0 milestones.

## Development workflow

- For now, the design docs are the backlog: the roadmap and each doc's open-questions section say what comes next, and finishing a design means updating the doc and its ADRs. No separate task-tracker files (no TODO lists, no kanban files) — the docs themselves carry the plan. GitHub Issues may take over once code lands.
- Keep pull requests small and focused: one issue, one concern.
- Sign every commit with `git commit -s` (Developer Certificate of Origin — see [ADR-0001](docs/adr/0001-apache-2-license-dco.md)).
- When a decision changes, update the relevant ADR (or write a new one superseding it) and any affected doc in the same pull request. Docs that contradict the code are worse than no docs.

## Naming

Use standard distributed systems vocabulary in code, docs, CLI, and logs: node, cluster, shard, partition, write buffer, data directory. Hamster is the brand, not an operational vocabulary — do not introduce themed names for system components.
