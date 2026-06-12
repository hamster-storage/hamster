# ADR-0013: klauspost/reedsolomon for erasure coding

## Status

Accepted, implemented (`internal/ec` — the stripe codec of [ADR-0026](0026-stripe-and-shard-layout.md) drives it)

## Context

Durability is built on Reed-Solomon erasure coding ([ADR-0003](0003-erasure-coding-over-replication.md)), which puts encode/decode on the hot path of every PUT and every degraded GET. The implementation needs to be fast (it touches every byte stored), correct (it is the durability mechanism), and permissively licensed ([ADR-0011](0011-permissive-only-dependencies.md)).

`github.com/klauspost/reedsolomon` is the de facto standard in Go: MIT licensed, heavily optimized with SIMD assembly (AVX2/AVX-512 on amd64, NEON on arm64), offering both whole-buffer and streaming APIs. It is the library MinIO built its erasure coding on, so it has processed exabytes in production object-storage workloads — the exact shape of Hamster's use.

## Decision

Hamster uses **`klauspost/reedsolomon`** for all Reed-Solomon encode, decode, and shard reconstruction.

Encoding is pure computation — deterministic output, no I/O, no clocks — so the library sits naturally under the simulation harness ([ADR-0009](0009-deterministic-simulation-testing.md)) without needing an interface boundary. Any internal parallelism options it offers do not affect output determinism.

## Consequences

- Hot-path performance comes from a decade of assembly-level optimization we do not have to write or maintain.
- MIT license satisfies the dependency policy.
- Production pedigree (MinIO, among many others) means the failure modes are well explored; correctness risk concentrates in our shard placement and repair logic, not the math.
- We take a dependency on amd64/arm64-optimized code paths; other architectures fall back to generic Go, slower but correct. Acceptable for the target deployments.

## Alternatives considered

- **Implementing Reed-Solomon ourselves.** Educational, but matrix-multiply-over-GF(256) at competitive speed means writing SIMD assembly, and bugs here lose data. Rejected without much agonizing.
- **`templexxx/reedsolomon`.** Comparable raw speed, but far less production exposure and a less active maintenance history than klauspost's. Rejected.
- **`vivint/infectious`.** Solid library (used by Storj historically), but less actively maintained and slower than klauspost. Rejected.
