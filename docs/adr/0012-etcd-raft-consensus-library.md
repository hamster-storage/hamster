# ADR-0012: etcd-io/raft as the consensus library

## Status

Accepted

## Context

Metadata replication is built on Raft ([ADR-0005](0005-metadata-badgerdb-raft.md)), and writing a consensus implementation from scratch is the most dangerous way to spend a young project's correctness budget. The Go ecosystem offers mature options, but they differ in exactly the dimension Hamster cares most about: whether the library can run under the deterministic simulation harness ([ADR-0009](0009-deterministic-simulation-testing.md)).

- **`hashicorp/raft`** is batteries-included: it spawns its own goroutines, runs its own timers, and owns its transport. Convenient in a normal project — and nearly impossible to simulate deterministically, because time and I/O happen inside the library where the simulator cannot reach. It is also MPL 2.0, which fails the dependency license policy ([ADR-0011](0011-permissive-only-dependencies.md)).
- **`etcd-io/raft`** (`go.etcd.io/raft/v3`, Apache 2.0) is deliberately inert: a pure state machine with no goroutines, no clocks, and no network. The caller invokes `Tick()` to advance time, feeds incoming messages in, and receives back entries to persist and messages to send. The `RawNode` API is fully synchronous. It is the most battle-tested Raft in the Go ecosystem — etcd runs on it, and CockroachDB built multi-raft on a fork of it, which is direct precedent for Hamster's planned multi-raft evolution.

## Decision

Hamster uses **`etcd-io/raft`** via the synchronous **`RawNode`** API. Hamster owns everything around it: the write-ahead log, snapshot orchestration, the transport, and configuration-change handling.

The integration contract follows from ADR-0009: the simulator drives `Tick()` from the virtual clock and delivers messages through the simulated network, making consensus fully deterministic under test. Production wiring swaps in the real clock and transport around the identical state machine.

## Consequences

- Consensus becomes deterministic by construction under the simulation harness — no fighting a library's internal goroutines.
- The assembly work is real and consistency-critical: WAL, snapshots, transport, and membership changes are ours to write. This is accepted deliberately — that surrounding code is precisely what the simulation harness exists to validate, and we would rather own it than have it hidden inside a library we cannot simulate.
- License policy is satisfied (Apache 2.0), and the multi-raft path ([ADR-0005](0005-metadata-badgerdb-raft.md)) has industry precedent on this exact library.
- We inherit etcd-raft's API stability story; it is a mature, slowly-moving library, which suits a storage system.

## Alternatives considered

- **`hashicorp/raft`.** Mature and widely deployed, but MPL 2.0 (fails ADR-0011) and architecturally closed to deterministic simulation — its goroutines and timers are internal. Rejected on both grounds; either alone would suffice.
- **`lni/dragonboat`.** Apache 2.0 and ships native multi-raft, which is tempting given our roadmap. But like hashicorp/raft it owns its own execution (internal goroutines, timers, transport), which defeats the simulator, and it is effectively a single-maintainer project for consensus-critical code. Rejected.
- **Writing our own consensus** (the TigerBeetle path, which implemented Viewstamped Replication from scratch). Maximum simulability and control, but consensus is the most subtle code in the system and etcd-raft already offers the inert state-machine shape we would be reimplementing. Rejected: spend the correctness budget on the storage engine instead.
