# ADR-0009: Deterministic simulation testing and end to end upgrade tests

## Status

Accepted

## Context

Hamster asks people to trust it with data, and durability claims are exactly the kind that conventional testing fails to earn. Unit tests exercise happy paths; integration tests on real machines hit failure interleavings by luck and reproduce them never. The bugs that lose data live in the gaps: a disk failing mid-commit during a partition, a Raft leader change racing a shard write, a rebalance interrupted by a node crash.

FoundationDB and TigerBeetle demonstrated the alternative: run the entire cluster in a single process inside a **simulated world** — simulated clocks, network, and disks — where the test harness controls every source of nondeterminism, injects faults aggressively, and reproduces any failure exactly from a seed. FoundationDB's reputation rests substantially on this; their simulator found bugs that years of production would not have surfaced.

The catch: determinism cannot be retrofitted. If code calls `time.Now()`, spawns uncontrolled goroutines, or touches the real network directly, the simulator cannot control it. This decision shapes the architecture, so it must be made before the first line of the core is written.

## Decision

**Deterministic simulation testing is the primary correctness mechanism, foundational from v0.**

- The whole cluster runs in a simulated world with controllable clocks and injected network partitions, disk failures, and message reordering — all reproducible from a single seed.
- Consequently (and enforced as a code convention in CLAUDE.md): time, randomness, network, and filesystem access go behind interfaces the simulator can substitute. No exceptions on the core paths.
- Changes to durability or consistency behavior must survive the simulation harness — thousands of simulated failure schedules — before merging. This is what makes future contributions safe to accept: the harness, not reviewer vigilance, is the backstop.

**End to end upgrade tests** complement the simulator: stand up a cluster at version N, write data, roll node by node to N+1, and assert **no data loss and continuous availability** throughout. These validate the upgrade machinery of [ADR-0008](0008-versioned-formats-rolling-upgrades.md) against real binaries.

## Consequences

- Every failure found in simulation is replayable from its seed — debugging distributed races becomes deterministic single-process debugging.
- The dependency-injection discipline costs ongoing ergonomic overhead (no bare `time.Now()`, no ad hoc goroutines on core paths) and constrains library choices: a dependency that does its own I/O or timing internally may be unusable on the simulated paths.
- The simulator only validates what it models; the fault model (partitions, disk failures, reordering, crashes) must grow with the system, and simulation does not replace real-hardware testing for performance.
- CI gets slower and more demanding — by design. A contribution that fails one seed in thousands is a bug found before a user's data was behind it.
- Building the harness is a large up-front investment that delays visible features in v0. Accepted: it is the cheapest this investment will ever be.

## Alternatives considered

- **Conventional integration tests plus chaos engineering (Jepsen-style) later.** Finds real bugs, but the interleavings are not reproducible and coverage is luck-bound. Useful as a complement someday; insufficient as the primary mechanism. Rejected as the foundation.
- **Retrofit simulation after v1.** The history of such retrofits is grim — nondeterminism diffuses through a codebase and cannot be extracted later. Rejected.
- **Model checking (TLA+) of the protocols.** Valuable for designs, but verifies the model, not the Go code that ships. Possible complement for the trickiest protocols; not a substitute. Not adopted as the primary mechanism.
- **Skipping the upgrade test suite until v1 approaches.** The expand-then-contract discipline needs validation from the first format change between v0 releases, not after habits have formed. Rejected.
