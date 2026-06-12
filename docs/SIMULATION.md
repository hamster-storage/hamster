# Simulation and Testing Design

This document designs the testing strategy that [ADR-0009](adr/0009-deterministic-simulation-testing.md) committed to: deterministic simulation as the primary correctness mechanism, plus end to end tests against real binaries. It is the blueprint for the harness, written before the code on purpose — the harness shapes the architecture, not the other way around.

> **Status: design document, foundations implemented.** The harness skeleton lives in [`internal/sim`](../internal/sim/): the global event queue, virtual time, the seeded PRNG, the faulty network (drops, duplication, latency, asymmetric partitions), and the crash-faithful disk (unsynced writes lost or torn, durable content honored — including under the write buffer's appends). The first real composition runs under it: a single-node store — metadata store, blob store, and the WAL persister ([`internal/wal`](../internal/wal/)) — crash-and-restarted adversarially across seeds and checked against a reference model, the single-node degenerate case of the checking loop below. The full fault schedules, multi-node workloads, and the remaining invariants arrive with the features they check (Raft in v0.2, erasure coding in v0.3), never retrofitted.

## Two layers, two jobs

| Layer | Runs as | Validates | Reproducible? |
|---|---|---|---|
| **Deterministic simulation** | One process, simulated world | All logic: consensus, placement, repair, versioning, locks | Exactly, from a seed |
| **End to end tests** | Real binaries on real OS | The plumbing: real disk, real sockets, real upgrades | Best effort |

The split follows from a fact about Go and a fact about testing. The Go fact: the runtime scheduler is nondeterministic, `select` over ready channels is random, and map iteration is random — so seed-exact reproducibility cannot come from running real concurrent code, no matter how time is faked. The testing fact: a simulator only validates what it models, so the gap between simulated adapters and the real OS must be covered by something that runs the real thing.

Logic is proven in the simulator. Plumbing is proven end to end. The design rule that makes the split sound: **no logic in the adapters** — any code that makes a decision lives on the simulated side of the interface, and the production adapters are thin, boring translation.

Neither layer uses containers (see [End to end tests](#layer-2-end-to-end-tests-real-binaries)).

## Layer 1: deterministic simulation

### Where determinism comes from

Not from the Go scheduler — from the architecture:

- A simulated node runs as a **single logical thread**: an event loop that takes one event, runs the node's state machines to completion, and emits new events. No shared-memory concurrency inside a simulated node.
- The **simulator owns a global event queue** ordered by virtual time. It pops the next event, advances the clock to that event's time, and dispatches it. Nothing else advances time.
- A **seeded PRNG makes every choice**: message delivery order and latency, which faults fire and when, workload contents, tiebreaks. Same seed, same binary → identical execution, event for event.

A failing run prints its seed. Replaying the seed replays the bug exactly, turning a distributed-systems heisenbug into single-threaded, breakpointable debugging.

### The world model

The simulator provides the world; nodes live in it:

- **Clock** — virtual time. Timers are just events in the queue. A thousand simulated hours of cluster time runs in seconds of wall time, because idle time costs nothing.
- **Network** — sending a message enqueues a delivery event. The simulator (driven by the PRNG and the active fault schedule) may delay, reorder, duplicate, or drop it, or partition node pairs entirely — including asymmetric partitions (A reaches B, B cannot reach A).
- **Disk** — reads and writes go to an in-memory simulated disk that models the failure semantics that matter: write errors, a full disk, and — critically — **crash semantics**: data written but not yet fsynced at crash time may be lost or torn on restart. Lying-fsync and bit-rot models arrive when scrubbing does.
- **Process** — a simulated crash drops everything except the simulated disk; restart re-initializes the node from that disk, exactly as a real restart recovers from a real one. Simulated pauses (a la GC or VM stalls) let us test clock-skew and lease-expiry behavior.

### Single-threaded simulation, concurrent production

The simulation being single-threaded does **not** mean production is. The contract is about where concurrency is allowed to live:

- **The control plane is single-threaded in both worlds.** Everything that makes a decision — metadata transactions, the Raft loop, placement, repair planning, lock enforcement — runs as one event loop per node, in production exactly as in simulation. This costs nothing: these are microsecond operations on small state, and it is how etcd runs its own Raft loop. Control-plane multi-core scaling arrives later via multi-raft ([ADR-0005](adr/0005-metadata-badgerdb-raft.md)): many partitions, each its own event loop, each still single-threaded inside.
- **The data plane is as parallel as we want, in production only.** Bulk byte work — streaming objects, EC encode/decode, checksumming, writing `k+m` shards in parallel — fans out across goroutines freely, because it holds no shared mutable core state. Its only channel back to the control plane is a completion event ("shard 3 durable", "shard write failed") delivered into the node's event loop.

Why this preserves the coverage argument: the only effect production concurrency can have on core logic is changing the *order and timing* of events reaching it. Event orderings are precisely what the simulator's PRNG searches, and it searches them more adversarially than real hardware ever produces — last shard ack delivered first, a crash between two completions, a partition in the gap, all on demand and reproducible. The orderings production can generate are a subset of what simulation explores; we test at the event level what production randomizes at the instruction level.

The rule that keeps the subset argument true, enforced in review: **core state is owned by its event loop, full stop.** Two goroutines sharing mutable metadata state behind a mutex would create instruction-level interleavings the simulator cannot represent, silently voiding the coverage claim. This is the "no logic in adapters" rule seen from the concurrency side.

The honest residue: data-plane and adapter code can have concurrency bugs of its own (buffer reuse races, connection pool mistakes), and the simulator will not see them, by design. That is covered by Go's race detector on every test layer, `synctest` for timer-driven adapter code, and the e2e suite running real concurrent binaries.

### The interface seam

Production code never touches time, randomness, the network, or the filesystem directly (already a CLAUDE.md convention). It receives interfaces, which live in [`internal/seam`](../internal/seam/):

```go
type Loop interface {
    Post(fn func())
    // The node's event loop: the single logical thread that owns all core
    // state. Adapters and the data plane hand work back to the core by
    // posting, never by sharing memory.
}

type Clock interface {
    Now() time.Time
    AfterFunc(d time.Duration, fn func()) Timer
    // Callbacks are delivered on the node's event loop, never a thread.
}

type Transport interface {
    Send(to NodeID, msg []byte)
    // Delivery happens via the node's event loop, not a callback thread.
}

type MessageHandler interface {
    HandleMessage(from NodeID, msg []byte)
    // The receiving half of the network contract: core logic implements
    // it, the drivers consume it — the simulator and the production
    // listener call the same method, one message at a time, on the loop.
}

type Disk interface {
    // Files written once and never edited in place, because objects are
    // immutable blobs. Writes are staged: durable only after Sync,
    // lost-or-torn at crash otherwise. Append is the write buffer's form —
    // it builds a file incrementally with bounded memory, and a crash
    // never takes back content that was durable before the appends began.
    WriteFile(name string, data []byte) error
    Append(name string, data []byte) error
    Sync(name string) error
    ReadFile(name string) ([]byte, error)
    ReadFileAt(name string, offset int64, length int) ([]byte, error)
    Remove(name string) error
    List() ([]string, error)
}
```

Randomness needs no interface: core code receives a `*math/rand/v2.Rand`, deterministic by construction once the simulator picks the seed. Each interface has two implementations: the simulated one in [`internal/sim`](../internal/sim/) (deterministic, fault-injectable) and the production one in [`internal/sys`](../internal/sys/) (thin, boring, no decisions). The same compiled core runs under both — what the simulator proves is what ships. The shapes grow with the code — `Append` arrived when the write buffer needed it — always settled in `internal/seam` first.

### Driving Raft

This is why [ADR-0012](adr/0012-etcd-raft-consensus-library.md) chose `etcd-io/raft` via `RawNode`: the consensus library is itself an inert state machine. The node's event loop:

1. Calls `Tick()` when the virtual clock fires the tick timer.
2. Calls `Step(msg)` for each message the simulated network delivers.
3. Processes `Ready()` synchronously: persists entries to the simulated disk, hands outgoing messages to the simulated transport, applies committed entries to the metadata store.

Consensus becomes fully deterministic under test with almost zero special-casing — the one exception found in practice is the library's internal election jitter, which draws unseedable entropy; [ADR-0024](adr/0024-deterministic-election-timing.md) moves the election timer into Hamster's code, on the seam. `internal/raftnode` implements this contract, and cluster schedules (elections, leader crashes, partitions, snapshot compaction with lagging-follower catch-up, membership growth with learner joins and voter promotion under the ADR-0017 cap, seed-replay equality) run under the harness.

### The fault model

Per run, the PRNG composes a fault schedule from:

- **Network**: partitions (full, partial, asymmetric), latency spikes, reordering, duplication, drops.
- **Clock**: per-node wall-clock skew and drift — readings differ between nodes while timers still fire correctly, modeling un-synced NTP.
- **Disk**: write errors, full disk, torn/lost unsynced writes at crash.
- **Process**: crashes and restarts at arbitrary points (including mid-PUT, mid-rebalance, mid-snapshot), long pauses.
- **Combinations**, because real outages are combinations: a partition during a rebalance during a node restart is a Tuesday.

We also plan FoundationDB-style **buggify hooks**: code sites in the core that, only under simulation, amplify rare conditions (force a buffer flush at the worst moment, return the rare error branch) so coverage doesn't wait on luck.

### Checking correctness

Every simulated run drives a generated workload (PUTs, GETs, overwrites, deletes, versioning and lock operations, membership changes) against the cluster and, in parallel, a **model**: a trivial in-memory reference implementation of what an object store should do. Invariants checked continuously and at end of run:

1. **Durability**: every acknowledged write is readable with correct content, provided faults stayed within that object's budget — `durable-at-ack − k` shard losses (at least one, by the write floor in [ERASURE-CODING.md](ERASURE-CODING.md)), rising to `m` once repair completes. The checker tracks each object's actual budget rather than assuming the healthy-path `m`.
2. **Linearizable metadata**: observed metadata operations admit a legal sequential order (Raft + quorum claim, actually checked).
3. **Version semantics**: version lists are append-ordered, current-version resolution matches the model, delete markers behave.
4. **Lock enforcement**: every attempt to delete or shorten retention on a COMPLIANCE-locked version fails — the harness actively tries, including with administrator credentials.
5. **Placement**: no two shards of one object on the same node, ever, including mid-rebalance.
6. **Convergence**: after faults heal and the schedule quiesces, repair restores full redundancy and the cluster reports healthy.

### CI shape

- **PR gate**: a fixed wall-time budget of fresh random seeds, plus a **regression corpus** — every seed that ever found a bug is rerun forever.
- **Nightly**: much longer runs, nastier fault schedules.
- A failure artifact is two lines: git SHA + seed. Anyone can replay it locally.

## Layer 2: end to end tests (real binaries)

> **Status:** the first incarnation exists (`test/e2e`, `task e2e`): the v0.2 cluster lifecycle against the built binary — init, token joins, status, leader failover on SIGINT, restart from disk, clean shutdown — plus an S3 serve smoke. The upgrade suite below arrives with the feature-gate machinery (v0.8).

### Direct process execution, not containers

The e2e suite spawns Hamster binaries as **plain child processes on localhost** — each with its own data directory, its own ports, talking over real loopback sockets. No Docker, no testcontainers. The reasoning:

- **Hamster is a single static binary with no external services** ([ADR-0002](adr/0002-single-binary-no-external-dependencies.md)). The problem containers solve — packaging a runtime environment — does not exist here. A container would wrap one self-contained executable in an image build, a daemon dependency, and seconds-to-minutes of startup tax.
- **CI speed**: GitHub Actions is the target environment, and process spawn is milliseconds. The upgrade suite starts and restarts nodes constantly; container overhead would multiply across every roll.
- **Developer friction**: `go test` with no Docker daemon requirement works on every contributor machine, including macOS without virtualization running.
- **Nothing is lost**: network fault injection is the simulator's job, not this layer's. E2e exists to validate the real plumbing and the upgrade machinery, and real processes on a real OS do exactly that.

Containers would be revisited only if we someday need kernel-level isolation or true multi-host topologies in CI — neither is on the roadmap.

### The upgrade suite

The end to end test that matters most ([ADR-0008](adr/0008-versioned-formats-rolling-upgrades.md), [ADR-0009](adr/0009-deterministic-simulation-testing.md)):

1. Obtain binaries for version N (last release) and N+1 (this commit).
2. Start a cluster at N; write a known workload, including versioned and locked objects.
3. Roll node by node to N+1, with a live read/write workload running throughout.
4. Assert continuous availability, zero data loss, and correct mixed-version behavior at every step.
5. Finalize the cluster version (feature gates), assert gated features activate; verify the full workload reads back intact.

A downgrade variant (roll one node back before finalization) keeps the expand-then-contract discipline honest.

## What this strategy does not catch

Stated plainly so nobody over-trusts it:

- **The fault model's blind spots.** The simulator validates against the faults we thought to model. The model grows with every incident and every new feature.
- **Real-OS behavior** beyond what e2e exercises — exotic filesystem semantics, kernel network edge cases. Mitigation: thin adapters, e2e coverage, and humility.
- **Performance.** Simulated time says nothing about throughput. Benchmarks are separate, later work.

## Open questions

- ~~Exact interface shapes — settled with the first v0.1 code, not in this doc.~~ Settled: first cut lives in `internal/seam`, shown above.
- `testing/synctest` for adapter-level concurrency tests (production write-buffer timers, HTTP timeouts) where fake time helps but seed-replay is not needed: likely yes, as a third, minor layer.
- ~~Whether the workload generator and model checker live in-repo from v0.1 (likely) or start as a hardcoded scenario list and grow.~~ Settled: in-repo from v0.1 — the metadata reference model in `internal/meta`'s tests and the crash-recovery workload in `internal/sim`'s single-node integration. Both grow into the cluster-wide checker as the features land.
