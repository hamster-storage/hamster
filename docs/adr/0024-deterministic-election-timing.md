# ADR-0024: Hamster owns Raft election timing

## Status

Accepted

## Context

[ADR-0012](0012-etcd-raft-consensus-library.md) chose `etcd-io/raft` because it is inert — no goroutines, no clocks, no network — which is what lets the simulation harness ([ADR-0009](0009-deterministic-simulation-testing.md)) drive consensus deterministically. Integration surfaced the one exception: the library randomizes each node's election timeout internally, and in v3.6 that draw comes from **`crypto/rand`** — hardware entropy, unseedable by design. Left in charge, it decides *when* nodes campaign, which decides message orderings, which breaks the seed-replay promise every time an election matters — precisely the moments a simulator exists for.

The library offers no injection point for this randomness, and forking the most battle-tested Raft in the ecosystem to add one would forfeit the reason it was chosen.

## Decision

Hamster disables the library's internal election trigger and **owns the election timer**, extending ADR-0012's "Hamster owns everything around it" to election timing:

- `Config.ElectionTick` is set effectively infinite, so the library's randomized timeout never fires. (Its `crypto/rand` draw still happens internally, but the value feeds a comparison that can never trigger — discarded entropy cannot influence event order, so determinism survives without a fork.)
- Each node runs its own election clock in `internal/raftnode`, on the seam: virtual time from `seam.Clock`, jitter from the node's seeded PRNG. A follower that has not heard from a leader for a full randomized timeout calls `RawNode.Campaign()` explicitly.
- `PreVote` is enabled, so an ill-timed campaign (a partitioned node rejoining, a too-eager timer) probes before disrupting a live term — the same protection etcd deploys.

The same compiled timer runs in production with the real clock and a crypto-seeded PRNG from the composition root. What the simulator proves is what ships.

## Consequences

- Elections become schedulable faults: the simulator's PRNG decides which node times out first, identically on every replay of a seed.
- The election trigger — when to campaign, how to jitter, what counts as hearing from a leader — is now Hamster code, with Hamster bugs. This is the smallest slice of Raft we could take on: vote counting, term safety, log matching, and commitment all remain the library's. The simulation harness exercises the trigger under partitions, crashes, and skew, which is more scrutiny than an internal timer ever gets.
- Features keyed to the library's internal election clock — `CheckQuorum` leader self-demotion, lease-based reads — stay off until they get the same treatment. Reads will use commit-index barriers until then.

## Alternatives considered

- **Accept the entropy.** Elections would replay differently per run; a failing seed could not be replayed exactly when it matters most. Rejected — it nullifies ADR-0009 exactly where consensus needs it.
- **Fork or vendor the library to inject a rand source.** A standing fork of consensus-critical code, rebased forever against upstream. Rejected; revisit only if upstream grows an injection point worth adopting.
- **Drive `Tick()` selectively to fake timeouts.** Withholding or bursting ticks to steer the internal timer. Rejected: it fights the library through a side channel and still cannot control the random draw inside.
