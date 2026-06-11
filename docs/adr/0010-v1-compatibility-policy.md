# ADR-0010: v0.x to v1 compatibility policy

## Status

Accepted

## Context

Hamster needs to develop in the open without trapping early users, and eventually to make a compatibility promise strong enough to trust with real data. Those goals pull in opposite directions: format stability too early freezes mistakes forever (and [ADR-0008](0008-versioned-formats-rolling-upgrades.md) makes format fields effectively permanent once promised — never removed, never repurposed); stability too late means nobody can rely on the system.

Go's compatibility promise is the model worth copying: a clearly drawn line (1.0), an explicit statement of what is covered, and a track record of holding it. Semantic versioning gives the vocabulary.

## Decision

Hamster uses **semantic versioning** with an explicit format-stability line:

- **v0.x — the format-change window.** On disk and on wire formats **may change between v0 releases**, and a v0 upgrade may require migration steps or, in the worst case, re-ingesting data. This is stated loudly in the README and release notes. The window exists to get the formats right before promising to keep them.
- **v1.0 — the compatibility promise**, Go style: **v1 formats remain readable forever.** Every subsequent release reads data written by any v1.x release. Combined with ADR-0008, upgrades within the promise are rolling and zero downtime, crossing at most one major version at a time.
- Reaching v1.0 is gated on the formats having survived real exercise: the simulation harness and the end to end upgrade suite ([ADR-0009](0009-deterministic-simulation-testing.md)) passing across v0 format transitions, so the promise is backed by tests rather than optimism.

## Consequences

- Early adopters get an honest contract: v0 is for kicking the tires, not for objects you cannot re-create. The README's "do not trust real data to Hamster yet" banner is this ADR speaking.
- Development speed stays high exactly when format mistakes are cheapest to fix, and v0 format changes still flow through expand-then-contract where practical — as rehearsal for the discipline v1 makes mandatory.
- v1.0 becomes a meaningful event rather than a marketing milestone: it is the date after which format mistakes can no longer be fixed by changing the format, only by additive evolution.
- "Readable forever" accumulates legacy decode paths over the years. Accepted — that is what the promise costs, and the upgrade test suite keeps the old paths honest.

## Alternatives considered

- **Format stability from the first release.** Maximum early-adopter safety, but the first design draft of every format would be frozen forever. Storage formats need iteration informed by the simulator and real workloads. Rejected.
- **A perpetual v0 / no promise** (stability by reputation instead of contract). Avoids commitment, but users planning storage deployments need a contract, not vibes. Rejected.
- **Time-boxed format support (e.g., "formats readable for two major versions").** Common in databases and lighter to maintain, but it forces periodic forced migrations on users — and "stuff it in, pull it out later" should not have an asterisk. Rejected in favor of the Go style promise.
