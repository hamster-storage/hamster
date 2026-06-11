# ADR-0011: Permissive-only dependency licensing

## Status

Accepted

## Context

Hamster is Apache 2.0 ([ADR-0001](0001-apache-2-license-dco.md)) precisely because the self hosted storage ecosystem was burned by licensing: MinIO's community edition was archived, and HashiCorp relicensed its products under BUSL in 2023. The permissive promise to Hamster's users is only as strong as the licenses of everything compiled into the binary — and because Hamster ships as a single static binary ([ADR-0002](0002-single-binary-no-external-dependencies.md)), every dependency's license travels with every distribution.

The question became concrete while evaluating Raft libraries: `hashicorp/raft` is MPL 2.0. MPL is weak, file-level copyleft and is generally considered usable inside Apache 2.0 projects — but it adds a second license regime users must reason about, and depending on a HashiCorp library after the 2023 relicensing means betting core infrastructure on a company that has already moved licenses once. Meanwhile permissive alternatives exist for everything Hamster needs (`etcd-io/raft` is Apache 2.0, BadgerDB is Apache 2.0, `google/uuid` is BSD-3, `klauspost/reedsolomon` is MIT).

## Decision

**Every imported package must be under a permissive license: Apache 2.0, MIT, BSD (2- or 3-clause), or ISC.** This applies transitively — a permissive direct dependency with a copyleft transitive dependency fails the policy.

Not allowed:

- **Copyleft**, including weak copyleft: GPL, AGPL, LGPL, MPL, EPL.
- **Source-available**: BUSL, SSPL, Elastic License, and similar.

Exceptions require an ADR making the case for that specific dependency. Once the build exists, CI should verify the policy mechanically (e.g., `go-licenses` or equivalent) rather than relying on review vigilance.

## Consequences

- Hamster's "no legal friction" promise holds for the whole binary, not just our code. Users never need a license inventory to embed or redistribute it.
- Some technically fine libraries are off the table — `hashicorp/raft` being the immediate example. In Go's ecosystem this costs little; permissive equivalents exist for the infrastructure Hamster needs.
- Excluding even weak copyleft (MPL) is stricter than legally necessary. Accepted deliberately: the simplicity of "one license story" and independence from relicensing-prone vendors outweigh the occasional excluded library.
- Dependency review gains a step, which the planned CI license check automates away.

## Alternatives considered

- **Allow weak copyleft (MPL, LGPL) case by case.** Legally workable — many Apache projects ship MPL dependencies. Rejected for simplicity: a single bright-line rule needs no legal judgment per dependency, and the project's origin story argues for maximum distance from relicensing risk.
- **No formal policy; review dependencies ad hoc.** This is how projects discover a GPL transitive dependency two years in. Rejected.
- **Vendor-and-fork copyleft code when needed.** Forking MPL code keeps the file-level obligations anyway and adds maintenance burden. Rejected as a default; a fork could still be proposed via the ADR exception path.
