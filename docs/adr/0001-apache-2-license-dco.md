# ADR-0001: Apache 2.0 license with a Developer Certificate of Origin

## Status

Accepted

## Context

Hamster exists in part because of a licensing event: MinIO archived its community edition in 2026 and steered users toward a commercial product, leaving self hosters who had built on it exposed. License choice is therefore not a formality for this project — it is part of the value proposition. Users need confidence that they can deploy, embed, and build on Hamster without legal friction, and that the project cannot pull the same rug.

The project also needs a contribution policy. The two common options are a Contributor License Agreement (CLA), which requires contributors to sign a legal agreement (often assigning broad rights to a single entity), and the Developer Certificate of Origin (DCO), where contributors certify provenance of their changes with a `Signed-off-by` line.

## Decision

Hamster is licensed under the **Apache License 2.0**. Contributions are accepted under the **Developer Certificate of Origin**: every commit carries a `Signed-off-by` line (`git commit -s`).

## Consequences

- Anyone can use, modify, redistribute, and commercialize Hamster. The explicit patent grant in Apache 2.0 protects users in a way MIT/BSD do not spell out.
- A CLA-style relicensing of the community's contributions is structurally hard: contributors retain their copyright, so the project cannot unilaterally move the combined work to a restrictive license. This is a feature.
- Contributors have a one-time, low-friction obligation: sign commits with `-s`. CI will enforce the sign-off.
- The project gives up the leverage a CLA provides (e.g., dual licensing as a business model). Hamster accepts that trade deliberately.

## Alternatives considered

- **AGPL or another copyleft license.** This is the path MinIO took. Copyleft would deter exactly the users Hamster wants — people embedding object storage in their own stacks — and the 2026 landscape shows how copyleft plus a commercial entity creates relicensing anxiety. Rejected.
- **MIT or BSD.** Equally permissive, but without an explicit patent grant. For infrastructure software touching storage and erasure coding, the Apache 2.0 patent clause is worth the slightly longer license text. Rejected.
- **CLA instead of DCO.** Higher contributor friction, requires legal infrastructure, and concentrates relicensing power in the project owner — the exact failure mode the ecosystem just lived through. Rejected.
