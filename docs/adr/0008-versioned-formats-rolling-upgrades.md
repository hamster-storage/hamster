# ADR-0008: Versioned formats, expand then contract, and zero downtime rolling upgrades

## Status

Accepted

Decision 6 (a manually-finalized cluster version) is partially superseded by
[ADR-0034](0034-rolling-upgrade-machinery.md): the cluster version auto-rolls
etcd-style instead. Everything else in this ADR stands.

## Context

A storage system's on disk formats outlive every version of the code that wrote them, and during any rolling upgrade two versions of the software read and write the same wire protocols simultaneously. Most upgrade disasters in storage systems trace back to format changes that were not designed to be survivable: a node writes a new format its peers cannot read, or an upgrade takes down a node while the cluster is already degraded and quorum breaks.

Hamster promises "safe to upgrade" in its README, and v1 will promise zero downtime rolling upgrades. Those are properties you design in from the first byte written, not features added later.

## Decision

Format evolution and upgrades follow a fixed discipline:

1. **Every on disk and on wire structure carries a version field** and uses additively evolvable serialization (**protobuf**). Fields are **only ever added, never removed or repurposed**. New code must always read old formats.
2. **The cluster layout is versioned state** ([ADR-0004](0004-partitioned-placement.md)). Topology changes create a new layout version with old→new transition tracking, so reads find data that is mid migration.
3. **Zero downtime rolling upgrades** (a v1 commitment): upgrade one node at a time, staying within EC fault tolerance, with a **health check interlock** that refuses to take a node down while the cluster is already degraded — quorum is never broken by an upgrade.
4. **Breaking changes follow expand then contract** across releases: first teach all nodes to *read* the new format, then start *writing* it once every node can read it, then drop the old.
5. **Published upgrade policy: cross at most one major version at a time.**
6. **A cluster version with feature gates**: features that require all nodes stay dormant until an administrator finalizes the version bump after every node is upgraded — mixed-version clusters never half-enable a feature.

## Consequences

- Mixed-version operation is the designed-for state, not an emergency. Old readers skip unknown protobuf fields; new readers handle old data by construction.
- "Never remove or repurpose fields" means formats accumulate cruft over time. Accepted: dead fields are far cheaper than corrupted reads. The v0.x window ([ADR-0010](0010-v1-compatibility-policy.md)) exists to minimize the cruft v1 inherits.
- Breaking changes take at least two releases (expand, then contract). Format changes get slower and more deliberate — that is the point.
- The interlock and feature gate machinery is real engineering, and the end to end upgrade tests ([ADR-0009](0009-deterministic-simulation-testing.md)) exist specifically to validate it: stand up version N, write data, roll node by node to N+1, assert no data loss and continuous availability.
- This ADR constrains every future ADR and PR that touches a persistent or networked structure — hence its place among the CLAUDE.md invariants.

## Alternatives considered

- **Hand rolled binary formats.** Maximum control and compactness, but we would be rebuilding protobuf's unknown-field tolerance by hand, and getting it wrong corrupts data. Rejected.
- **JSON/CBOR with schema versioning.** Flexible and debuggable, but weaker typing, no enforced field-number discipline, and worse performance on hot metadata paths. Rejected.
- **Stop-the-world upgrades with offline format migration.** Drastically simpler — no mixed-version support, no interlocks — and honestly how many systems at this scale operate. But it caps Hamster below the "safe to upgrade" promise, and migrations of large stores of objects take unbounded downtime. Rejected.
- **Supporting upgrades across multiple major versions.** Every additional supported gap multiplies the expand-then-contract matrix and the upgrade test surface. One major version at a time keeps the promise testable. Rejected.
