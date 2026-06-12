# Architecture Decision Records

This directory captures the decisions that shape Hamster — not just what was decided, but why, and what was rejected. When a decision changes, the old ADR is not rewritten: it is marked **Superseded** with a link to its replacement, so the history of the design stays readable.

ADRs are numbered in order of creation and never renumbered. New decisions get the next number.

## Index

| ADR | Title |
|---|---|
| [0001](0001-apache-2-license-dco.md) | Apache 2.0 license with a Developer Certificate of Origin |
| [0002](0002-single-binary-no-external-dependencies.md) | Single binary with no external service dependencies |
| [0003](0003-erasure-coding-over-replication.md) | Erasure coding over replication for object durability |
| [0004](0004-partitioned-placement.md) | Partitioned placement with a versioned layout, not fixed pools |
| [0005](0005-metadata-badgerdb-raft.md) | Metadata in BadgerDB replicated by Raft, with object data outside the log |
| [0006](0006-versioning-and-object-lock.md) | First class versioning and object lock |
| [0007](0007-uuidv7-version-ids.md) | UUIDv7 for version IDs |
| [0008](0008-versioned-formats-rolling-upgrades.md) | Versioned formats, expand then contract, and zero downtime rolling upgrades |
| [0009](0009-deterministic-simulation-testing.md) | Deterministic simulation testing and end to end upgrade tests |
| [0010](0010-v1-compatibility-policy.md) | v0.x to v1 compatibility policy |
| [0011](0011-permissive-only-dependencies.md) | Permissive-only dependency licensing |
| [0012](0012-etcd-raft-consensus-library.md) | etcd-io/raft as the consensus library |
| [0013](0013-klauspost-reedsolomon.md) | klauspost/reedsolomon for erasure coding |
| [0014](0014-metadata-keyspace-design.md) | Metadata keyspace: version-list truth table, derived current index, partition-indirect shard addressing |
| [0015](0015-storage-profiles.md) | Storage profiles: auto-by-default profile policy, per-object parameters, replication as k=1 |
| [0016](0016-failure-domain-hierarchy.md) | Failure domains above the node: hosts and zones |
| [0017](0017-raft-voter-cap-learners.md) | Raft voters capped at five; all other nodes join as learners |
| [0018](0018-sigv4-auth.md) | SigV4 authentication, implemented in-house on the standard library |
| [0019](0019-md5-etags.md) | MD5 ETags for compatibility, with integrity carried by internal checksums |
| [0020](0020-embedded-htmx-web-console.md) | Embedded web console on the admin port, server-rendered with htmx |
| [0021](0021-envelope-encryption-at-rest.md) | Envelope encryption at rest, encrypt-then-EC, pluggable key source |
| [0022](0022-cluster-mtls.md) | Mutual TLS for all inter-node traffic, cluster CA minted at init |
| [0023](0023-handwritten-protowire-codecs.md) | Hand-written protowire codecs for metadata records, deterministic, unknown-preserving |
| [0024](0024-deterministic-election-timing.md) | Hamster owns Raft election timing: external, seeded, simulator-schedulable |
| [0025](0025-force-new-cluster-recovery.md) | `cluster recover` is an offline force-new-cluster, and the local log wins |
| [0026](0026-stripe-and-shard-layout.md) | Stripe and shard layout: contiguous 256 KiB slices, self-describing shard files |
| [0027](0027-v03-distributed-data-path.md) | The v0.3 distributed data path: derived placement, shard transfer over the seam, coordinator state machines |

## Template

```markdown
# ADR-NNNN: Title

## Status

Proposed | Accepted | Superseded by [ADR-NNNN](NNNN-slug.md)

## Context

What situation, constraints, and forces made this decision necessary?
Written so a newcomer can understand the problem without the decision.

## Decision

What was decided, stated plainly and completely.

## Consequences

What follows from the decision — the good, the costs, and the new
obligations it creates. Honest about trade-offs.

## Alternatives considered

Each serious alternative, with the reason it was rejected. "Rejected"
should carry a why, not just a verdict.
```
