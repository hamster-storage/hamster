# Roadmap

High level milestones only. Day to day work lives in the design docs themselves — each doc's open-questions section is the backlog, and a specific behavioral gap is guarded by a skipped or failing test that names the ADR it asserts (GitHub Issues may take over once there are outside contributors).

> Hamster is at v0 and not production ready. Everything below is a goal, not a shipped capability.

## v0.x — build the core, get the formats right

The v0 series proves the design end to end:

- Core PUT and GET through the S3 API
- Erasure coded durability with self healing repair
- Partitioned placement with the versioned cluster layout (equal weight nodes, manual rebalance)
- Object versioning (version lists, delete markers, `ListObjectVersions`)
- Object lock and WORM retention (GOVERNANCE and COMPLIANCE modes, legal holds)
- Encrypted, authenticated inter-node traffic ([ADR-0022](adr/0022-cluster-mtls.md)) and envelope encryption at rest ([ADR-0021](adr/0021-envelope-encryption-at-rest.md))
- The deterministic simulation harness — foundational from the first release, not retrofitted
- The end to end upgrade test suite

**On disk and on wire formats may change between v0 releases.** This window exists to get the formats right before promising to keep them forever.

### Release rhythm

Each v0 minor release carries one headline feature, in roughly this order. The ordering is a plan, not a promise — features may split or merge as the code pushes back:

| Release | Headline feature |
|---|---|
| v0.1 | Single node store: the core S3 surface (PUT/GET, listings, multipart, SigV4 auth), write buffer, version-list metadata model, simulation harness skeleton |
| v0.2 | Clustering: Raft-replicated metadata, multi-node membership, voter cap with learners, `cluster recover`, inter-node mTLS ([ADR-0022](adr/0022-cluster-mtls.md)) |
| v0.3 | Erasure coding with self healing repair (shard rebuild), over the framed object stream ([DATA-STREAM.md](DATA-STREAM.md) — designed before this release freezes the shard layout) |
| v0.4 | Partitioned placement: versioned layout, zone-aware spread, capacity weighting, manual rebalance, repair re-encode (existing data climbs to the active profile) |
| v0.5 | Full versioning API: delete markers, ListObjectVersions |
| v0.6 | Object lock: GOVERNANCE, COMPLIANCE, legal holds |
| v0.7 | Encryption at rest: envelope encryption over the framed stream, pluggable key source, SSE-S3 surface ([ADR-0021](adr/0021-envelope-encryption-at-rest.md)) |
| v0.8 | Upgrade machinery: feature gates, health interlock, the upgrade test suite |
| v0.9+ | Hardening and format finalization until v1 feels earned |

The simulation harness is not a milestone of its own: it ships in v0.1 and grows with every release, because each new feature must arrive with its failure schedules.

**Not yet scheduled:** migrating a single-node `serve` deployment into a cluster. The two are different data paths — `serve` stores single-node blobs, a cluster stores erasure-coded shards — so there is no *in-place* conversion; the cross-path move is a data migration. For ordinary objects that migration is already possible with generic S3 tooling (copy to the new cluster, delete from the source as each object lands — no double storage), which covers the homelab/evaluation case it is meant for. It is deliberately **not** positioned as a path for regulated data: a generic copy does not preserve version history (v0.5) or object-lock state (v0.6), and a COMPLIANCE-locked object cannot be deleted from the source at all, so versioned or regulated workloads are expected to start as a multi-node cluster (mutual TLS is automatic from the first node) rather than grow into one. A native, lock- and version-aware migration tool could close that gap later but is a low-priority convenience, not committed to a minor release. A deployment that is already a cluster needs none of this — it grows by adding nodes.

## v1.0 — the compatibility promise

v1.0 is the line where Hamster asks to be trusted:

- Stable formats with a Go style compatibility promise: v1 formats remain readable forever
- Zero downtime rolling upgrades, validated by the upgrade test suite
- The web console ([ADR-0020](adr/0020-embedded-htmx-web-console.md): embedded, on the admin port, server-rendered with htmx)

See [ADR-0010](adr/0010-v1-compatibility-policy.md) for what the version numbers promise.
