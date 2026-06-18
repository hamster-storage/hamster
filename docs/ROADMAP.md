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
| v0.7 | Encryption at rest (SSE-S3): envelope encryption over the framed stream, a pluggable key source, the SSE-S3 surface ([ADR-0021](adr/0021-envelope-encryption-at-rest.md)) |
| v0.8 | Key and CA rotation: KEK rewrap under a new master key (object bytes untouched), and CA custody and rotation ([ADR-0022](adr/0022-cluster-mtls.md), [ADR-0029](adr/0029-ca-custody-and-issuance.md)) |
| v0.9 | Zero-downtime rolling upgrades: etcd-style cluster version advertisement, the health interlock (`cluster can-stop`), the end-to-end upgrade test suite, and the supported operator-driven roll ([ADR-0034](adr/0034-rolling-upgrade-machinery.md), [UPGRADES.md](UPGRADES.md)). The binary swap is the deployment system's job, per node; Hamster owns the safety machinery and the proof |
| v0.10 | Observability and telemetry |
| v0.11 | Web console ([ADR-0020](adr/0020-embedded-htmx-web-console.md): embedded, on the admin port, server-rendered with htmx) |
| v0.12+ | Hardening and format finalization until v1.0 feels earned |

The simulation harness is not a milestone of its own: it ships in v0.1 and grows with every release, because each new feature must arrive with its failure schedules.

**On the v0.7 / v0.8 split.** v0.7 ships encryption at rest (SSE-S3) — envelope encryption, the pluggable key source, the SSE surface — as a complete, self-contained feature. The *rotation* work it was first bundled with is split out to v0.8: it is substantial, security-sensitive, and independent of the at-rest path, so it earns its own release. [ADR-0022](adr/0022-cluster-mtls.md) pairs CA rotation with KEK rotation, so the cluster CA's custody and rotation travel with the master-key rewrap as one keys problem. v0.8 makes issuance pluggable (an operator's external PKI — Vault, an offline or HSM root — as a first-class issuer, [ADR-0029](adr/0029-ca-custody-and-issuance.md)) and lands recovery from a *lost* CA key. That recovery does not break the trust model: losing the key costs issuance, not the running cluster — existing node certs still validate, because validation needs only the CA *certificate*, which survives in every node's trust store. To restore issuance you regenerate a new CA and migrate trust through a **multi-CA trust bundle** — trust the old and new CA at once, reissue every node cert from the new CA, then drop the old — the same dual-trust rollover etcd, CockroachDB, and Vault use, with no moment where nothing is trusted. The design enabler to build is that multi-CA bundle; the rotation flow (and its lost-key case) gets its own ADR when the work starts.

**Not yet scheduled:** migrating a single-node `serve` deployment into a cluster. The two are different data paths — `serve` stores single-node blobs, a cluster stores erasure-coded shards — so there is no *in-place* conversion; the cross-path move is a data migration. For ordinary objects that migration is already possible with generic S3 tooling (copy to the new cluster, delete from the source as each object lands — no double storage), which covers the homelab/evaluation case it is meant for. It is deliberately **not** positioned as a path for regulated data: a generic copy does not preserve version history (v0.5) or object-lock state (v0.6), and a COMPLIANCE-locked object cannot be deleted from the source at all, so versioned or regulated workloads are expected to start as a multi-node cluster (mutual TLS is automatic from the first node) rather than grow into one. A native, lock- and version-aware migration tool could close that gap later but is a low-priority convenience, not committed to a minor release. A deployment that is already a cluster needs none of this — it grows by adding nodes.

## v1.0 — the compatibility line

v1.0 is deliberately un-glamorous: not a feature, a commitment. It says we are
confident in where the software is, and that from here updates are supported with
backward compatibility for existing clusters — a running v1 cluster upgrades in
place and keeps reading the data and formats it already wrote, indefinitely. The
capabilities that earn this promise — stable formats, zero-downtime rolling
upgrades, the upgrade test suite — ship in the v0.x releases above; v1.0 is the
point where we commit to holding them, not the point where they first appear.

See [ADR-0010](adr/0010-v1-compatibility-policy.md) for exactly what the version
numbers promise.
