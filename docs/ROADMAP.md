# Roadmap

High level milestones only. Day to day work lives in the design docs themselves — each doc's open-questions section is the backlog (GitHub Issues may take over once the code matures).

> Hamster is at v0 and not production ready. Everything below is a goal, not a shipped capability.

## v0.x — build the core, get the formats right

The v0 series proves the design end to end:

- Core PUT and GET through the S3 API
- Erasure coded durability with self healing repair
- Partitioned placement with the versioned cluster layout (equal weight nodes, manual rebalance)
- Object versioning (version lists, delete markers, `ListObjectVersions`)
- Object lock and WORM retention (GOVERNANCE and COMPLIANCE modes, legal holds)
- The deterministic simulation harness — foundational from the first release, not retrofitted
- The end to end upgrade test suite

**On disk and on wire formats may change between v0 releases.** This window exists to get the formats right before promising to keep them forever.

### Release rhythm

Each v0 minor release carries one headline feature, in roughly this order. The ordering is a plan, not a promise — features may split or merge as the code pushes back:

| Release | Headline feature |
|---|---|
| v0.1 | Single node store: the core S3 surface (PUT/GET, listings, multipart, SigV4 auth), write buffer, version-list metadata model, simulation harness skeleton |
| v0.2 | Clustering: Raft-replicated metadata, multi-node membership, voter cap with learners, `cluster recover` |
| v0.3 | Erasure coding with self healing repair (shard rebuild) |
| v0.4 | Partitioned placement: versioned layout, zone-aware spread, capacity weighting, manual rebalance, repair re-encode (existing data climbs to the active profile) |
| v0.5 | Full versioning API: delete markers, ListObjectVersions |
| v0.6 | Object lock: GOVERNANCE, COMPLIANCE, legal holds |
| v0.7 | Upgrade machinery: feature gates, health interlock, the upgrade test suite |
| v0.8+ | Hardening and format finalization until v1 feels earned |

The simulation harness is not a milestone of its own: it ships in v0.1 and grows with every release, because each new feature must arrive with its failure schedules.

## v1.0 — the compatibility promise

v1.0 is the line where Hamster asks to be trusted:

- Stable formats with a Go style compatibility promise: v1 formats remain readable forever
- Zero downtime rolling upgrades, validated by the upgrade test suite
- The web console ([ADR-0020](adr/0020-embedded-htmx-web-console.md): embedded, on the admin port, server-rendered with htmx)

See [ADR-0010](adr/0010-v1-compatibility-policy.md) for what the version numbers promise.
