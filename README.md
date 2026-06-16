# Hamster

**S3 compatible object storage in a single binary. Stuff it in, pull it out later.**

<img width="1280" height="640" alt="hammy" src="https://github.com/user-attachments/assets/aafa0809-a736-4c9f-9dc6-1f6411e02d5b" />

Hamster is a self hosted, S3 compatible object store built around one idea: object storage should be simple to run and safe with your data, without a heavyweight distributed system or a restrictive license.

> **Status: early development (v0). Not production ready.**
> The design is settled and the core is being built in the open. Please don't trust real data to Hamster yet. Star or watch the repo to follow progress toward v1.

## Why Hamster

When MinIO archived its community edition in 2026 and steered users toward a commercial product, the open source S3 stores that remain split into two camps: feature-rich systems that bring real operational weight, and admirably simple ones that leave out the features regulated data can't live without. Hamster aims for the missing middle:

- A single binary you can run anywhere — no ZooKeeper, no etcd, no external database.
- Erasure-coded durability, so storage stays cheap without giving up safety.
- The compliance controls simpler stores skip: versioning, object lock, and WORM retention — what retention and audit regimes (HIPAA, SEC 17a-4 territory) actually ask for.
- A permissive Apache 2.0 license, so you can build on it without legal friction.

## Design principles

- **Single binary, no external dependencies.** Laptop, VPS, or cluster — nothing else to operate.
- **S3 compatible.** Works with existing S3 SDKs, CLIs, and tools.
- **Durable by default.** Reed–Solomon erasure coding spreads each object across independent failure domains, so you can lose drives or whole nodes without losing data.
- **Grows smoothly.** Partitioned placement rebalances as you add capacity — add a node and data redistributes without reshaping the cluster.
- **Safe to upgrade.** Additively versioned on-disk and on-wire formats, built for backwards-compatible, zero-downtime rolling upgrades validated by end-to-end tests.
- **Trustworthy.** Durability and consistency run under a deterministic simulation harness that injects partitions, disk failures, and reordering — correctness tested, not hoped for.

## Features

High level and honest: a check mark means shipped and tested, not promised. Versions beyond that are the [roadmap](docs/ROADMAP.md)'s plan and may shift as the code pushes back. On-disk and on-wire formats may change between v0 releases.

| Version | Features | Status |
|---|---|---|
| [v0.1](https://github.com/hamster-storage/hamster/releases/tag/v0.1.0) | <ul><li>Core S3 API — buckets, objects, listings, multipart, presigned URLs, SigV4 auth (verified with <code>aws</code>, <code>rclone</code>, <code>restic</code>, <code>s3cmd</code>)</li><li>Durable single-node store with streaming uploads</li></ul> | ✅ |
| [v0.2](https://github.com/hamster-storage/hamster/releases/tag/v0.2.0) | Clustering — Raft-replicated metadata, mTLS between nodes, token-based join | ✅ |
| [v0.3](https://github.com/hamster-storage/hamster/releases/tag/v0.3.0) | Erasure-coded durability with self-healing repair, the S3 endpoint served from the cluster | ✅ |
| [v0.4](https://github.com/hamster-storage/hamster/releases/tag/v0.4.0) | Partitioned placement (failure-domain spread, capacity weighting) and online rebalancing — drain, replace, remove, grow, downsize — plus a continuous background scrubber that self-heals bitrot and lost shards | ✅ |
| [v0.5](https://github.com/hamster-storage/hamster/releases/tag/v0.5.0) | Object versioning — per-bucket versioning config, version IDs, delete markers, `ListObjectVersions`, by-version GET/DELETE — on the single node and the cluster | ✅ |
| v0.6 | Object lock and WORM retention (GOVERNANCE and COMPLIANCE modes) | planned |
| v0.7 | Encryption at rest (SSE-S3) and key/CA rotation | planned |
| v0.8 | Upgrade machinery: feature gates, health interlock, the upgrade test suite | planned |
| v0.9 | Zero-downtime rolling upgrades | planned |
| v0.10 | Observability/Telemetry | planned |
| v0.11 | Web console | planned |
| TBD | TBD prior to v1 | planning |
| v1.0 | Software updates and migrations supported from v1 | planned |

## Quick start

Grab a binary from the [releases page](https://github.com/hamster-storage/hamster/releases) (or `go build ./cmd/hamster` — no cgo, no build tricks), then start the server. The `HAMSTER_*` variables define the credentials it will accept:

```sh
export HAMSTER_ACCESS_KEY_ID=hamster
export HAMSTER_SECRET_ACCESS_KEY=keep-this-one-secret
hamster serve -data-dir ./data
```

That's a standard S3 endpoint on `127.0.0.1:9000` — any S3 client works as is. The client sends its own credentials (the standard `AWS_*` variables) set to the same values:

```sh
export AWS_ACCESS_KEY_ID=hamster
export AWS_SECRET_ACCESS_KEY=keep-this-one-secret
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://stash
aws --endpoint-url http://127.0.0.1:9000 s3 cp video.mp4 s3://stash/
```

`aws s3`, `rclone`, `restic`, and `s3cmd` all work — a [compatibility suite](test/compat/) runs all four against every change.

## Running a cluster

A cluster is Raft-replicated metadata (v0.2) plus an erasure-coded data path (v0.3): mutual TLS between nodes with zero TLS configuration, single-use join tokens, and — with `-s3` — the full S3 API on every node, objects spread `k+m` across the cluster and reconstructed from any `k`. (Writes commit on the Raft leader for now; a non-leader answers `503` and clients retry. Multipart and server-side copy join this path in a later release.)

Three terminals, sharing the credentials each node's S3 endpoint accepts:

```sh
export HAMSTER_ACCESS_KEY_ID=hamster HAMSTER_SECRET_ACCESS_KEY=keep-this-one-secret

# terminal 1 — found the cluster, serve S3 on :9000
hamster cluster init -data-dir ./n1 -node n1 -listen 127.0.0.1:7946
hamster cluster run -data-dir ./n1 -s3 127.0.0.1:9000

# terminal 2 — mint a single-use token and join in one command, serve S3 on :9001
TOKEN=$(hamster cluster token -data-dir ./n1)
hamster cluster run -data-dir ./n2 -node n2 -listen 127.0.0.1:7956 -token "$TOKEN" -s3 127.0.0.1:9001

# terminal 3 — same again, serve S3 on :9002
TOKEN=$(hamster cluster token -data-dir ./n1)
hamster cluster run -data-dir ./n3 -node n3 -listen 127.0.0.1:7966 -token "$TOKEN" -s3 127.0.0.1:9002
```

`hamster cluster status -data-dir ./n1` shows every member and who leads. Point any S3 client at a node and the data is erasure-coded across all three; kill a node and the object still reads, reconstructed from the survivors.

## Operations

Two ways to run Hamster, and they don't convert in place.

**Single node.** `hamster serve` is a standalone S3 endpoint on one disk — no Raft, no inter-node TLS, no CA, nothing to configure. Right for a laptop, a homelab, or any workload that fits on one machine. Its durability is one disk's, and it **cannot become a cluster in place**: a `serve` node stores single-node blobs while a cluster stores erasure-coded shards, so there's nothing to promote. To grow, stand up a cluster and migrate the data over S3 (`rclone move`, which copies then deletes each object as it lands). That migration carries **current object data only** — version history and object-lock/WORM state (v0.6) do not transfer, and a COMPLIANCE-locked object can't be moved at all. **If you keep versioned or locked data, start clustered.**

**Cluster.** `hamster cluster init` founds a cluster — the CA is minted for you — and objects are erasure-coded `k+m` across the nodes, spread across failure domains and weighted by each node's capacity. The lifecycle below is online: no downtime, durability preserved throughout. A continuous background scrubber heals bitrot and lost shards on its own, before any read trips over them. (Grow, drain, replace, remove, and downsize are the v0.4 placement work.)

| Operation | How | What happens |
|---|---|---|
| **Add a node** | `cluster run -token …` | joins as a learner, auto-promoted to voter (five-voter cap); existing data migrates onto it at its current width — no reshape |
| **Grow into the new size** | `cluster optimize` | re-encodes existing data *up* to the larger cluster's profile, spreading objects written when it was smaller across the new nodes (run after adding nodes — never automatic) |
| **Reboot for maintenance** | just reboot it | erasure coding tolerates a node briefly down (a 4+2 object survives two); repair rebuilds whatever was written during the outage when it returns — no drain needed |
| **Take a node out of service** | `cluster drain <node>` | new writes steer off it and its shards migrate away; **reversible** with `cluster undrain` |
| **Replace a node** | `cluster run -token … -replaces <old>` | swaps a fresh node in for an existing one at the **same cluster size** — same profile, no re-encode |
| **Remove a node** | `cluster remove <node>` | evicts a drained, empty node for good (its ID is tombstoned — a return needs a fresh join) |
| **Shrink the cluster** | `cluster drain <node>` past a profile boundary | re-encodes every object down to the smaller profile (with a `[y/N]` showing the durability/efficiency trade), then `cluster remove` |
| **Recover from quorum loss** | `cluster recover` | rebuilds a cluster from one surviving node — the last resort |

Drain is reversible (undrain) and pairs with remove to decommission — the same split as `kubectl drain`/`uncordon` and `delete node`. A quick reboot needs neither: the erasure coding already covers a node being briefly down. (Two voters is a valid but failure-intolerant cluster; three is the first size that survives losing one.)

## Documentation

- [Glossary](docs/GLOSSARY.md) — the vocabulary (object, version, shard, stripe, partition, node, cluster, layout, …), grouped by layer. Start here if a term is unfamiliar.
- [Architecture](docs/ARCHITECTURE.md) — the system design narrative: request paths, metadata/data separation, erasure coding, placement, upgrades.
- [Architecture Decision Records](docs/adr/README.md) — one decision per file, with the reasoning and the rejected alternatives.
- [Roadmap](docs/ROADMAP.md) — the v0.x and v1.0 milestones.

## Contributing

Early, but contributions are welcome. Hamster is Apache 2.0 licensed, and contributions are accepted under a Developer Certificate of Origin (DCO). Sign your commits with `git commit -s`.

## License

Apache License 2.0. See [LICENSE](LICENSE).
