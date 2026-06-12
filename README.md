# Hamster

**S3 compatible object storage in a single binary. Stuff it in, pull it out later.**

<img width="1280" height="640" alt="hammy" src="https://github.com/user-attachments/assets/aafa0809-a736-4c9f-9dc6-1f6411e02d5b" />

Hamster is a self hosted, S3 compatible object store built around one idea: object storage should be simple to run and safe with your data, without forcing you into a heavyweight distributed system or a restrictive license.

> **Status: early development (v0). Not production ready.**
> The design is settled and the core is being built in the open. Please do not trust real data to Hamster yet. Star or watch the repo to follow progress toward v1.

## Why Hamster

The self hosted S3 landscape shifted in 2026 when MinIO archived its community edition and steered users toward a commercial product. The open source stores that remain are good software, but they cluster at two ends: feature-rich systems that bring real operational weight, and admirably simple ones that leave out the features regulated data can't live without.

Hamster aims for the missing middle:

- The simplicity of a single binary you can run anywhere.
- The durability of erasure coding, so storage stays cheap without giving up safety.
- The compliance features that simpler stores skip: versioning, object lock, and WORM retention — the controls that retention and audit regimes (think HIPAA or SEC 17a-4) actually ask for.
- A permissive Apache 2.0 license, so you can build on it without legal friction.

## Design principles

- **Single binary, no external dependencies.** Run it on a laptop, a VPS, or a cluster. No ZooKeeper, no etcd, no separate database to operate.
- **S3 compatible.** Works with existing S3 SDKs, CLIs, and tools.
- **Durable by default.** Reed Solomon erasure coding spreads each object across independent failure domains, so you can lose drives or whole nodes without losing data.
- **Grows smoothly.** Partitioned placement rebalances as you add capacity. Add a node, and data redistributes without reshaping the cluster.
- **Safe to upgrade.** Versioned on disk and on wire formats with a backwards compatible upgrade path and zero downtime rolling upgrades, validated by end to end upgrade tests.
- **Trustworthy.** Durability and consistency are exercised under a deterministic simulation harness that injects partitions, disk failures, and reordering, so correctness is tested rather than hoped for.

## Features

High level and honest: a check mark means shipped and tested, not promised. Versions beyond that are the [roadmap](docs/ROADMAP.md)'s plan and may shift as the code pushes back.

| Version | Features | Status |
|---|---|---|
| [v0.1](https://github.com/hamster-storage/hamster/releases/tag/v0.1.0) | <ul><li>Core S3 API — buckets, objects, listings, multipart, presigned URLs, SigV4 auth (verified with <code>aws</code>, <code>rclone</code>, <code>restic</code>, <code>s3cmd</code>)</li><li>Durable single-node store with streaming uploads</li></ul> | ✅ |
| v0.2 | Clustering — Raft-replicated metadata, mTLS between nodes, token-based join | 🚧 in progress |
| v0.3 | Erasure-coded durability with self-healing repair | planned |
| v0.4 | Partitioned placement and online rebalancing | planned |
| v0.5 | Object versioning | planned |
| v0.6 | Object lock and WORM retention (GOVERNANCE and COMPLIANCE modes) | planned |
| v0.7 | Encryption at rest (SSE-S3) | planned |
| v0.8+ | Zero-downtime rolling upgrades | planned |
| v1.0 | <ul><li>Web console</li><li>Stable formats with a compatibility promise</li></ul> | planned |

## Quick start

Grab a binary from the [releases page](https://github.com/hamster-storage/hamster/releases) (or `go build ./cmd/hamster` with Go installed — no cgo, no build tricks), then start the server. The `HAMSTER_*` variables define the credentials it will accept:

```sh
export HAMSTER_ACCESS_KEY_ID=hamster
export HAMSTER_SECRET_ACCESS_KEY=keep-this-one-secret
hamster serve -data-dir ./data
```

That is a standard S3 endpoint on `127.0.0.1:9000`, so any S3 client works as is. The client sends its own credentials — the standard `AWS_*` variables, not the `HAMSTER_*` ones — set to the same values:

```sh
export AWS_ACCESS_KEY_ID=hamster
export AWS_SECRET_ACCESS_KEY=keep-this-one-secret
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://stash
aws --endpoint-url http://127.0.0.1:9000 s3 cp video.mp4 s3://stash/
```

## Clients

`aws s3`, `rclone`, `restic`, and `s3cmd work too` — a [compatibility suite](test/compat/) runs all four against every change. The v0.1 server is a single durable node (this is the dev preview: real workloads should wait for clustering and erasure coding); `hamster cluster` commands arrive with v0.2.

## Roadmap

- **v0.x** — core PUT and GET, erasure coding with repair, partitioned placement, versioning, object lock, the simulation harness, and the upgrade test suite. On disk and on wire formats may change between v0 releases.
- **v1.0** — stable formats with a compatibility promise, zero downtime rolling upgrades, and the web console.

## Contributing

Early, but contributions are welcome. Hamster is Apache 2.0 licensed, and contributions are accepted under a Developer Certificate of Origin (DCO). Sign your commits with `git commit -s`.

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Release history

High level only — details live in each [release](https://github.com/hamster-storage/hamster/releases). On disk and on wire formats may change between v0 releases.

- **v0.1** (June 2026) — The single-node store. Core S3 API: objects, listings, multipart uploads, server-side copies, batch deletes, presigned URLs; full SigV4 authentication including `aws-chunked` streaming; path-style and virtual-hosted addressing; MD5 ETags, exactly like S3. Uploads stream through the write buffer (a 1 GiB PUT needs ~12 MB of server memory). Durable single-node storage: BadgerDB metadata, versioned protobuf formats with golden-pinned encodings. Verified by a third-party client compatibility suite (`aws` CLI, rclone, restic, s3cmd) and a deterministic simulation harness that crash-tests the store against a reference model. Dev preview — single node, not production ready.
