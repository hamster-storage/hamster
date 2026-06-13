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
| [v0.2](https://github.com/hamster-storage/hamster/releases/tag/v0.2.0) | Clustering — Raft-replicated metadata, mTLS between nodes, token-based join | ✅ |
| [v0.3](https://github.com/hamster-storage/hamster/releases/tag/v0.3.0) | Erasure-coded durability with self-healing repair, the S3 endpoint served from the cluster | ✅ |
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

`aws s3`, `rclone`, `restic`, and `s3cmd` work too — a [compatibility suite](test/compat/) runs all four against every change. `hamster serve` is a single durable node, the simplest way in; the [cluster below](#cluster-preview) now serves the same S3 API with objects erasure-coded across its nodes (v0.3). Still a dev preview either way — don't trust real data to it yet.

## Cluster preview

The cluster is Raft-replicated metadata (v0.2) plus an erasure-coded data path (v0.3): mutual TLS between nodes with zero TLS configuration, single-use join tokens, and — with `-s3` — the full S3 API on every node, objects spread `k+m` across the cluster and reconstructed from any `k`. Writes commit on the Raft leader for now (a non-leader answers `503` and clients retry); multipart and server-side copy join this path in a later release.

Three terminals, all sharing the credentials the S3 API will accept:

```sh
# in every terminal — the keys each node's S3 endpoint accepts (-s3 requires them)
export HAMSTER_ACCESS_KEY_ID=hamster HAMSTER_SECRET_ACCESS_KEY=keep-this-one-secret

# terminal 1 — found the cluster, serve S3 on :9000
hamster cluster init -data-dir ./n1 -node n1 \
  -listen-cluster 127.0.0.1:7946 -listen-join 127.0.0.1:7947
hamster cluster run -data-dir ./n1 -s3 127.0.0.1:9000

# terminal 2 — mint a single-use token and join in one command, serve S3 on :9001
TOKEN=$(hamster cluster token -data-dir ./n1)
hamster cluster run -data-dir ./n2 -node n2 \
  -listen-cluster 127.0.0.1:7956 -listen-join 127.0.0.1:7957 -token "$TOKEN" -s3 127.0.0.1:9001

# terminal 3 — same again, serve S3 on :9002
TOKEN=$(hamster cluster token -data-dir ./n1)
hamster cluster run -data-dir ./n3 -node n3 \
  -listen-cluster 127.0.0.1:7966 -listen-join 127.0.0.1:7967 -token "$TOKEN" -s3 127.0.0.1:9002
```

Watch it: `hamster cluster status -data-dir ./n1` shows every member and who leads. Nodes join as learners and are promoted to voters automatically (capped at five voters no matter how large the cluster grows). If a majority of voters is ever permanently lost, `hamster cluster recover` rebuilds a cluster from a survivor — read its warning first.

Now it stores objects, not just metadata. Point any S3 client at a node and the data is erasure-coded across all three:

```sh
export AWS_ACCESS_KEY_ID=hamster AWS_SECRET_ACCESS_KEY=keep-this-one-secret
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://vault
aws --endpoint-url http://127.0.0.1:9000 s3 cp video.mp4 s3://vault/
```

Kill a node and the object still reads — reconstructed from the survivors. Writes commit on the leader's node in v0.3, so if one answers `503 SlowDown`, retry against another.

## Deployment

There are two ways to run Hamster today, and they are separate paths — pick by whether you ever expect to grow.

**A single node — the simplest thing.** `hamster serve` is a standalone S3 endpoint backed by one node's disk (the [Quick start](#quick-start) above). No cluster machinery, no inter-node TLS, no certificate authority — nothing to configure. It is the right choice for a laptop, a homelab box, or any workload that fits on one machine and does not need to scale out. The trade-off: its durability is one disk's durability, and it cannot become a cluster in place (see below).

```sh
hamster serve -data-dir ./data            # one node, S3 on :9000
```

**A cluster — durable across machines, and able to grow.** `hamster cluster init` founds a cluster; `cluster init` mints the cluster CA once, automatically, and every node you add reuses it (the [Cluster preview](#cluster-preview) above shows three). Objects are erasure-coded `k+m` across the nodes and reconstructed from any `k`, so you can lose drives or whole machines without losing data. A one-node cluster is a valid starting point — it serves S3 just like `serve`, but on the path that scales:

```sh
hamster cluster init -data-dir ./n1 -node n1 \
  -listen-cluster 127.0.0.1:7946 -listen-join 127.0.0.1:7947
hamster cluster run -data-dir ./n1 -s3 127.0.0.1:9000    # one-node cluster, S3 on :9000
# later: mint a token on n1, then `cluster run -token …` on n2, n3, …
```

**Growing from one node to many.** A *cluster* grows by adding nodes — mint a join token, run the new node with `-token`, and it joins as a learner and is promoted automatically. Existing objects climbing from a single-node profile up to a wider `k+m` as the cluster gains capacity is the v0.4 placement work (in progress).

What is **not** supported is promoting a single-node `serve` deployment into a cluster: `serve` stores single-node blobs while a cluster stores erasure-coded shards, so there is no in-place conversion between them yet. **If you think you might ever grow beyond one machine, start with `cluster init` rather than `serve`** — a one-node cluster is cluster-ready from the first node and costs you nothing extra. In-place `serve` → cluster promotion is a recognized future convenience, but it is not yet scheduled to a release.

## Roadmap

- **v0.x** — core PUT and GET, erasure coding with repair, partitioned placement, versioning, object lock, the simulation harness, and the upgrade test suite. On disk and on wire formats may change between v0 releases.
- **v1.0** — stable formats with a compatibility promise, zero downtime rolling upgrades, and the web console.

## Contributing

Early, but contributions are welcome. Hamster is Apache 2.0 licensed, and contributions are accepted under a Developer Certificate of Origin (DCO). Sign your commits with `git commit -s`.

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Release history

High level only — details live in each [release](https://github.com/hamster-storage/hamster/releases). On disk and on wire formats may change between v0 releases.

- **v0.3** (June 2026) — Erasure coding and self-healing repair. Objects are erasure-coded into `k+m` self-describing shards spread across distinct nodes and reconstructed from any `k`; only the small metadata commit ever touches the Raft log. `hamster cluster run -s3` serves the full S3 API from the cluster, with the write-ack rule enforced mechanically (all `k+m` durable on the healthy path, a hard floor of `k+1`, `SlowDown` below it). A repair sweep scrubs every shard against its replicated checksum and rebuilds missing or bit-rotted shards from any `k` verified survivors, without anyone reading the object first. The whole data path runs under the deterministic simulation harness, and a six-node e2e kills nodes mid-workload over real sockets. Leader-only writes; multipart and server-side copy land on this path later.
- **v0.2** (June 2026) — Clustering foundations. The Raft-replicated metadata plane as a runnable preview: `hamster cluster` (init, token, join, run, status, recover), mutual TLS between nodes with no plaintext mode and zero TLS configuration, single-use CA-pinned join tokens, automatic learner-to-voter promotion under a five-voter cap, crash-safe log compaction with streamed snapshot catch-up, and disaster recovery from a surviving node. Deterministic election timing makes the whole consensus layer simulation-testable, and an e2e suite drives the real binary through the full lifecycle. S3 serving stays single-node until the data path replicates (v0.3).
- **v0.1** (June 2026) — The single-node store. Core S3 API: objects, listings, multipart uploads, server-side copies, batch deletes, presigned URLs; full SigV4 authentication including `aws-chunked` streaming; path-style and virtual-hosted addressing; MD5 ETags, exactly like S3. Uploads stream through the write buffer (a 1 GiB PUT needs ~12 MB of server memory). Durable single-node storage: BadgerDB metadata, versioned protobuf formats with golden-pinned encodings. Verified by a third-party client compatibility suite (`aws` CLI, rclone, restic, s3cmd) and a deterministic simulation harness that crash-tests the store against a reference model. Dev preview — single node, not production ready.
