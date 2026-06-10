# Hamster

**S3 compatible object storage in a single binary. Stuff it in, pull it out later.**

Hamster is a self hosted, S3 compatible object store built around one idea: object storage should be simple to run and safe with your data, without forcing you into a heavyweight distributed system or a restrictive license.

> **Status: early development (v0). Not production ready.**
> The design is settled and the core is being built in the open. Please do not trust real data to Hamster yet. Star or watch the repo to follow progress toward v1.

## Why Hamster

The self hosted S3 landscape shifted in 2026. MinIO archived its community edition and steered users toward a commercial product. The remaining open source options each ask you to give something up: SeaweedFS is capable but carries a lot of moving parts, and Garage is wonderfully simple but skips versioning and object lock.

Hamster aims for the missing middle:

- The simplicity of a single binary you can run anywhere.
- The durability of erasure coding, so storage stays cheap without giving up safety.
- The compliance features that lightweight stores leave out: versioning, object lock, and WORM retention.
- A permissive Apache 2.0 license, so you can build on it without legal friction.

## Design principles

- **Single binary, no external dependencies.** Run it on a laptop, a VPS, or a cluster. No ZooKeeper, no etcd, no separate database to operate.
- **S3 compatible.** Works with existing S3 SDKs, CLIs, and tools.
- **Durable by default.** Reed Solomon erasure coding spreads each object across independent failure domains, so you can lose drives or whole nodes without losing data.
- **Grows smoothly.** Partitioned placement rebalances as you add capacity. Add a node, and data redistributes without reshaping the cluster.
- **Safe to upgrade.** Versioned on disk and on wire formats with a backwards compatible upgrade path and zero downtime rolling upgrades, validated by end to end upgrade tests.
- **Trustworthy.** Durability and consistency are exercised under a deterministic simulation harness that injects partitions, disk failures, and reordering, so correctness is tested rather than hoped for.

## Features (target for v1)

- Core S3 API: buckets, objects, multipart upload, presigned URLs, prefix listing
- Erasure coded durability with self healing repair
- Object versioning
- Object lock and WORM retention (GOVERNANCE and COMPLIANCE modes)
- Partitioned placement with online rebalancing
- A clean, friendly web console

## The Hamster dialect

The metaphor is load bearing, not just decoration:

- **Warren** — a cluster. A connected system of burrows.
- **Burrow** — a single node.
- **Pouch** — the write buffer. Writes land in the pouch first, then get flushed and erasure coded into the burrow, the way a hamster fills its cheeks before stashing food away.
- **Hoard** — the set of objects you have stored.

## Quick start

Not available yet. The first v0 release will ship a single binary. The intended experience looks roughly like:

```sh
# illustrative, not yet working
hamster burrow init --data ./nest      # start a node
hamster warren join <addr>             # join it into a cluster
```

Hamster will expose a standard S3 endpoint, so any S3 client can point at it.

## Roadmap

- **v0.x** — core PUT and GET, erasure coding with repair, partitioned placement, versioning, object lock, the simulation harness, and the upgrade test suite. On disk and on wire formats may change between v0 releases.
- **v1.0** — stable formats with a compatibility promise, zero downtime rolling upgrades, and the web console.

## Contributing

Early, but contributions are welcome. Hamster is Apache 2.0 licensed, and contributions are accepted under a Developer Certificate of Origin (DCO). Sign your commits with `git commit -s`.

## License

Apache License 2.0. See [LICENSE](LICENSE).
