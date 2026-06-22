# ADR-0038: Erasure-coded multipart and cluster data-path parity

## Status

Accepted. A technical enabler of [ADR-0036](0036-one-clustered-path.md) (one
clustered path); ships in the same release (v0.11).

## Context

The cluster erasure-coded path ([`internal/coord`](../../internal/coord/) behind the
gateway's `ObjectBackend`) handles **whole objects**: `Put(body []byte)` places,
encodes, transfers, and commits in one shot, and `Get` returns the whole object as
`[]byte`. Relative to the single-node `serve` path it is missing four things:

- **Streaming** — both `coord.Put` and the gateway materialize the whole object in
  memory (`Put` takes `[]byte`; `Get` returns `[]byte`), so a multi-GB upload that is
  fine on a single node buffers whole in RAM on a cluster.
- **Range efficiency** — `coord.Get` already takes `off, length` and prefetches only
  the covering shard ranges (`stream.Cover`), but the gateway `ObjectBackend.Get`
  interface discards the range and fetches the whole object.
- **Server-side copy** — `CopyObject`/`UploadPartCopy` are refused (501).
- **Multipart** — `CreateMultipartUpload` is refused (501), so the whole group is
  unreachable.

[ADR-0036](0036-one-clustered-path.md) makes this the **only** data path, so it must
reach parity with `serve` before `serve` can retire. The metadata apply side for
multipart already exists (the `u/` keyspace, `UploadRecord`/`PartRecord`,
`ApplyCreateMultipartUpload`/`UploadPart`/`Complete`/`Abort` — [METADATA.md](../METADATA.md));
it is the **data path** for parts that is unbuilt.

## Decision

1. **Streaming PUT.** `ObjectBackend.Put` and `coord.Put` take `io.Reader`+size,
   pacing the body through the existing `stream → ec → datapath` pump without
   materializing it. The pump is already window-bounded; the change is the boundary —
   the gateway and `coord` stop buffering `[]byte`. Bounded memory at any size,
   restoring `serve`'s promise on the cluster path.

2. **Range-efficient GET.** The gateway `ObjectBackend` gains
   `Get(bucket, key, off, length)` and plumbs the HTTP `Range` into the existing
   `coord.Get(off, length)`. Whole-object GET is the `off=0, length=all` case. A
   streaming GET writer (decode chunks straight to the `ResponseWriter`) is the
   matching read-side bounded-memory step.

3. **Server-side `CopyObject` / `UploadPartCopy`.** The coordinator reads the source
   through the EC read path and re-encodes to the destination — streaming,
   server-side, no client round-trip. The destination is an ordinary single-part
   object (ETag = MD5 of the bytes), matching S3 and the single-node path.

4. **Erasure-coded multipart — the core design.** Each part is its own EC unit,
   encoded and made **durable on upload** (parts must survive before the object's
   total size or final geometry is known), recorded under the existing `u/` prefix:
   - **`UploadPart`** is a small whole-object write — placement, `k+m` shards on
     `k+m` distinct nodes, the durability ack rule — recorded as a `PartRecord` with
     its size, MD5 ETag, and shard locations.
   - **`CompleteMultipartUpload`** is one metadata transaction assembling the ordered
     part list into a version entry whose `Slice` index references each part's shards
     in order — the linearization point, like any PUT — with **no whole-object
     re-encode**. Parts are not concatenated and re-sharded; the version entry points
     at the parts where they already live.
   - **GET** stitches parts in order; a **Range** read maps the requested span to the
     covering parts and, within each, to the covering shard ranges.
   - The composite `-N` ETag (MD5 of concatenated part MD5s) is computed at Complete,
     matching S3 and what rclone/restic verify ([ADR-0019](0019-md5-etags.md)).

   This mirrors the single-node model — "each part encoded independently; Complete
   assembles the list" — on the EC path, which is exactly why it needs no re-encode.

## Consequences

- **The cluster path becomes a strict superset of the retired single-node path:**
  `aws s3 cp` on a large file (automatic multipart), server-side copy, and large
  streaming uploads all work against a cluster. [ADR-0036](0036-one-clustered-path.md)
  can drop `serve`.
- **Per-part geometry.** A multipart object's parts may carry different EC geometry
  than a single-shot object of the same size (each part is sized independently). The
  version entry's `Slice` index already addresses per-slice geometry; reads resolve
  per part. Repair/scrub/re-encode generalize to operate per part — each part's shards
  verified and rebuilt against its recorded checksums.
- **Independent part durability** means an aborted or never-completed upload leaves
  orphan part shards, reclaimed by the existing GC; `AbortMultipartUpload` drops the
  `u/` rows and their shards.
- **New formats** — the `PartRecord` shard locations and the version entry's multipart
  `Slice` index — are additively versioned protobuf (invariant 2) and golden-pinned.
- Proven under simulated cluster schedules per invariant 5: a multipart upload with
  crashed part-holders, Complete after a leadership change, a Range read spanning a
  part boundary, and repair of a single part's shards.

## Alternatives considered

- **Buffer the whole multipart object and EC it once at Complete.** Rejected: defeats
  the point of multipart (parts must be durable on upload, before the total size is
  known) and re-buffers gigabytes.
- **One stripe layout spanning all parts, parts as byte-ranges into it.** Rejected:
  parts arrive and must be durable independently and out of order, before the final
  size and geometry are known; a single spanning layout cannot be sized at first-part
  time without a re-encode at Complete.
- **Keep Range reads decoding the whole object (today's behavior).** Rejected:
  large-object Range reads are a core S3 access pattern (video seeking, restic
  `check --read-data`); `coord.Get` already supports covering-range fetch, so
  discarding it at the gateway leaves working efficiency on the floor.
