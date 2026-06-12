# ADR-0026: Stripe and shard layout — contiguous 256 KiB slices, self-describing shard files

## Status

Accepted, implemented (`internal/ec`).

## Context

Erasure coding ([ADR-0003](0003-erasure-coding-over-replication.md), [ADR-0013](0013-klauspost-reedsolomon.md)) needs a concrete data-plane format: how a framed object stream ([DATA-STREAM.md](../DATA-STREAM.md)) maps onto k+m shard files, and what a shard file looks like on disk. ERASURE-CODING.md deferred both to the v0.3 code; this ADR is that decision. The constraints: a multi-gigabyte PUT must encode with bounded memory, a ranged GET must touch only the bytes its range needs, repair must work on shards without keys or metadata gymnastics, and every format must be additively versioned (invariant 2).

## Decision

1. **Stripes of contiguous slices.** The frame is encoded in stripes: k slices of `slice_size` bytes each (256 KiB to start), plus m parity slices, encoded as the frame streams through. Within a stripe, slice `j` is the frame's bytes `[j·L, (j+1)·L)` — contiguous, not interleaved — so a byte range maps to specific data slices and a healthy-path read touches only the data shards its range covers. Shard file `i` is the concatenation of its slices across stripes, so a stripe's slice sits at a computable offset: stripe `s` of shard `i` is at `payload_start + s·slice_size`.

2. **The final stripe is short and zero-padded.** The last `frame_size mod k·slice_size` bytes form a stripe with slice length `ceil(rem/k)`, zero-padded so the k slices are equal (Reed-Solomon requires it). The padding is at most k−1 bytes plus slice rounding, never recorded as data: the frame knows its own end, and the shard header states the true frame size.

3. **Shard files are self-describing.** A shard file is `magic "HMS1" + u32 header length + versioned protobuf header + payload`. The header carries: format version, the object's data ID, the shard index, k, m, `slice_size`, and `frame_size`. Any k shard files are sufficient to reconstruct the object *and to know what they are* — a scrubber can identify a stray file, and a reader cross-checks every shard against its siblings (mixed sets and misplaced files are refused at open). Shard checksums (SHA-256 of the whole file, header included) live in the object's metadata (`VersionEntry.ShardChecksums`), never mirrored inside the file — a shard cannot vouch for itself.

4. **Slice size is recorded, not assumed.** 256 KiB balances sequential shard I/O against range-read amplification, and conveniently makes a `4+2` stripe (1 MiB) coincide with the frame's nominal chunk. Every shard records its own slice size, so retuning changes new writes only.

5. **Frame size is declared up front.** The encoder writes shard headers first, so it needs `frame_size` before the first byte; identity frames make it computable from the plaintext size (`stream.FrameSize`). When the compression transform arrives, frame size stops being predictable — that work will either stage the frame or move the field additively. Known, accepted, documented here so it surprises no one.

6. **Repair verifies in both directions.** `Reconstruct` streams survivor shards through their recorded checksums while rebuilding, and checks each rebuilt shard against its own recorded checksum — corruption is never laundered into fresh shards. A failed source checksum fails the whole pass; callers stage rebuilt shards and commit only on success.

## Consequences

- Bounded memory on both paths: one stripe (k+m slices, ~1.5 MiB at `4+2`) in flight while encoding; one stripe materialized per read, with a single-stripe cache absorbing sequential GETs whose 1 MiB frame chunks straddle stripe boundaries.
- The whole engine is pure computation over caller-supplied readers and writers — no seam, simulation-friendly, and the same code serves the gateway, repair, and the future re-encode task.
- k=1 writes (the small-object rule) produce parity slices that are byte-identical full copies, proven by test — the "replication is k=1 erasure coding" claim is mechanical fact, not analogy.
- Equal-length shard files (headers included, while shard indices stay below 128) make capacity accounting trivial.

## Alternatives considered

- **Interleaved (round-robin) striping** — slice `j` takes every k-th block. Spreads a small hot range across all data shards, but makes every range read touch every data shard, which is backwards for an object store's read patterns. Rejected.
- **Per-stripe checksums inside the shard file.** The frame already carries per-chunk CRC-32C and metadata carries whole-shard SHA-256; a third layer would buy little and bloat the format. Rejected — scrub reads whole shards anyway.
- **Frame size in a shard trailer instead of the header** (would free the encoder from knowing it up front). Costs every open an extra seek and the format a second locator; the up-front computation is free today and the field can move additively if compression demands it. Deferred, not needed.
- **Library-default stripe handling (`reedsolomon.Split` on the whole object).** Whole-object memory on the write path — exactly what the write buffer exists to avoid. Rejected.
