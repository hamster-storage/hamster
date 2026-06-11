# The Framed Object Stream

This document designs the byte format that sits between the S3 gateway and erasure coding: how object data is chunked, optionally compressed, optionally encrypted, and framed before it is sharded. Companion ADRs: [ADR-0021](adr/0021-envelope-encryption-at-rest.md) (encryption at rest) and [ADR-0013](adr/0013-klauspost-reedsolomon.md) (erasure coding).

> **Status: design document.** Nothing here is implemented. The v0.1 blob store writes raw object bytes; the framed stream must land **before v0.3 freezes the shard layout**, because retrofitting a frame under existing shards would be a format migration this design exists to avoid.

## Why one format carries both transforms

Compression and encryption at rest look like independent features, but they impose the same structural requirement: **Range requests need random access into transformed data.** A `Range: bytes=5000000-` against a zstd stream or a single AES-GCM blob means decompressing or decrypting from byte zero. Every serious implementation solves this the same way — split the object into chunks, transform each chunk independently, and keep an index — so the chunking, framing, and index are designed once, here, and both transforms become per-chunk steps inside it.

Ordering is fixed and load bearing:

```
object bytes → chunk → compress? → encrypt? → frame → erasure code → shards
```

- **Compress before encrypt**, always: ciphertext does not compress.
- **Encrypt before erasure coding**, always: shards are ciphertext, and shard checksums cover ciphertext, so **repair, rebalance, and re-encode never need keys or plaintext**. The repair path designed in [ERASURE-CODING.md](ERASURE-CODING.md) is unchanged by encryption — a storage node holding shards can verify and rebuild them while knowing nothing.

## The pipeline

1. **Chunk.** The plaintext object is split into fixed-size chunks (nominal 1 MiB; the last chunk is short). The chunk size is recorded in the frame header, not assumed.
2. **Compress (optional).** Each chunk is compressed independently. If a chunk does not shrink, it is stored raw and flagged — incompressible data costs a flag bit, not a pessimization.
3. **Encrypt (optional).** Each chunk is encrypted with AES-256-GCM under the object's data key. The nonce is the chunk index — safe because every object has a fresh random data key, never reused ([ADR-0021](adr/0021-envelope-encryption-at-rest.md)) — which keeps the write path deterministic under the simulator: the only randomness is the key itself.
4. **Frame.** Header + transformed chunks + trailer index (below).
5. **Erasure code.** The framed stream is what `k+m` sharding splits ([ERASURE-CODING.md](ERASURE-CODING.md)). To the EC layer the frame is opaque bytes.

The stream is **always framed once this format ships, even when both transforms are off** — an identity frame costs a few dozen bytes and buys exactly one read path. Per-object transform flags live in the frame header *and* in the `VersionEntry` (additive fields), following the same rule as EC parameters: the active configuration says what new writes do; the per-object record says what old objects are, forever.

## Frame layout

All multi-byte integers little-endian; the header is a versioned structure per the additive-formats invariant (CLAUDE.md invariant 2).

```
header:
  magic            4 bytes   "HMF1"
  format_version   varint    (1)
  flags            varint    bit 0: compressed, bit 1: encrypted
  chunk_size       varint    plaintext bytes per chunk (last may be short)
  plaintext_size   varint    total object bytes

chunks:
  chunk[0..n-1]              transformed chunk bytes (GCM tag included when encrypted)

trailer:
  chunk_lengths    n varints stored length of each chunk
  trailer_size     4 bytes   length of the trailer, so it can be found from the end
```

- With no compression, chunk offsets are computable and the trailer is degenerate; it is kept anyway — one parser, no special case.
- A Range request maps `[start, end]` to chunk indices through `chunk_size`, seeks via the trailer, and transforms only the chunks it touches.
- The GCM tag authenticates each encrypted chunk individually; a corrupted chunk fails decryption loudly rather than decoding to garbage.

## Checksums and integrity

Three layers, each with a distinct job (see [ADR-0019](adr/0019-md5-etags.md)):

| Checksum | Over | Job |
|---|---|---|
| ETag (MD5) | plaintext | S3 client compatibility, never integrity |
| `ObjectChecksum` (SHA-256, in `VersionEntry`) | plaintext | end-to-end read verification after decrypt/decompress |
| Shard checksums (in `VersionEntry`) | ciphertext frame | repair and scrub without keys |
| GCM auth tags (in the frame) | each encrypted chunk | per-chunk authenticity, Range reads included |

A full GET verifies `ObjectChecksum`; a Range GET relies on the per-chunk GCM tags (encrypted objects) or per-chunk verification via the frame (a per-chunk plaintext checksum for unencrypted objects is an open question below).

## Open questions

- Chunk size: 1 MiB is the placeholder; measure against EC stripe sizes before v0.3 — the frame chunk and the EC stripe do not have to coincide, but pathological interactions should be ruled out.
- Compression codec: `klauspost/compress` offers zstd (better ratio) and s2 (faster); pick one default, record per-object, both decodable forever.
- Unencrypted Range integrity: add a small per-chunk CRC to the trailer, or accept that only full GETs verify on unencrypted objects?
- SSE-C (client-supplied keys): the frame and DEK machinery support it naturally — the wrap key comes from the request instead of the cluster KEK — but it is not scheduled.
- Whether multipart parts are framed independently (each part is already encoded independently per [S3-API.md](S3-API.md)) — almost certainly yes, the part boundary is a natural frame boundary.
