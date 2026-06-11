# S3 API Surface

This document defines Hamster's S3 compatibility: which operations exist, when they arrive, how authentication works, and what the semantics promise. Companion ADRs: [ADR-0018](adr/0018-sigv4-auth.md) (authentication) and [ADR-0019](adr/0019-md5-etags.md) (ETags).

> **Status: design document.** Nothing here is implemented. Operations are commitments of intent, scheduled against [ROADMAP.md](ROADMAP.md).

## What "S3 compatible" means here

Compatibility is defined by **real tools, not the full AWS surface**: the acceptance bar is that `aws` CLI, rclone, restic, s3cmd, and the major SDKs (Go, Python, JS) work against Hamster without special flags. AWS S3 has hundreds of operations; the set below is what those tools actually call. Everything outside it returns a proper S3 `NotImplemented` error, honestly.

## The v0.1 surface

v0.1 is the single-node store, and it ships a *complete usable* S3 core — including multipart, because every SDK switches to multipart automatically above ~8 MiB and a store that breaks `aws s3 cp` on a video file is a toy.

| Group | Operations | Notes |
|---|---|---|
| Service | `ListBuckets` | |
| Bucket | `CreateBucket`, `DeleteBucket`, `HeadBucket`, `GetBucketLocation` | Delete requires empty. Location returns the configured region string. |
| Listing | `ListObjectsV2`, `ListObjects` (V1) | Both: V1 costs little and old tools still call it. Prefix, delimiter, continuation tokens — pure `c/` keyspace scans ([METADATA.md](METADATA.md)). |
| Object | `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects`, `CopyObject` | `GetObject` supports `Range`. v0.1 `CopyObject` is a full server-side read-and-rewrite — shard sharing between versions would need GC refcounting; an optimization for later, not a semantic change. |
| Multipart | `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads`, `ListParts` | Upload state lives under the reserved `u/` metadata prefix. Each part is encoded independently; `CompleteMultipartUpload` is one metadata transaction assembling the part list into a version entry — the linearization point, like any PUT. `UploadPartCopy` deferred. |
| Auth | SigV4: `Authorization` header, presigned query URLs, `UNSIGNED-PAYLOAD`, and `aws-chunked` streaming signatures | See [Authentication](#authentication). Presigned GET/PUT URLs fall out of query-string SigV4 for free. |

## Arrival schedule for the rest

API groups land with the release that builds their machinery, matching the [roadmap](ROADMAP.md):

| Release | API additions |
|---|---|
| v0.5 | Versioning: `PutBucketVersioning`, `GetBucketVersioning`, `ListObjectVersions`, `GetObject`/`HeadObject`/`DeleteObject` with `versionId`. The metadata already models version lists from v0.1; this release exposes them. |
| v0.6 | Object lock: `PutObjectLockConfiguration`, `GetObjectLockConfiguration`, `PutObjectRetention`, `GetObjectRetention`, `PutObjectLegalHold`, `GetObjectLegalHold`, plus the `x-amz-object-lock-*` headers on PUT. |
| v0.x later | Tagging (`Put/Get/DeleteObjectTagging`), `x-amz-checksum-*` additional checksums, `UploadPartCopy`, lifecycle expiration (a deliberately small subset). |

## Authentication

**SigV4, implemented in-house on the standard library** ([ADR-0018](adr/0018-sigv4-auth.md)). Server-side verification is the canonical-request/HMAC-chain dance — a few hundred lines of `crypto/hmac` + `crypto/sha256`, validated against AWS's published test vectors. Pulling the AWS SDK in for it would import a client-oriented dependency tree to avoid writing the one part that was always going to be our job: verification.

- **Supported modes (all v0.1):** `Authorization` header signing; presigned query URLs; `UNSIGNED-PAYLOAD`; `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` chunked signing (the `aws` CLI uses it on plain HTTP, which is exactly how homelabs run).
- **Credentials:** an access-key/secret pair created at cluster init (printed once, stored as replicated metadata). Additional keys via `hamster admin key create`. In v0.x every key is full-access; per-key/bucket policies are deliberately later — IAM is a swamp entered slowly.
- **Region:** a configured string (default `us-east-1`, because that is what every tool assumes when unset). It exists for SigV4 scope-matching and `GetBucketLocation`; it implies nothing about geography.
- **Anonymous mode:** off by default; an explicit `--insecure-no-auth` flag for throwaway dev instances, named so nobody mistakes it.

## ETags and checksums

**ETags are MD5, exactly as S3 clients expect** ([ADR-0019](adr/0019-md5-etags.md)): for a single-part PUT, the MD5 of the body; for multipart, the MD5 of the concatenated part MD5s suffixed `-N`. This is compatibility, not integrity — rclone and restic actively verify these formats, and breaking them breaks sync tooling silently. Hamster's *real* integrity story is internal and modern: the whole-object and per-shard checksums in the metadata record ([METADATA.md](METADATA.md)), verified on every read and repair. MD5 here is a wire-format obligation, never a security boundary. The `x-amz-checksum-*` family (SHA-256, CRC32C) arrives later, additively.

## Semantics and limits

- **Consistency:** strong read-after-write and list-after-write, from any node — the metadata layer gives it outright, so the API promises it.
- **Limits (S3 parity):** single PUT up to 5 GiB; parts 5 MiB–5 GiB, max 10,000 parts; keys up to 1024 bytes of UTF-8 — minus the literal NUL byte, Hamster's one documented key deviation ([METADATA.md](METADATA.md)), rejected as `400 InvalidObjectName`.
- **Errors:** S3's XML error envelope with standard codes (`NoSuchBucket`, `NoSuchKey`, `SignatureDoesNotMatch`, `BucketNotEmpty`, `SlowDown` for the write-floor refusal in [ERASURE-CODING.md](ERASURE-CODING.md)), so SDK retry and error mapping behave.
- **Bucket names:** AWS rules (3–63 chars, `[a-z0-9.-]`, DNS-label shaped). Virtual-hosted *and* path-style addressing both supported; path-style is the homelab default since it needs no wildcard DNS.

## Non-goals

Stated so nobody waits for them: website hosting (a reverse proxy like Caddy in front of a bucket is a better static-site host than S3's website mode ever was — index documents, redirects, and TLS are a web server's job, not a storage API's), S3 Select, inventory/analytics, BitTorrent, Requester Pays, access points, ACLs (legacy even on AWS — new buckets have had them disabled by default since 2023, with bucket policies as the recommended replacement, which is the path Hamster may take later; common no-op headers like `x-amz-acl: private` are accepted and ignored so tools don't break), and cross-region replication as an API (multi-region is a future cluster-to-cluster feature, not a bucket flag).

## Open questions

- Which lifecycle subset is worth having (expiration almost certainly; transitions probably never — there is no second storage class to transition to yet).
- Bucket policy model and scope — decided when multi-key authorization becomes real, not before.
- Whether `Content-MD5` validation on PUT is enforced or advisory when the client supplies it (leaning enforced — free integrity).
