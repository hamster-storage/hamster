# S3 API Surface

This document defines Hamster's S3 compatibility: which operations exist, when they arrive, how authentication works, and what the semantics promise. Companion ADRs: [ADR-0018](adr/0018-sigv4-auth.md) (authentication) and [ADR-0019](adr/0019-md5-etags.md) (ETags).

> **Status: partially implemented.** The gateway (`internal/gateway`) serves SigV4-authenticated bucket CRUD, PutObject/GetObject (with Range)/HeadObject/DeleteObject, both ListObjects versions, the full multipart group (`CreateMultipartUpload` through `ListMultipartUploads`, with composite `-N` ETags per [ADR-0019](adr/0019-md5-etags.md)), `CopyObject`/`UploadPartCopy` (read-and-rewrite, with the COPY/REPLACE metadata directive and `x-amz-copy-source-range`; conditional and versioned sources are refused until their machinery exists), and `DeleteObjects` (per-key outcomes, quiet mode) — verified end to end by the [compatibility suite](../test/compat/) (`task compat`) against the real `aws` CLI (including its default `aws-chunked` streaming uploads, automatic multipart split, and server-side `s3 cp` copies), rclone (`check` hash verification, sync, its own multipart path), restic (init/backup/`check --read-data`/restore over minio-go), and s3cmd. `Content-MD5` is verified whenever a client supplies it, and both addressing styles work (virtual-hosted needs the server's base domain via `hamster serve -domain`; path-style always works). That completes the v0.1 surface table below. Upload bodies stream to disk through the write buffer — bounded memory at any object size, with the 5 GiB single-payload limit enforced as `EntityTooLarge`; GET and server-side copy still buffer whole, until the framed data stream's per-chunk integrity ([DATA-STREAM.md](DATA-STREAM.md)) makes verify-while-serving possible. Everything else remains a commitment of intent, scheduled against [ROADMAP.md](ROADMAP.md).

## What "S3 compatible" means here

Compatibility is defined by **real tools, not the full AWS surface**: the acceptance bar is that `aws` CLI, rclone, restic, s3cmd, and the major SDKs (Go, Python, JS) work against Hamster without special flags. AWS S3 has hundreds of operations; the set below is what those tools actually call. Everything outside it returns a proper S3 `NotImplemented` error, honestly.

This bar is executable: `task compat` runs the real CLIs (`aws`, rclone, restic, s3cmd — four independent SigV4 and XML implementations) against an in-process gateway, one test file per tool, in [`test/compat`](../test/compat/). A tool that is not installed skips rather than fails. The suite is separate from `task test` by design — it shells out to external binaries with real clocks and sockets, the opposite of the hermetic main suite — and a known gap is committed as an explicit skip or accommodation with the reason in a comment (currently one: the `aws` CLI's `x-amz-checksum-*` default is pinned to `when_required` until the checksum family lands).

## The v0.1 surface

v0.1 is the single-node store, and it ships a *complete usable* S3 core — including multipart, because every SDK switches to multipart automatically above ~8 MiB and a store that breaks `aws s3 cp` on a video file is a toy.

| Group | Operations | Notes |
|---|---|---|
| Service | `ListBuckets` | |
| Bucket | `CreateBucket`, `DeleteBucket`, `HeadBucket`, `GetBucketLocation` | Delete requires empty. Location returns the configured region string. |
| Listing | `ListObjectsV2`, `ListObjects` (V1) | Both: V1 costs little and old tools still call it. Prefix, delimiter, continuation tokens — pure `c/` keyspace scans ([METADATA.md](METADATA.md)). |
| Object | `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects`, `CopyObject` | `GetObject` supports `Range`. v0.1 `CopyObject` is a full server-side read-and-rewrite — shard sharing between versions would need GC refcounting; an optimization for later, not a semantic change. |
| Multipart | `CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads`, `ListParts` | Upload state lives under the `u/` metadata prefix ([METADATA.md](METADATA.md)). Each part is encoded independently; `CompleteMultipartUpload` is one metadata transaction assembling the part list into a version entry — the linearization point, like any PUT. |
| Auth | SigV4: `Authorization` header, presigned query URLs, `UNSIGNED-PAYLOAD`, and `aws-chunked` streaming signatures | See [Authentication](#authentication). Presigned GET/PUT URLs fall out of query-string SigV4 for free. |

## Arrival schedule for the rest

API groups land with the release that builds their machinery, matching the [roadmap](ROADMAP.md):

| Release | API additions |
|---|---|
| v0.5 | Versioning, on both the single-node gateway and the cluster data path: `PutBucketVersioning`/`GetBucketVersioning` (Enabled/Suspended), `x-amz-version-id` on PUT and the versioned reads/deletes, `GetObject`/`HeadObject`/`DeleteObject` with `versionId` (a delete marker answers `405 MethodNotAllowed` with `x-amz-delete-marker`; the null version is `versionId=null`), `ListObjectVersions`, and permanent version delete that frees the version's data. The metadata modeled version lists from v0.1, so no schema change. **MFA Delete is out of scope** (object lock, v0.6, is the WORM mechanism); each version stores independent shards — no cross-version sharing. |
| v0.6 | Object lock, on both the single-node gateway and the cluster path: `Put/GetObjectLockConfiguration` (bucket default retention in days/years), `Put/GetObjectRetention`, `Put/GetObjectLegalHold`, the `x-amz-object-lock-*` PUT and response headers, and creating a bucket with object lock enabled (which enables versioning). GOVERNANCE retention yields to `x-amz-bypass-governance-retention`; **COMPLIANCE retention and legal holds are absolute** — no path deletes or shortens them (invariant 4). |
| v0.7 | Encryption at rest ([ADR-0021](adr/0021-envelope-encryption-at-rest.md)): `x-amz-server-side-encryption: AES256` reported on PUT/HEAD/GET when the cluster encrypts (SSE-S3 semantics; the key is cluster-managed, not per-request). SSE-KMS and SSE-C deferred. |
| v0.x later | Tagging (`Put/Get/DeleteObjectTagging`), `x-amz-checksum-*` additional checksums, conditional copies (`x-amz-copy-source-if-*`), lifecycle expiration (a deliberately small subset). |

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
- **Bucket names:** AWS rules (3–63 chars, `[a-z0-9.-]`, DNS-label shaped). Virtual-hosted *and* path-style addressing both supported; path-style is the homelab default since it needs no wildcard DNS. Virtual-hosted addressing is enabled by telling the server its base domain (`hamster serve -domain s3.example.com`): a request for `Host: bucket.s3.example.com` addresses that bucket, every label before the domain belongs to the bucket name (dots included), and any other Host — the bare domain, an IP — is path-style.

## Object lock — what is enforced, and what is not

Object lock (v0.6) implements S3's retention and legal-hold *mechanism*, and the COMPLIANCE guarantee is real. But AWS's object-lock model assumes things Hamster v0 does not yet have, and a compliance story has to account for them honestly.

- **COMPLIANCE mode is absolute, and tested.** No request — no header, no flag, no operator, no "root" — can delete or shorten a COMPLIANCE-locked version before its retention expires, and a legal hold blocks deletion with no expiry. This is invariant 4, enforced in the metadata apply layer (the one layer no caller can bypass) and proven by an adversarial test that drives every delete-or-shorten path and asserts refusal.
- **GOVERNANCE mode is not access-controlled.** AWS gates the governance bypass behind an IAM permission (`s3:BypassGovernanceRetention`); Hamster has no per-user authorization yet (every credential is full-access — see [Authentication](#authentication)), so `x-amz-bypass-governance-retention: true` is honored for any authenticated request. Governance still requires the explicit bypass flag, so it is real soft-delete protection — but it is not the access-controlled gradient AWS describes. This gap closes only when per-key authorization (bucket policies) lands, which is not yet scheduled.
- **No bucket-policy retention bounds.** S3's minimum/maximum allowable retention periods are a bucket-policy feature; Hamster has no bucket policies, so only the per-bucket *default* retention (`PutObjectLockConfiguration`) and per-object retention exist.
- **Not assessed or certified.** AWS's S3 Object Lock has been assessed for SEC Rule 17a-4(f), FINRA 4511, and CFTC 1.31. Hamster has not — by anyone. It implements the WORM building block those rules require, which is necessary but nowhere near sufficient, and it is v0, not production-ready, with formats that may still change. Describe Hamster as *implementing object-lock retention and legal holds, including a no-override COMPLIANCE mode* — never as 17a-4(f) compliant or assessed.

## Non-goals

Stated so nobody waits for them: MFA Delete (an S3 legacy control tied to root-account MFA; object lock in v0.6 — GOVERNANCE and COMPLIANCE retention with legal holds — is Hamster's WORM mechanism, and the one a compliance story should rest on), website hosting (a reverse proxy like Caddy in front of a bucket is a better static-site host than S3's website mode ever was — index documents, redirects, and TLS are a web server's job, not a storage API's), file protocols — WebDAV, NFS, SMB (the S3 API is the product; the gateway sits behind the same core interfaces another front end could use, so this can be revisited post-v1 if demand proves out, and `rclone serve webdav` against any S3 endpoint bridges the gap today), S3 Select, inventory/analytics, BitTorrent, Requester Pays, access points, ACLs (legacy even on AWS — new buckets have had them disabled by default since 2023, with bucket policies as the recommended replacement, which is the path Hamster may take later; common no-op headers like `x-amz-acl: private` are accepted and ignored so tools don't break), and cross-region replication as an API (multi-region is a future cluster-to-cluster feature, not a bucket flag).

## Open questions

- Which lifecycle subset is worth having (expiration almost certainly; transitions probably never — there is no second storage class to transition to yet).
- Bucket policy model and scope — decided when multi-key authorization becomes real, not before.
- ~~Whether `Content-MD5` validation on PUT is enforced or advisory~~ — resolved: enforced whenever supplied, on every body-bearing operation (`BadDigest` on mismatch, `InvalidDigest` when malformed), but never *required* — checksum-era SDKs send `x-amz-checksum-*` instead of `Content-MD5`, and hard-requiring the old header would break them. The `x-amz-checksum-*` family arrives later, additively.
