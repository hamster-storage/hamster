# ADR-0018: SigV4 authentication, implemented in-house on the standard library

## Status

Accepted (verification implemented in `internal/sigv4`, validated against the AWS example signatures)

## Context

Every S3 client signs requests with AWS Signature Version 4 by default — the SDKs, the `aws` CLI, rclone, restic. An S3-compatible server that cannot verify SigV4 doesn't work with real tools; unauthenticated access is the exotic case, not the default. So v0.1 needs verification of: `Authorization`-header signing, presigned query URLs, `UNSIGNED-PAYLOAD`, and `aws-chunked` streaming signatures (which the CLI uses over plain HTTP — exactly how homelab deployments run).

The question is how to get it: implement verification from the specification, or import it.

## Decision

**Implement SigV4 verification in-house, using only the standard library** (`crypto/hmac`, `crypto/sha256`), validated against AWS's published test vectors and exercised by the simulation workload generator (including malformed and expired signatures).

Supporting decisions:

- **Credentials are replicated metadata**: a root access-key/secret pair created at cluster init, additional keys via `hamster admin key create`. Every key is full-access in v0.x; per-key and per-bucket authorization is deliberately deferred.
- **Region is a configured string** (default `us-east-1`, what tools assume when unset), existing for SigV4 scope-matching and `GetBucketLocation` — it implies nothing about geography.
- **Anonymous access** only behind an explicit `--insecure-no-auth` flag, named to be unmistakable.

## Consequences

- A few hundred lines of carefully tested crypto plumbing become ours to own — the kind of code where the test vectors and the simulator earn their keep.
- Zero new dependencies, in line with [ADR-0002](0002-single-binary-no-external-dependencies.md) and the small-module-graph principle.
- Verification logic sits behind ordinary interfaces and runs deterministically under the simulator (signature checks are pure functions of the request plus stored credentials — time-bounded validity uses proposal-style explicit time, never ambient clock reads in core logic).
- The streaming-chunked mode adds real parsing complexity to the upload path; it is in scope from v0.1 because plain-HTTP CLI uploads — the most common first contact with Hamster — use it.

## Alternatives considered

- **Ship v0.1 without authentication.** Tempting scope cut, backwards in practice: clients sign by default, so the unauthenticated server is the one that needs special client configuration. It would also ship an object store that is unsafe to expose to any network. Rejected.
- **Import the AWS SDK for Go's signer.** Apache 2.0, so licensing passes — but it is a client-side signing library inside a large dependency tree, and *verification* (parsing, canonicalization, constant-time comparison, clock-skew windows) is the server's own job regardless. We would import a lot to still write the hard part ourselves. Rejected.
- **Signature Version 2.** Long deprecated by AWS; modern SDKs don't send it. Not worth carrying. Rejected (may be revisited only if a tool that matters proves to need it).
