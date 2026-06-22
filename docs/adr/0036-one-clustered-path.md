# ADR-0036: Hamster is one clustered path — retire the single-node store, flatten the CLI

## Status

Accepted.

Depends on two technical-enabler ADRs written as their work begins:
[ADR-0037](0037-proposal-forwarding.md) (proposal forwarding — any node accepts
writes) and [ADR-0038](0038-ec-multipart-and-data-path-parity.md) (erasure-coded
multipart and cluster data-path parity). All three ship together as one release
(v0.11), because the dependency below is hard.

## Context

Hamster today has **two data paths and two command namespaces**, and they have
quietly diverged.

- **Two data paths.** `hamster serve` runs the v0.1 single-node store
  ([`internal/blob`](../../internal/blob/)): whole blobs on one disk, no Raft, no
  mTLS. `hamster cluster run -s3` runs the erasure-coded path
  ([`internal/coord`](../../internal/coord/)) across the cluster. The gateway
  serves both behind one `Config`, branching on whether the EC `Objects` backend
  is wired in. The two paths **do not offer the same S3 surface**: the single-node
  path streams large uploads to disk and supports multipart and server-side copy;
  the cluster path buffers whole objects in memory and **refuses multipart and
  copy** (501). So `aws s3 cp` on a video file works against a single node but
  fails against a cluster — and a store that breaks `aws s3 cp` is, by our own
  [S3-API.md](../S3-API.md), a toy. The divergence is invisible until a user hits
  it, and it is exactly backwards: the cluster is the *production* path.
- **Two command namespaces.** The top level is `serve` / `version`; everything
  operational lives under a `cluster` sub-tree of fifteen commands. The top-level
  help (`hamster -h`) lists only a curated seven of those fifteen; the full list
  appears only under `hamster cluster -h`. The two help texts drift independently,
  and the headline help under-represents what the binary can do.
- **A single node is already a one-node cluster.** Founding a node alone
  (`init` then `run`) yields a working single Raft voter that erasure-codes at the
  1+0 profile and serves S3. The single-node *deployment* is already expressible on
  the cluster path; only the separate `serve` code path and command are redundant.

The redundancy costs real things: divergent feature surfaces that confuse users, a
nested CLI that hides commands, and two data paths to build, test, and keep in
sync. Hamster's identity is "an S3-compatible object store" — it should present as
**one product with one path** that scales from one node to many.

## Decision

1. **A node is a one-node cluster; retire `serve` as a deployment, `internal/blob`
   as a production path.** The minimum deployment is `hamster init` + `hamster
   serve` — a one-node cluster that erasure-codes (1+0) and serves S3. It scales to
   N by joining nodes. There is no separate single-node code path and no in-place
   "promotion" (there never was — the formats differ). The single-node `serve`
   command and its blob-backed deployment retire; a node always runs the EC
   `Objects` backend, so **no user can reach the blob path**. `internal/blob` itself
   is retained as the gateway's simple, synchronous *test* backend (`Config.Blobs`),
   and the gateway keeps its `Blobs`-vs-`Objects` branch: fully deleting the package
   and collapsing the branch was weighed and **declined**. `blob` is the substrate
   for the gateway's 41 S3-surface unit tests (SigV4, XML, ETag/composite rules,
   multipart assembly, range math — all backend-agnostic); the production EC path
   (`internal/coord`) carries its own 41 simulation tests proving durability by
   decoding shards off disk; and the gateway↔coord seam is covered by the cluster
   e2e. So the collapse is a large, test-quality-reducing change for no
   production-coverage benefit. "One path" is achieved where it matters — in
   production — not by deleting the test seam.

2. **Flatten the CLI — hard break, no aliases.** The fifteen `cluster <sub>`
   commands become top-level verbs: `hamster init`, `hamster serve`, `hamster
   status`, `hamster join`, `hamster token`, `hamster drain`, `hamster remove`,
   `hamster optimize`, `hamster encrypt`, `hamster rotate-key`, `hamster
   rotate-ca`, `hamster recover`, `hamster can-stop`, `hamster metrics`, `hamster
   undrain`. The `cluster` namespace is removed entirely; `hamster cluster …` stops
   working. We are pre-1.0 and a hard break is cheap now and clean forever. There
   is **one** help text listing every command, and a test asserts that every verb
   in the dispatch table appears in the usage string, so help can never again drift
   from the implemented surface.

3. **S3 on by default.** `hamster serve` serves the S3 API on a default address
   (`127.0.0.1:9000`, the old `serve` default), and **requires credentials** —
   it refuses to boot without `HAMSTER_ACCESS_KEY_ID`/`HAMSTER_SECRET_ACCESS_KEY`,
   exactly as `serve` did, so the data API is never unauthenticated. `-s3 <addr>`
   overrides the address; `-no-s3` runs a headless storage-only node.

4. **Any node accepts writes**, via proposal forwarding ([ADR-0037](0037-proposal-forwarding.md)).
   A non-leader does the leadership-independent data work locally — placement,
   erasure coding, shard transfer — then forwards only the small metadata commit
   to the leader and awaits it, replacing today's `503 SlowDown` redirect. Only the
   commit crosses the leader hop; object bytes never do, so invariant 1 holds.

5. **The cluster data path reaches single-node parity** ([ADR-0038](0038-ec-multipart-and-data-path-parity.md)):
   streaming PUT (bounded memory at any object size), Range-efficient GET (fetch
   only the covering shards — `coord.Get` already takes `off,length`), server-side
   `CopyObject`/`UploadPartCopy`, and erasure-coded multipart. These are the
   prerequisites that let `serve` retire without regressing the S3 surface.

Decisions 4 and 5 are why this is one release: dropping `serve` (1) is only safe
once the cluster path is a strict superset of it, which 4 and 5 deliver.

## Consequences

- **One product, one path, one help.** A new user runs `hamster init && hamster serve`
  and has an authenticated S3 endpoint; the identical binary and commands
  scale to a cluster by joining nodes. Nothing about the surface changes as they
  grow.
- **A one-node deployment now runs Raft (single voter) and mints a CA.** This is
  marginally heavier than a bare blob server, but mTLS on a one-node cluster is
  trivial (self-signed, no peers) and the CA is minted automatically, so the
  configuration burden stays near zero — and the operational model is finally
  uniform from one node to many.
- **No in-place migration from an existing `serve` deployment.** There never was
  one (different on-disk formats). A `serve` user migrates data over S3 to a
  one-node cluster — current object data only, the same caveat the README already
  documents. `serve` was a dev preview on a v0 store, so a one-time data migration
  is acceptable; it is documented, not silent.
- **One production data path.** Production runs only the EC path, so there is one
  S3 surface for a user to learn and one to keep in sync, exercised end to end by
  the cluster e2e. The gateway's `Blobs`-vs-`Objects` branch is *retained* for unit
  testing (the `Blobs` side is test-only): the gateway's S3-surface tests stay fast
  and isolated from the distributed data plane, while production correctness is
  proven by `internal/coord`'s own simulation suite. The single-node deployment,
  the second CLI namespace, and the divergent S3 surface are what retire — not the
  test seam.
- **Wide but mechanical doc/test churn.** README prose, CLAUDE.md, the GLOSSARY,
  the demo Taskfile, and every e2e/compat invocation move to flat commands and the
  one-path model in the same release. Accepted ADRs that describe the old
  `serve`/`cluster` split stay as written (they are immutable history); only
  living docs change.

## Alternatives considered

- **Keep both paths; just complete the help and default `-s3` on.** Rejected: it
  fixes the symptoms (hidden commands, opt-in S3) while leaving the disease — two
  divergent S3 surfaces and two code paths to maintain. The user confusion is the
  divergence, not the flag.
- **Keep the `cluster` namespace and add top-level aliases for the common
  commands.** Rejected: two spellings for every command is its own confusion, and
  the nested-help drift persists. A single flat surface is simpler to learn and
  cheaper to keep honest.
- **Soft-deprecate `serve` with a warning and remove it later.** Rejected: while
  both exist they re-introduce the divergence this ADR exists to end, and pre-1.0
  is precisely when a hard break costs the least. Make the cut once.
- **Reach parity but keep `serve` as the "simple single-node" option.** Rejected:
  parity means the one-node cluster already *is* the simple option, with a uniform
  surface. A second path earning its keep only as "simpler to start" is not worth a
  permanent fork of the data plane and the CLI.
