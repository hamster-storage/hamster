# Metadata Schema Design

This document designs the metadata layer: the protobuf records and the BadgerDB keyspace that hold everything Hamster knows about its objects. It is the concrete companion to [ADR-0005](adr/0005-metadata-badgerdb-raft.md) (where metadata lives), [ADR-0006](adr/0006-versioning-and-object-lock.md) (what versioning must support), [ADR-0008](adr/0008-versioned-formats-rolling-upgrades.md) (how formats evolve), and [ADR-0014](adr/0014-metadata-keyspace-design.md) (the keyspace decisions made here).

> **Status: design document.** No `.proto` files or code exist yet. Message shapes below are illustrative and will be settled with the first v0.1 code — but the keyspace structure and the principles are the design of record.

## Principles

Five rules shape everything below. The first four restate critical invariants from CLAUDE.md in schema terms; the fifth is new and earns its keep at rebalance time.

1. **The version list is the only truth.** Every key is an ordered list of version entries, one BadgerDB row per version — even with versioning disabled, when the list holds one entry. Anything else (the current-version index) is derived and rebuildable from it.
2. **Every record is additively versioned protobuf.** Every message carries `format_version` as field 1. Fields are added, never removed or repurposed. New code reads old records forever within the [compatibility policy](adr/0010-v1-compatibility-policy.md).
3. **One S3 mutation, one transaction.** Each metadata change is a single Raft proposal applied as a single BadgerDB write transaction. The Raft commit is the linearization point; the Badger transaction makes it crash-atomic on each replica.
4. **Apply is deterministic.** The same proposal produces the same state change on every replica and on every replay — which means apply code never reads the wall clock, the filesystem, or anything else ambient. Time, where needed (lock checks), is a field in the proposal.
5. **Object records never change after commit, and never grow with cluster activity.** Repair and rebalance must not rewrite per-object metadata. This is why shard addressing goes through the partition (below) rather than naming nodes per object.

## Shard addressing: the partition is the location

A version entry does not store "shard 0 is on node A, shard 1 is on node B." It stores its **partition ID** and its EC parameters (`k`, `m`). The [cluster layout](adr/0004-partitioned-placement.md) — versioned metadata, replicated like everything else — maps each partition to an ordered list of `k+m` nodes; shard `i` of every object in the partition lives on the `i`-th node of that list.

What this buys:

- **Rebalance and repair touch one record, not millions.** Moving a partition or rebuilding a lost node's shards is a layout change plus data movement. Per-object metadata is untouched, which is what makes invariant 5 (and "shards just move, never re-encoded") real rather than aspirational.
- **Reads stay correct mid-migration** by consulting both the old and new assignment in a transitioning layout, exactly as ADR-0004 describes.
- The metadata commit still records the shard locations in the sense that matters: partition + layout resolves every shard deterministically.

The cost is one level of indirection on every read, against metadata that is small, hot, and local. Cheap.

## The keyspace

BadgerDB is a flat sorted key-value store; structure comes from key encoding. One byte of table prefix, then components:

| Prefix | Key | Value | Contents |
|---|---|---|---|
| `s/` | `s/cluster`, `s/layout`, `s/node/<node-id>` | `ClusterConfig`, `ClusterLayout`, `NodeRecord` | Cluster-wide system state |
| `b/` | `b/<bucket>` | `BucketConfig` | Bucket configuration |
| `v/` | `v/<bucket>\x00<key>\x00<~version-id>` | `VersionEntry` | Version lists — **the truth** |
| `c/` | `c/<bucket>\x00<key>` | `CurrentRecord` | Current-version index — **derived** |

Encoding rules:

- **Components are NUL-delimited.** Bucket names are already NUL-safe (S3 constrains them to `[a-z0-9.-]`). Object keys are arbitrary UTF-8; Hamster rejects keys containing the literal NUL byte (`0x00` — the unprintable character, not the text "0x00"). No typeable string is affected; the only way to produce such a key is deliberately percent-encoding `%00` into the URL. (A documented deviation: AWS technically accepts NUL in keys. Nothing real uses it, and the flat encoding it buys is worth the asterisk.)
- **NUL rejection is enforced twice, because a stored NUL would corrupt the keyspace** — the parser would split the encoded key at the wrong delimiter. Request validation rejects it at the S3 layer with `400 InvalidObjectName`, before any proposal exists. And apply independently rejects any proposal whose key contains `0x00` as malformed — a deterministic byte check, so replicas agree — which means no path, including a buggy internal caller, can ever put a NUL-bearing key into BadgerDB. The simulation harness's workload generator should attempt it. Because UTF-8 byte order equals code-point order, a raw scan returns keys in exactly the order S3 listings require.
- **`~version-id` is the bitwise complement of the 16-byte UUIDv7.** UUIDv7 sorts oldest-first; the complement sorts newest-first, so the first row under a key's prefix is always its newest version — the order `ListObjectVersions` returns and the order current-version resolution wants.
- Prefixes `u/` (multipart uploads) and `g/` (garbage/orphan tracking) are **reserved** for later features so they arrive additively.

The keyspace is ordered by bucket then key, so the future multi-raft split ([ADR-0005](adr/0005-metadata-badgerdb-raft.md)) is a range split along boundaries this layout already respects.

### The current-version index

`ListObjects` must not pay for version history or skip over delete markers, so the schema keeps a derived index: a `c/` row exists **if and only if** the key's newest version is a real object (not a delete marker), and it carries the denormalized fields a listing row needs. Every transaction that changes a key's newest version updates the `c/` row in the same Badger transaction, so the index is never observably stale. If it were ever lost, it is mechanically rebuildable by one scan of `v/` — that is what "derived" means, and the simulation harness should verify the equivalence as an invariant.

## Records

Illustrative shapes — field names and numbers settle with the first code. Every message starts with `format_version`.

```proto
// One row under v/ — one version of one key. Immutable after commit,
// except the lock fields, which may only strengthen.
message VersionEntry {
  uint32 format_version = 1;
  bytes  version_id     = 2;   // 16-byte UUIDv7 (ADR-0007)
  Kind   kind           = 3;   // OBJECT or DELETE_MARKER
  int64  size           = 4;
  int64  created_unix_ms = 5;
  bytes  etag           = 6;
  string content_type   = 7;
  map<string, string> user_metadata = 8;

  // Shard addressing (see "the partition is the location")
  uint64 partition        = 9;
  uint32 ec_data_shards   = 10;  // k
  uint32 ec_parity_shards = 11;  // m
  bytes  object_checksum  = 12;
  repeated bytes shard_checksums = 13;  // k+m entries

  // Object lock (ADR-0006)
  RetentionMode retention_mode      = 14;  // NONE, GOVERNANCE, COMPLIANCE
  int64         retain_until_unix_ms = 15;
  bool          legal_hold          = 16;

  // S3 "null version" marker for suspended-versioning writes (see below)
  bool null_version = 17;
}

// One row under c/ — derived listing row for the current version.
message CurrentRecord {
  uint32 format_version = 1;
  bytes  version_id     = 2;
  int64  size           = 3;
  bytes  etag           = 4;
  int64  created_unix_ms = 5;
}

message BucketConfig {
  uint32 format_version = 1;
  string name           = 2;
  int64  created_unix_ms = 3;
  VersioningState versioning = 4;  // UNVERSIONED, ENABLED, SUSPENDED
  bool   object_lock_enabled = 5;  // immutable after creation, requires versioning
  DefaultRetention default_retention = 6;
}

message ClusterLayout {
  uint32 format_version = 1;
  uint64 layout_version = 2;   // monotonic; bumped by every topology change
  repeated PartitionAssignment assignments = 3;  // partition -> ordered k+m node IDs
  // During a rebalance: the previous assignments, so reads consult both.
  repeated PartitionAssignment previous_assignments = 4;
  TransitionState transition = 5;
}

message ClusterConfig {
  uint32 format_version  = 1;
  bytes  cluster_id      = 2;
  uint32 partition_count = 3;  // fixed at creation, never resized (ADR-0004)
  string cluster_version = 4;  // feature-gate version (ADR-0008)
  string storage_profile = 5;  // active k+m for new writes; "auto" follows the ladder (ADR-0015)
}

message NodeRecord {
  uint32 format_version = 1;
  bytes  node_id        = 2;
  string control_addr   = 3;
  string data_addr      = 4;
  NodeStatus status     = 5;   // JOINING, ACTIVE, DRAINING, DOWN
  string binary_version = 6;   // feeds the upgrade interlock
  string host           = 7;   // auto-detected machine identity (ADR-0016)
  string zone           = 8;   // operator failure-domain label, defaults to host
  bool   voter          = 9;   // Raft voter vs learner (ADR-0017)
}
```

Notes:

- **Checksums live in metadata**, both whole-object and per-shard. `(k+m)` 32-byte checksums per version is small, and it means repair and scrub verify shards against replicated truth rather than trusting whatever the disk hands back.
- **`created_unix_ms` is stored even though UUIDv7 embeds a timestamp**, because the ID's timestamp is nudged for intra-millisecond monotonicity ([ADR-0007](adr/0007-uuidv7-version-ids.md)) and null-version handling complicates derivation. One explicit field beats two clever ones.
- **Lock fields may only strengthen**: `retain_until` may be extended, never shortened on a COMPLIANCE version; mode may never weaken; legal holds toggle by their own rules. These are the only mutations a committed `VersionEntry` ever sees.

### The "null version" (suspended versioning)

S3's wart, handled head-on: with versioning suspended, a PUT creates the version `"null"`, replacing any existing null version. Hamster keeps every version ID a real UUIDv7 and marks the entry with `null_version = true` — at most one per key. A write under suspension deletes the previous flagged entry and inserts the new one in the same transaction; the API renders the flagged entry's ID as `"null"`. No sentinel zero-UUID, so ADR-0007's "every version ID is a UUIDv7" stays true without exceptions.

## Operations as transactions

What goes through the Raft log is a `Proposal` — versioned protobuf like everything else, one S3 mutation each:

```proto
message Proposal {
  uint32 format_version = 1;
  int64  proposed_at_unix_ms = 2;  // time is an input, not ambient — see below
  oneof command {
    CommitObject       commit_object        = 3;
    CommitDeleteMarker commit_delete_marker = 4;
    DeleteVersion      delete_version       = 5;
    UpdateRetention    update_retention     = 6;
    CreateBucket       create_bucket        = 7;
    DeleteBucket       delete_bucket        = 8;
    UpdateLayout       update_layout        = 9;
    UpdateNode         update_node          = 10;
  }
}
```

| Operation | Transaction at apply |
|---|---|
| PUT, unversioned bucket | Insert new `v/` entry; delete the prior entry (the list holds one); upsert `c/` |
| PUT, versioning enabled | Insert new `v/` entry; upsert `c/` |
| PUT, versioning suspended | Insert new `v/` entry with `null_version`; delete prior null-version entry; upsert `c/` |
| DELETE (no version ID), versioned | Insert delete-marker `v/` entry; delete `c/` row |
| DELETE (no version ID), unversioned | Delete the `v/` entry and the `c/` row |
| DELETE with version ID | **Lock check first** — reject if held; else delete that `v/` entry; if it was newest, recompute `c/` from the next entry |
| PutObjectRetention / LegalHold | **Lock check first** — COMPLIANCE may only strengthen; rewrite the lock fields of that `v/` entry |
| CreateBucket / DeleteBucket | Insert/delete `b/` row; DeleteBucket verifies emptiness with one prefix seek inside the transaction |
| Join / layout change | Update `s/` rows; layout bumps `layout_version` |

Two properties worth making explicit:

- **Lock enforcement lives inside apply.** The check runs in the deterministic, single-threaded apply path on every replica, against replicated state, with no time-of-check gap. "COMPLIANCE has no override path" is structural here: there is simply no `Proposal` whose apply deletes a COMPLIANCE-locked version, so nothing an administrator can send — through any API — expresses the operation. The simulation harness actively tries anyway ([SIMULATION.md](SIMULATION.md), invariant 4).
- **Time is a proposal field, not a clock read.** Nodes read their wall clocks normally at the API layer — to stamp `Last-Modified`, mint UUIDv7s, fill `proposed_at_unix_ms`. What never reads a clock is *apply*: it must produce bit-identical state on every replica and every crash replay, so time reaches it only as proposal data. Retention comparisons use `proposed_at_unix_ms`, which means a skewed clock (an un-NTPed VPS, say) fuzzes a lock boundary by the skew — seconds of slop against retention measured in days and years, and the strengthen-only rule means skew can never shorten a lock already set.
- **Commit order beats clock order.** Version IDs embed the proposing node's clock, so under skew a write that commits *second* could carry a UUIDv7 that sorts *first* — and "current version" would quietly stop meaning "last write." Apply closes this deterministically: if a proposal's version ID does not sort after the key's newest existing version, apply bumps it just past it (incremented as a 128-bit value, preserving sortability per [ADR-0007](adr/0007-uuidv7-version-ids.md)). Legal because apply is a pure function of proposal plus replicated state — every replica computes the identical bump. Version lists are therefore always append-ordered by Raft commit regardless of any node's clock; skew can cost cosmetic timestamps, never ordering. More broadly, Hamster has no leases or TTL-based ownership anywhere — clock skew degrades labels, not invariants — and the simulator's fault model includes per-node skew to keep that claim tested.

## Listings

- **`ListObjects(V2)`**: scan `c/<bucket>\x00` — every row is a live current object, already in S3 order. Prefixes and delimiters are seeks within the scan; `StartAfter`/continuation tokens are seek targets.
- **`ListObjectVersions`**: scan `v/<bucket>\x00` — keys in order, versions newest-first within each key, delete markers included, exactly the wire shape the API needs.
- **GET (current)**: one read of `c/`, then one read of the `v/` entry. **GET with version ID**: one read of `v/` directly (complement the ID).

Strongly consistent reads come from the Raft layer (leader reads or read-index), per ADR-0005 — the keyspace just makes them cheap.

## Garbage and orphans

PUT writes shards *before* the metadata commit, so a crash mid-PUT leaves orphaned shards that were never visible objects — by design, the metadata commit is the linearization point. Reconciling data directories against metadata (and collecting deleted versions' shards) is a background GC whose design is deferred; the `g/` prefix is reserved for its bookkeeping so it lands additively. The simulation harness will own proving that GC never collects a reachable shard — that invariant joins the list when GC does.

## Deliberately absent (deferred, additive)

- **Multipart uploads** — reserved prefix `u/`, designed with the v0.1 S3 API surface.
- **ACLs, bucket policies, IAM-shaped auth state** — with the API surface work.
- **Usage accounting and quotas, scrub state** — later, additive rows.
- **Multi-raft range metadata** — the bucket/key ordering already leaves the door open.

## Open questions

- ~~ETag semantics~~ — resolved by [ADR-0019](adr/0019-md5-etags.md): ETags are MD5 (compatibility), integrity rides the internal checksums; the `etag` field stores the MD5 or multipart composite.
- Final proto field layout and the exact key-encoding bytes — settled with the first v0.1 code, with format conformance tests from day one.
- Whether `shard_checksums` should also be mirrored into shard file headers on disk (probably yes, for offline inspection) — a data-plane format question, not a metadata one.
