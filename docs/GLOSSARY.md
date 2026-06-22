# Glossary

The vocabulary Hamster uses in code, docs, CLI, and logs. It is deliberately
standard distributed-systems terminology — *node, cluster, shard, partition,
write buffer, data directory*. Hamster is the brand, not an operational
vocabulary: there are no themed names for system components, by rule.

Terms are grouped by the layer they belong to, roughly outside-in: the S3 data
model an application sees, the storage that makes it durable, the cluster the
storage spreads over, the consensus that keeps metadata consistent, and the
operational surface an operator touches. Where a term has a fuller treatment, it
links to the design doc or [ADR](adr/README.md) that owns it.

> Hamster is in early development (v0). Some terms name designs that are still
> being built; the definitions describe what the term *means* in Hamster, not a
> guarantee that the feature has shipped. See [ROADMAP.md](ROADMAP.md) and
> [PLAN.md](PLAN.md) for status.

---

## The S3 data model

What an application talks to. The compatibility surface is [S3-API.md](S3-API.md).

- **Bucket** — a named container for objects, and the unit S3 configures
  versioning and object lock on. Created before the objects it holds.

- **Object** — an immutable blob of bytes stored under a bucket and key, written
  once and never edited in place. Overwriting a key creates a *new version*; the
  old bytes are untouched. (A load-bearing invariant: objects are immutable
  blobs.)

- **Key** — an object's name within its bucket. Bucket + key + version identifies
  exact bytes.

- **Version** — one immutable snapshot of a key's contents. Hamster models every
  key as an *ordered list of versions*, never a single record — even when
  versioning is disabled and the list holds one entry — so turning versioning on
  is never a schema migration. See [METADATA.md](METADATA.md),
  [ADR-0006](adr/0006-versioning-and-object-lock.md).

- **Version ID** — the unique identifier of a version: a
  [UUIDv7](adr/0007-uuidv7-version-ids.md) (`meta.VersionID`, a `[16]byte`),
  minted in-repo from explicit clock and randomness inputs so it stays
  deterministic under simulation.

- **Current version** — the version a plain GET (no version ID) returns: the head
  of the key's version list, resolved inside the metadata transaction.

- **Delete marker** — a versioned tombstone: deleting a key under versioning
  inserts a marker as the new current version rather than destroying data, so the
  prior versions remain retrievable by ID.

- **Null version** — the single un-versioned entry a key holds while versioning is
  *suspended* (S3's quirk). Still a real UUIDv7 internally, flagged so the API
  renders its ID as `"null"`. See [METADATA.md](METADATA.md).

- **ETag** — the S3 entity tag for an object, an MD5 digest for client
  compatibility; integrity is carried by Hamster's own internal checksums, not
  the ETag. See [ADR-0019](adr/0019-md5-etags.md).

- **Object lock** — write-once-read-many retention on a version: a *retention*
  period and/or a *legal hold*.
  - **Retention** — a date until which a version may not be deleted or overwritten.
  - **Legal hold** — an on/off flag that prevents deletion independent of any
    retention date.
  - **COMPLIANCE mode** — retention with *no override path*: not for
    administrators, not for operators, not behind a flag. If a code path can
    delete or shorten a COMPLIANCE-locked version, it is a bug.
  - **GOVERNANCE mode** — retention that a sufficiently privileged caller *can*
    bypass. The bypassable counterpart to COMPLIANCE.

  See [ADR-0006](adr/0006-versioning-and-object-lock.md).

---

## Storage and durability

How an object's bytes are made to survive disk and machine loss. The design is
[ERASURE-CODING.md](ERASURE-CODING.md) and [DATA-STREAM.md](DATA-STREAM.md).

- **Erasure coding (EC)** — splitting an object's bytes into `k` *data* shards and
  `m` *parity* shards such that any `k` of the `k+m` reconstruct the original.
  Hamster's durability comes from this spread, not from full-copy replication.
  See [ADR-0003](adr/0003-erasure-coding-over-replication.md).

- **Shard** — one of the `k+m` pieces of an erasure-coded stripe, stored as a
  self-describing file on a single node. A **data shard** carries object bytes; a
  **parity shard** carries redundancy. No two shards of one object ever share a
  node.

- **Stripe** — the unit erasure coding operates over: a span of the object split
  into `k` contiguous *slices* plus the `m` parity slices computed from them. A
  large object is many stripes. See [ADR-0026](adr/0026-stripe-and-shard-layout.md).

- **Slice** — a fixed-size (256 KiB) contiguous piece of a stripe; one shard is
  the concatenation of its slices across stripes. A range read touches only the
  slices it covers.

- **Storage profile** — the chosen `k+m` for a write (e.g. `4+2`). The default is
  `auto`: an *auto ladder* that picks a profile from the cluster's node count, so
  a small cluster stays survivable and a larger one spreads wider. See
  [ADR-0015](adr/0015-storage-profiles.md).

- **Replication (small-object / k=1)** — the degenerate profile where `k=1`, so
  every shard is a full copy. Used for objects too small to benefit from coding;
  it is erasure coding with one data shard, not a separate code path.

- **Chunk** — a fixed-size framed unit of the object *stream* between the gateway
  and erasure coding, each protected by a CRC-32C verified on every read. The
  layer where optional compression and encryption slot in as per-chunk
  transforms. See [DATA-STREAM.md](DATA-STREAM.md).

- **Object stream** — the framed byte stream (chunks + a trailer index for random
  access) the gateway hands to erasure coding. Pure computation; the boundary
  between "an object's bytes" and "shards on disk".

- **Data ID** — the internal identifier (`meta.VersionID`-shaped) that addresses a
  version's shard set on the data plane, distinct from the user-facing key. Shard
  files are named by `(data ID, shard index)`.

- **Write buffer** — the bounded in-memory staging a write passes through on its
  way to disk: appended, then synced durable *before* the metadata commit, so an
  acknowledged write is on disk.

- **Scrub** — reading shards and verifying them against their replicated
  checksums to catch silent corruption (bitrot) before a client ever asks for the
  object. In Hamster, scrub and repair are one pass.

- **Repair** — rebuilding missing or corrupted shards from any `k` verified
  survivors and re-committing them, restoring an object's full `k+m` spread. The
  `RepairSweep` walks every version, scrubbing and rebuilding as it goes.

- **Orphan** — a shard file no committed metadata refers to (e.g. left by an
  interrupted write, or a shard whose placement moved). Unreadable as an object
  and reclaimed by garbage collection.

---

## Cluster topology and placement

Where shards live, and the rules that spread them. The design is
[ARCHITECTURE.md](ARCHITECTURE.md); placement decisions are
[ADR-0004](adr/0004-partitioned-placement.md) and
[ADR-0016](adr/0016-failure-domain-hierarchy.md).

- **Cluster** — the set of nodes that together store data and replicate metadata,
  founded by `init` and grown by token-authenticated joins. A one-node
  cluster is valid (it elects itself leader) and the path that can scale out.

- **Node** — one running Hamster process with its own data directory and identity;
  the **failure domain** placement treats as the hard floor — two shards of one
  object never land on the same node. Hosts and zones group nodes *above* the
  node; the node rule is non-negotiable.

- **Host** — the machine a node runs on, its identity auto-detected (the OS
  hostname). Several nodes on one box share a host. Placement spreads across hosts
  when it can, so one machine's loss costs at most one shard per object.

- **Zone** — an operator-set failure-domain label above the host — a rack, an
  availability zone — defaulting to the host. The widest level placement spreads
  over. See [ADR-0016](adr/0016-failure-domain-hierarchy.md).

- **Failure domain** — any level of the hierarchy *node < host < zone* across
  which placement tries to spread shards so a correlated failure (a dead machine,
  a lost rack) takes out as few shards of an object as possible.

- **Partition** — a fixed-count bucket in a layer of indirection between objects
  and nodes: each object's data ID hashes to a partition, and a partition maps to
  an ordered list of nodes. The partition count is set at cluster creation
  (default 4096) and **never resized** — rebalancing moves partitions between
  nodes, it never re-encodes objects. See
  [ADR-0004](adr/0004-partitioned-placement.md).

- **Placement** — the deterministic function `data ID → partition → ordered node
  list`. Every node and every restart computes the same mapping from a committed
  layout, so the same object always resolves to the same nodes. Shard `i` of a
  version lives on the `i`-th node of its partition's list (positional
  addressing). Implemented in `internal/place`.

- **Rendezvous hashing (HRW)** — the scoring rule under placement: each
  `(partition, node)` pair gets a score, and the highest scorers win. Adding or
  removing one node changes only the assignments that involve it, keeping
  rebalances proportional to the capacity changed.

- **Cluster layout** — the stored, *versioned* record of the cluster's member set
  that placement ranks over (`meta.ClusterLayout`, a singleton). Read once per
  operation so an object's partition and node ranking share one generation;
  installed by compare-and-set. See [ADR-0028](adr/0028-stored-cluster-layout.md).

- **Capacity weight** — a node's relative storage capacity (operator-set, default
  equal). A higher-weight node holds proportionally more partitions within the
  failure-domain spread. See [ADR-0004](adr/0004-partitioned-placement.md).

- **Draining** — an operator-set flag marking a node for removal: placement
  demotes it below every active node so new writes steer away, while existing
  shards stay readable until repair migrates them off. Set with `drain`.

- **Transition** — a layout caught mid-change (a drain, a join, a weight shift):
  it carries both the new member set and the *previous* one. Because shard
  addressing is positional, a member change relocates shards, so during a
  transition a read **dual-reads** — fetching each shard from its new home, or its
  old one if repair has not migrated it yet — keeping data available throughout.

- **Rebalance** — migrating partitions (and their shards) from old nodes to new
  ones to follow a layout change, *without* re-encoding objects (the fixed
  partition count guarantees this). Repair performs the shard migration during a
  transition.

---

## Metadata and consensus

How Hamster keeps the small, authoritative record of what exists consistent
across nodes. The design is [METADATA.md](METADATA.md);
[ADR-0005](adr/0005-metadata-badgerdb-raft.md) is the core decision.

- **Metadata** — the small records describing every key, version, bucket, and the
  cluster layout: keys, sizes, checksums, shard locations, lock state. Object
  *bytes* are never metadata. Stored in BadgerDB on each replica and replicated
  by Raft.

- **Raft** — the consensus protocol that replicates the metadata log so every node
  agrees on the same ordered history. Hamster drives etcd-io/raft over its
  interface seam. **Object data never passes through the Raft log** — only the
  small metadata commit does. See [ADR-0012](adr/0012-etcd-raft-consensus-library.md).

- **Proposal** — one metadata change submitted to the Raft log (create bucket, put
  object, set layout, …). It is the linearization point of an operation: a PUT's
  bytes are made durable first, then the proposal commits.

- **Leader** — the single node that may append to the Raft log at a time; all
  proposals commit through it. In the current preview, writes are leader-only and
  a non-leader answers `SlowDown`.

- **Voter** — a Raft member that votes in elections and counts toward quorum.
  Hamster caps voters at five; further nodes join as learners. See
  [ADR-0017](adr/0017-raft-voter-cap-learners.md).

- **Learner** — a Raft member that replicates the log but does not vote. New nodes
  join as learners and are automatically promoted to voter once caught up, up to
  the five-voter cap.

- **Quorum** — the majority of voters required to commit a Raft entry or elect a
  leader (e.g. three of five). Losing quorum stalls the metadata plane until it
  re-forms; permanent loss is the `recover` exit.

- **WAL** — the write-ahead log on disk that Raft appends entries to before they
  are applied, with periodic snapshots for compaction. The rebuild source of
  truth if a replica's BadgerDB store is lost. See
  [ADR-0005](adr/0005-metadata-badgerdb-raft.md).

- **Snapshot** — a full dump of the metadata state taken to compact the WAL and to
  catch up a lagging follower.

- **Persister** — the interface (a *seam*) through which applied metadata is made
  durable in a replica's store, so the same apply path runs in production and
  under the simulator.

---

## Operations and security

The surface an operator runs and the trust model around it. Cluster security is
[ADR-0022](adr/0022-cluster-mtls.md).

- **Data directory** — the on-disk home of one node: its blobs/shards, BadgerDB
  metadata, WAL, and cluster identity. Everything survives a restart; `-data-dir`
  names it.

- **Cluster CA** — the certificate authority minted automatically at `cluster
  init`. It signs every node's certificate; a node's identity *is* its
  certificate. See [ADR-0022](adr/0022-cluster-mtls.md).

- **mTLS (mutual TLS)** — the requirement that both ends of every inter-node
  connection present a cluster-CA certificate. There is no plaintext mode.

- **Join token** — a single-use, time-bound secret minted on the CA holder that
  lets a new node authenticate the cluster (it pins the CA hash) and be admitted,
  before it has any trust material of its own.

- **Cluster port** — the one mutually-authenticated port a node listens on
  (`-listen`): the peer transport (Raft + shard data) *and* the join/status/drain
  control protocol, split apart by ALPN. See
  [ADR-0030](adr/0030-single-cluster-port.md).

- **S3 port** — the separate HTTP endpoint (`-s3`) serving the public S3 API,
  authenticated by SigV4 access keys — a different trust domain from the cluster
  port.

- **Coordinator** — the per-node component that turns one S3 operation into
  placement + erasure coding + shard transfer + a metadata proposal. It paces a
  PUT through the write path and enforces the write-acknowledgment rule.

- **Recover** — the disaster exit (`recover`) for a cluster whose voter
  quorum is permanently lost: offline, local-log-wins, rewrites a surviving node
  into a new single-voter cluster. Irreversible. See
  [ADR-0025](adr/0025-force-new-cluster-recovery.md).

---

## Architecture and testing

Terms for how the code is built and proven. The design is
[SIMULATION.md](SIMULATION.md).

- **Seam** — an interface between core logic and the outside world (clock,
  network, disk, event loop). Core code receives seams and never touches the OS
  directly, so the simulator can substitute controlled versions. Lives in
  `internal/seam`.

- **Deterministic simulation** — running the whole system on a virtual clock,
  seeded randomness, a faulty in-memory network, and a crash-faithful disk, so a
  failure schedule replays identically. Any change to the write path, repair,
  placement, Raft, or formats must survive it before merging. See
  [ADR-0009](adr/0009-deterministic-simulation-testing.md).

- **Control-plane loop** — the single goroutine that runs a node's decision logic
  serially (FIFO), so core state needs no locks. The production half of the
  single-threaded-control-plane contract the simulator provides for free.

- **Write-acknowledgment rule** — when a PUT may be acknowledged durable: all
  `k+m` shards on the healthy path, a hard floor of `k+1`, and a refusal
  (`SlowDown`) below the floor. See [ADR-0015](adr/0015-storage-profiles.md).

- **Versioned formats** — every persistent or on-wire structure carries a version
  field and is only ever *added* to, never repurposed, so new code always reads
  old data (rolling upgrades). See
  [ADR-0008](adr/0008-versioned-formats-rolling-upgrades.md).
