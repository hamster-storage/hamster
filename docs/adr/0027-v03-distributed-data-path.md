# ADR-0027: The v0.3 distributed data path — derived placement, shard transfer over the seam, coordinator state machines

## Status

Accepted. Implemented: placement ([`internal/place`](../../internal/place/)), the shard transfer protocol ([`internal/datapath`](../../internal/datapath/)), and the coordinators ([`internal/coord`](../../internal/coord/)) — the PUT with the ack rule, and the GET that prefetches covering shard ranges (`stream.Cover` + `ec.ReadHeader`) and decodes through the pure readers, reconstructing from any k. Proven under simulated cluster schedules: crashed receivers, down nodes, floor refusals, mid-PUT coordinator loss, degraded reads through m crashed holders. The repair sweep (pass 4) is implemented in the same package: scrub-verify of every shard against replicated checksums plus rebuild from any k verified survivors, with its own schedules (emptied node, two-shard bitrot, beyond-tolerance, crash mid-sweep). Remaining: the `hamster serve` wiring (pass 5).

## Context

v0.3 connects the pieces that exist into a clustered data path: the framed object stream ([DATA-STREAM.md](../DATA-STREAM.md)), the erasure-coding engine ([ADR-0026](0026-stripe-and-shard-layout.md)), the Raft metadata plane ([ADR-0012](0012-etcd-raft-consensus-library.md)), and the mTLS transport ([ADR-0022](0022-cluster-mtls.md)). A PUT must erasure-code the body, land `k+m` shards on `k+m` distinct nodes, and commit one metadata record through Raft; a GET must do the reverse from any `k`. Several decisions sit between the design docs and running code:

1. How does an object map to a partition, and a partition to nodes, before the stored versioned layout ([ADR-0004](0004-partitioned-placement.md), scheduled for v0.4) exists?
2. How do shard bytes travel between nodes when the transport seam ([`internal/seam`](../../internal/seam/)) is unreliable, unordered messages — and they must, because the simulation harness can only prove what runs over the seam?
3. What shape does the coordinator take, given that core state is owned by a single-threaded event loop in both worlds (SIMULATION.md)?

The hard constraints: object data never passes through the Raft log (critical invariant 1), every wire format is additively versioned protobuf (invariant 2), no two shards of one object on one node (invariant 8), and all of it must run under the deterministic simulation harness (invariant 5).

## Decision

1. **Partition from the data ID: FNV-1a 64 over the 16 ID bytes, finalized with murmur3's 64-bit mixer, modulo the partition count.** (Bare FNV-1a has no avalanche — inputs differing in one trailing byte hash exactly one FNV-prime apart, which would also have quietly broken the rendezvous ranking below; the finalizer spreads every input bit, and a test proved the failure before the fix.) The count is fixed at cluster creation (`ClusterConfig.partition_count`, [ADR-0004](0004-partitioned-placement.md)), defaulting to **4096**. The hash choice is permanent in effect but not in consequence: every object records its partition in its `VersionEntry`, so a future algorithm change affects new writes only — the same retuning-is-free property the EC parameters have.

2. **v0.3 placement is derived, not stored: rendezvous hashing per partition.** A partition's node list is the member set ranked by the same mixed hash of `(partition, node ID)`, ties broken by node ID; an object written `k+m` wide uses the first `k+m` nodes of its partition's ranking. This is a pure function of `(partition, member set)` — every node computes the same answer from replicated state, nothing is stored, and the node-distinct invariant holds by construction (a ranking of distinct members is a permutation). Narrow objects use a prefix of the same ranking, so a small object (`1+2`) and its large sibling (`4+2`) in one partition share placement logic with no special case.

   **The stated limitation:** derived placement moves when membership does, and v0.3 ships no rebalance to move the shards after it. A v0.3 cluster's data-plane membership is therefore effectively static once data exists — joins for capacity must wait for v0.4's stored `ClusterLayout` with transition tracking and rebalance, exactly where ROADMAP.md schedules them. The simulation schedules hold membership fixed while crashing, partitioning, and restarting the fixed set. (Shard files are self-describing — [ADR-0026](0026-stripe-and-shard-layout.md) — precisely so misplaced data remains identifiable when layouts later move.)

3. **Shard bytes travel as versioned protobuf messages over the transport seam, with reliability built where the simulator can torture it.** The seam's contract is unreliable, unordered, duplicable delivery; the shard transfer protocol adds what it needs and no more:

   - **Write**: the coordinator streams a shard as sequenced chunks; the target stages each chunk to its `Disk` (`Append`), `Sync`s once at commit, and acknowledges durability. Chunks carry `(data ID, shard index, offset)`, so retries and duplicates are idempotent; acks are cumulative offsets, so loss costs retransmission, never corruption.
   - **Flow control is a fixed window** of unacknowledged bytes per shard stream — bounded memory at both ends regardless of object size, the write-buffer discipline extended over the wire.
   - **Read**: a request names `(data ID, shard index, byte range)`; the response carries the bytes. The reader retries on its own timer. Range requests mean a ranged GET moves only the slices it covers, preserving ADR-0026's random-access property end to end.
   - All timers come from `seam.Clock`; all sends from `seam.Transport`. The same protocol code runs under the simulator's drops, duplicates, reorderings, and crashes — that is the point.

4. **Coordinators are event-loop state machines; the pure engines stay pure.** A PUT is a state machine on the gateway node's loop: it paces the body stripe-by-stripe through the `stream`→`ec` composition (both pure computation, unchanged), feeding the next stripe only when every shard stream has window for it; shard acks arrive as loop events. A GET resolves metadata locally, prefetches the byte ranges the request covers (shard headers plus covering slices — any `k` when degraded) into buffers, then runs the synchronous `ec.Reader`/`stream.Reader` over them and serves the verified bytes. No blocking waits on the loop, no goroutines in core logic; production may parallelize the byte work later without changing what the simulator proved (SIMULATION.md's data-plane rule).

5. **The acknowledgment rule is [ADR-0015](0015-storage-profiles.md)'s, mechanically.** Healthy path: all `k+m` shards durable before the metadata commit. Degraded: shards targeted at known-down nodes may be skipped with a hard floor of `k+1` durable; below the floor the PUT is refused (`SlowDown`/503). The metadata commit — one small `PutObject` proposal through Raft — is the linearization point and happens only after the ack rule is satisfied; the proposal carries all `k+m` shard checksums regardless, so repair (v0.3 pass 4) can verify what it rebuilds. Only the proposal touches the Raft log; shard bytes never do (invariant 1).

6. **Data-plane and Raft messages share the transport behind one versioned envelope.** Each message on the inter-node transport gains a channel tag (versioned protobuf, like everything else): Raft traffic on one channel, shard transfer on another. Introduced now, before any compatibility promise exists, precisely so it never has to change shape later. v0.2→v0.3 is a restart-the-cluster upgrade (pre-v0.8 there is no rolling-upgrade machinery to preserve); the envelope itself evolves additively from here on.

## Consequences

- The whole data path — placement, transfer, ack rule, commit ordering — runs under the simulation harness, which can now check SIMULATION.md invariant 1 as written: each object's budget at ack (`durable − k`), not an assumed `m`.
- Bounded memory per in-flight PUT (`k+m` × window) and per GET (covering slices), independent of object size.
- A v0.3 cluster cannot grow its data-plane membership safely; `cluster join` remains a metadata-plane operation until v0.4. Honest, documented, temporary.
- v0.3 has no replicated health view (that arrives with v0.4's node records), so a PUT discovers a down node by its shard write timing out — a degraded write pays the timeout before acking at the floor. Slow, correct, temporary.
- The read path's prefetch-then-decode shape serves ranged GETs in one round trip of slice fetches, but very large whole-object GETs decode in stripe windows — streaming decode state machines can replace the prefetch later without protocol changes.
- 4096 partitions bounds the useful cluster size generously (ADR-0004's overprovisioning argument) and costs nothing while small.

## Alternatives considered

- **Storing the versioned layout now instead of deriving placement.** It is the v0.4 design anyway — but it drags in `NodeRecord`s, layout proposals, and transition-aware reads, none of which v0.3's goal (a correct EC data path under simulation) needs. Deferred to its scheduled release, with the limitation stated rather than half-built. Rejected for v0.3.
- **Consistent hashing directly over nodes (no partitions).** Already rejected by ADR-0004; deriving per-partition keeps v0.3 on the partition abstraction so v0.4 changes where assignments come from, not what records mean.
- **A separate streaming connection for shard bytes (alongside the message transport).** Real TCP streams would hand flow control to the kernel — and take the data path out from under the simulator, which only models the message seam. The protocol cost of windowed messages is the price of provability. Rejected.
- **Letting `ec.Writer`/`ec.Reader` block on network I/O behind `io.Writer`/`io.ReaderAt`.** Blocking inside a posted function deadlocks a single-threaded world; the engines stay pure and the coordinator owns all waiting as state. Rejected by the concurrency model.
- **Acknowledging the PUT before the metadata commit.** The commit is the linearization point; an acked-but-uncommitted object is visible to no read and durable to no purpose. Rejected — ack follows commit, always.
