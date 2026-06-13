# ADR-0028: The stored, versioned cluster layout

## Status

Accepted. Implemented (v0.4 pass 1): the `meta.ClusterLayout` record at the
reserved system key `s/layout`, and its `SetClusterLayout` proposal on the
reserved envelope field 15, applied as a compare-and-set on the layout
version ([`internal/meta`](../../internal/meta/)); the `place.Layout` snapshot
placement reads from ([`internal/place`](../../internal/place/)); the
coordinator resolving placement from the layout per operation, with the
auto-ladder profile following the layout's member count
([`internal/coord`](../../internal/coord/)); and the leader reconcile that
advances the layout toward Raft membership ([`internal/cluster`](../../internal/cluster/)).
Proven by the meta codec/apply/persist tests (round-trip, golden-pinned bytes,
compare-and-set, byte-identical restore), the coordinator simulation, and the
six-node real-process e2e that kills nodes mid-workload. Transition tracking /
mid-migration dual-read, zone-aware spread ([ADR-0016](0016-failure-domain-hierarchy.md)),
capacity weighting, manual rebalance, and repair re-encode are the remaining
v0.4 passes ([ROADMAP.md](../ROADMAP.md)) and are explicitly out of scope here.

## Context

v0.3 ([ADR-0027](0027-v03-distributed-data-path.md)) derived placement live:
partition→nodes was a pure function of the data ID and the *current* Raft
member set, recomputed on every operation. Correct and simple, but it made
placement move whenever membership moved — and v0.3 shipped no machinery to
relocate shards, so "the data-plane membership is effectively fixed once data
exists" was the headline v0.3 limitation.

[ADR-0004](0004-partitioned-placement.md) always specified the fix: the
partition→node assignment is the *cluster layout*, versioned and
Raft-replicated. This ADR is the first step toward it — moving the source of
truth for placement onto a stored record — without yet building the migration
that lets the layout change safely while data exists. The hard constraints are
unchanged: every persistent structure is additively versioned protobuf
(invariant 2), no two shards of one object on one node (invariant 8), and all
of it runs under the deterministic simulation harness (invariant 5).

## Decision

1. **The layout is a replicated singleton record.** `ClusterLayout` holds a
   monotonic `version`, the `partition_count` (the cluster constant, ADR-0004,
   carried here until a `ClusterConfig` record lands), and the ordered set of
   `members` placement ranks over. It lives at `s/layout` under the reserved
   `s/` system prefix, versioned protobuf like everything else. Members are
   stored as raw node-ID strings so `internal/meta` stays free of the seam, as
   it is by rule; the cluster layer maps them to `seam.NodeID`.

2. **It is installed and advanced by a compare-and-set proposal.**
   `SetClusterLayout` travels the Raft log on the reserved envelope field 15.
   Apply accepts it only when `version` is exactly the stored version plus one
   (the first install is version 1); a stale or gapped version is a
   *deterministic refusal* (`ErrStaleLayout`), not a crash, so a reconciling
   leader that retransmits or two proposals that race converge every replica to
   the same generation instead of clobbering each other. The partition count is
   fixed at the first install and may never change (ADR-0004: never resized).

3. **Placement derives from the layout's member set,** by the same rendezvous
   ranking as v0.3 (ADR-0027): node-distinct by construction, narrow widths a
   prefix of wide ones. The layout is read **once per operation** as a
   `place.Layout` snapshot, so an object's partition and its node ranking are
   always computed from a single generation, never two. The active storage
   profile follows the layout's member count up the auto ladder
   ([ADR-0015](0015-storage-profiles.md)).

4. **The leader reconciles the layout toward Raft membership** — a periodic,
   idempotent proposal. Members are canonicalized (sorted) before proposing, so
   the proposal is byte-identical whichever leader composes it; rendezvous ranks
   by hash, so the order recorded never affects where shards land. In pass 1 the
   reconcile tracks membership unconditionally, which makes placement
   **behavior-identical to v0.3's live placement at steady state** — there is not
   yet a rebalance to move shards, so there is nothing to protect by freezing it.
   Freezing the reconcile once object data exists, and gating further layout
   changes behind an explicit rebalance, is the next v0.4 pass.

## Consequences

- Placement is now a committed, versioned, restart-stable fact: every node and
  every replay computes the same assignment from the record, proven
  byte-identical through the `Persister` snapshot path.
- **No durability regression versus v0.3.** At steady state the assignment is
  identical; this pass relocates the *source* of placement into a replicated
  record, nothing more. The six-node kill-mid-workload e2e behaves exactly as
  before.
- The `version` field and the compare-and-set give a future rebalance the
  old→new pair it needs to track a migration and serve mid-migration reads from
  both assignments — the `ClusterLayout` record grows those fields additively.
- A forming cluster advances the layout v1→v2→… as members join, each a valid
  generation; the version number reflects how many layout changes have happened,
  not a defect.
- Until the reconcile is frozen-on-data (next pass), adding a node after data
  exists still moves placement and strands old shards exactly as v0.3 did —
  honest, documented, temporary, and removed by rebalance.
- A PUT before the first layout is installed (a cluster still forming) refuses
  transiently with `SlowDown`/503, the same family as a write below the ack
  floor — never a wrong placement.

## Alternatives considered

- **Store an explicit per-partition assignment table now** (the ~4096-row
  `assignments` form METADATA.md sketches). Rendezvous-over-stored-members
  captures the same assignment in O(members) bytes, derives narrow-from-wide for
  free, and changes only *where the member set comes from* versus v0.3 — the
  smallest correct step. The explicit table is what a weighted or zone-aware
  policy will want; it arrives with that policy, not before it. Rejected for
  pass 1.
- **Operator-only layout installation from the start** (no auto-reconcile).
  Faithful to ADR-0004's "manually triggered rebalance," but it would leave a
  freshly formed cluster unable to place wide until an explicit step — worse
  first-run behavior than v0.3. The reconcile preserves v0.3's smooth
  formation; the explicit-rebalance discipline arrives when there are shards to
  move. Rejected for pass 1.
- **Keep derived placement and add migration on top.** Migration needs a
  stable, versioned thing to migrate *between*; without the stored layout there
  is no old→new generation to track. The record comes first. Rejected.
