# ADR-0031: Re-encoding existing data across storage profiles

## Status

Accepted

## Context

Storage profiles ([ADR-0015](0015-storage-profiles.md)) are chosen per object at
write time and recorded in the object's own metadata: `k`, `m`, and the per-shard
checksums. The auto ladder picks the profile from the cluster's node count, but
once written an object keeps its profile forever — placement is positional and
derived from the member set ([ADR-0004](0004-partitioned-placement.md)), and
objects are immutable blobs (invariant 6). That is deliberate: enabling
versioning never migrates data, and a profile retune changes only new writes.

This leaves one operation impossible: **shrinking a cluster below an existing
object's width.** A 4+2 object needs six distinct nodes (the node-distinct floor,
invariant 8). On a six-node 4+2 cluster, draining a node can never evacuate it —
the object cannot place five shards and one nowhere — so `cluster drain`/`remove`
refuse it (the gate in ADR-0004's removal path). To go to five nodes, every 4+2
object must become 3+2 first. The bytes do not change; only their erasure-coded
representation does.

The reverse — climbing *up* a profile as a cluster grows, for efficiency — is the
same operation in the other direction, but optional: data is already safe at the
smaller profile.

The tension is with immutability and object lock. Rewriting an object's shards
touches fields on a committed version. And a COMPLIANCE-locked version
(invariant 4) admits no path that deletes it or shortens its retention.

## Decision

**Re-encoding is a physical re-representation of an object, not a content edit
and not a new version.** It rewrites a committed version's erasure-coded layout —
its `DataID`, `k+m`, and shard checksums — and *only* those. The object's bytes,
`ObjectChecksum`, size, ETag, user metadata, and every object-lock field are left
exactly as they were. It is the same maintenance category as repair rebuilding a
shard, which already rewrites shards without changing the version.

Four parts:

1. **The metadata operation** (`meta.ReEncodeObject`). A proposal that loads a
   version and replaces its EC/addressing fields, nothing else. It is therefore
   **COMPLIANCE-safe**: it can run on a locked version because it neither deletes
   the object nor shortens retention — invariant 6 governs the *bytes*, which are
   unchanged, so re-encode does not violate it, and it does not mint a new
   version (no delete-marker churn, no lock re-evaluation).

2. **The data operation** (`coord.ReEncode`). Read the object through its current
   shards, `ec`-decode to the framed stream, `ec`-encode that *same* stream at the
   new `k+m`, write the new shards to their placement, commit the metadata switch,
   then drop the old shards. The new shards are durable *before* the commit and
   the old ones drop only *after* it, so the object is readable throughout — at
   the old profile until the commit, at the new one after — and any failure
   before the commit leaves the version untouched (the old shards are still the
   live copy). Every new shard must ack: re-encode is maintenance, so a partial
   result aborts rather than committing a degraded object. The read resolves
   through the transition's old placement (`Locate`), since a downsize's
   un-migrated shards sit at the pre-drain ordering.

3. **The trigger.** The storage profile follows the **active (non-draining) node
   count**, not the total. So draining a node that crosses a ladder boundary
   lowers the profile, and new writes already land at the profile the shrink
   converges to. The repair sweep re-encodes any object whose width no longer
   fits the active count down to the active-count profile; the drain's layout
   transition closes only once every object fits, at which point the drained node
   holds nothing and `cluster remove` succeeds. A same-size drain leaves the
   active count — and so the profile — unchanged, and re-encodes nothing.

4. **The operator surface.** The operator names the node, not the profile
   (`cluster drain <node>`) — a node-count mental model, not an erasure-coding
   one. When the drain crosses a boundary the CLI prints the before/after profile
   and the real per-step trade and asks `[y/N]` (or takes `-reencode`). The trade
   is not uniform: 6→5 (4+2→3+2) keeps two-failure tolerance and costs only
   storage efficiency, but 5→4 (3+2→2+1) drops tolerance to one — the prompt
   states which.

## Consequences

- A cluster can shrink below an existing object's width, deliberately and with
  informed consent. Until re-encode converges, removal stays gated — durability
  is never traded silently.
- Re-encode is **version-targeted**, so it works unchanged when versioning ships
  (every key is already an ordered version list, invariant 3): a downsize
  re-encodes every version, skips delete markers (no shards), and preserves each
  version's lock independently. Multipart objects are out for now (not on the
  cluster path yet).
- This is the first operation that mutates a committed version's data-addressing
  fields. The exception is bounded and stated here: same bytes, same identity,
  same lock; only the EC representation moves; old shards drop only after the new
  are durable and committed.
- The same machinery climbs a profile *up* as a cluster grows — an efficiency
  optimization, not a safety requirement, so it is opt-in/background, never
  automatic on a join (a join must not trigger a full re-encode).

## Alternatives considered

- **Re-encode as an overwrite (new version).** Rejected: it changes version
  identity, breaks the immutable-version model, churns delete markers, and
  re-evaluates object lock — a content edit's semantics for what is not a content
  edit.
- **An operator-pinned profile** instead of node-count-driven. Rejected for the
  primary path: new S3 users reason in node counts, not `k+m`. A pinned profile
  is a reasonable future advanced override, layered on the auto ladder, not the
  default.
- **Transcoding shards directly** (without decoding to the stream). Impossible:
  changing `k` requires re-encoding from the data, not re-arranging existing
  parity.
