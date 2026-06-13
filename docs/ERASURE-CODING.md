# Erasure Coding Profiles

This document designs the durability parameters: which `k+m` configurations Hamster offers, how the cluster chooses one, what acknowledgment promises, how repair keeps reality matching the promise, and how all of it lands on real hardware — one box with three disks, or three servers with five disks each. It builds on [ADR-0003](adr/0003-erasure-coding-over-replication.md) (EC over replication), [ADR-0013](adr/0013-klauspost-reedsolomon.md) (the library), [ADR-0015](adr/0015-storage-profiles.md) (profiles and the policy), and [ADR-0016](adr/0016-failure-domain-hierarchy.md) (hosts and zones). The metadata side — where parameters are recorded — is in [METADATA.md](METADATA.md). The bytes the encoder splits are the framed object stream of [DATA-STREAM.md](DATA-STREAM.md) — chunked, optionally compressed, optionally encrypted before sharding, which is why repair and rebalance never touch plaintext or keys.

> **Status: the engine and the distributed write path are built.** The engine ([`internal/ec`](../internal/ec/)): the profile set, the auto ladder, the small-object rule, the stripe codec and shard file format ([ADR-0026](adr/0026-stripe-and-shard-layout.md)), checksum-verified reconstruction, all proven against every survivable loss pattern. The distribution ([ADR-0027](adr/0027-v03-distributed-data-path.md)): placement ([`internal/place`](../internal/place/)), shard transfer ([`internal/datapath`](../internal/datapath/)), and the coordinators ([`internal/coord`](../internal/coord/)) — the PUT enforcing the write-ack rule below, the GET reading any k shards over the network with reconstruction — proven under simulated cluster schedules. Repair's rebuild mechanics are built too: the sweep verifies every shard against its replicated checksum (which is scrub — bitrot on cold data is found without a read) and rebuilds missing or rotted shards from any k verified survivors through the same staged-then-marker commit as fresh writes, proven by schedules including an emptied node, two-shard bitrot, beyond-tolerance refusal, and a crash mid-repair. Still design: the operational repair *system* (continuous queue, throttling, status reporting), re-encode, and the profile policy wiring. Numbers like thresholds remain defaults to start from, not commitments.

## Profiles, not free parameters

A **storage profile** is a named, tested `k+m` configuration. The cluster has one active profile at a time, recorded in `ClusterConfig`; every object records the parameters it was written with in its own `VersionEntry` (`ec_data_shards`, `ec_parity_shards` — see [METADATA.md](METADATA.md)). The active profile says what new writes do; the per-object record says what old objects are, forever.

The v0 profile set:

| Profile | Nodes needed | Overhead | Tolerates (data) | Intended for |
|---|---|---|---|---|
| `1+0` | 1 | 1.0× | 0 nodes | Single-node deployments — explicit, eyes-open |
| `1+1` | 2 | 2.0× | 1 node for data | Two nodes; see the caveat below |
| `2+1` | 3 | 1.5× | 1 node | Small clusters, 3–4 nodes |
| `3+2` | 5 | 1.67× | 2 nodes | 5-node clusters |
| `4+2` | 6 | 1.5× | 2 nodes | **The recommended default**, 6+ nodes |

The library handles any `k+m` up to 256 total shards; the set is deliberately small anyway because each profile ships with its own simulation failure schedules — a profile is *tested* durability, not just arithmetic. Operators cannot set arbitrary `k+m` for the same reason: `13+5` would be untested arithmetic wearing a durability claim. Wider profiles (`8+3`, `8+4`) can join the set later, additively, when clusters that size exist to want them — the hyperscaler-wide stripes (Backblaze runs 17+3 across twenty pods at 1.176× overhead) only pay off with dozens of failure domains and the repair bandwidth to match.

**What the nines mean.** Under the textbook model — independent failures at realistic annual rates, repair completing in hours — losing a `4+2` object takes three of its six shards dying inside one repair window, which computes to roughly ten to twelve nines of annual durability, comparable to the big providers' claims (versus 3× the storage for 3-way replication with the same two-failure tolerance). State the assumptions whenever stating the number: the model assumes *independent* failures, and real data loss is dominated by correlated ones — a server holding several shard disks, a rack power event, a bad disk batch. Zone-aware spread ([ADR-0016](adr/0016-failure-domain-hierarchy.md)) and fast repair move real durability more than additional parity would. Hamster's documentation states the model, not just the nines.

**The honest small-end rows:**

- **One node (`1+0`)** stores objects whole. Durability is whatever the disk offers — Hamster adds checksums and crash-safety, not redundancy. This is a real supported mode (dev, homelab, "I have backups elsewhere"), not a degenerate case, but the docs and the CLI say plainly: a single node cannot lose hardware.
- **Two nodes (`1+1`)** mirror every object — but metadata Raft with two members has a quorum of two, so with either node down **every API call fails**, reads included: strongly consistent reads need quorum just like writes, and a GET needs metadata before it can touch a shard. This is an availability loss, never a durability one — quorum-of-two means both nodes held every acknowledged write before the client saw a 200 — and recovery is automatic: when the node returns, Raft re-elects, catches it up from the log, and the API resumes with no operator action. (A node *destroyed* rather than offline needs `hamster cluster recover` — see [Replacing dead hardware](#replacing-dead-hardware).) Two nodes buy mirroring, not availability. The quick-start recommendation is one node or three, and the CLI warns at `1+1`.
- **Three nodes (`2+1`)** is the smallest honest cluster: Raft tolerates one node down (quorum 2 of 3), reads tolerate one node down, and storage overhead drops below mirroring.

## The profile policy: auto by default

`ClusterConfig` records a **profile policy**: `auto` (the default) or a pinned profile.

Under `auto`, the cluster follows the ladder, re-deriving the profile from the node count:

| Nodes | Auto profile |
|---|---|
| 1 | `1+0` |
| 2 | `1+1` |
| 3–4 | `2+1` |
| 5 | `3+2` |
| 6+ | `4+2` |

Re-derivation happens at exactly one kind of moment: **when an explicit membership command commits** — `hamster cluster join` or `hamster cluster remove-node`. Never on health changes: a node crashing, flapping, or being partitioned does not move the profile (the degraded-write floor absorbs transients). Only the operator deliberately reshaping the cluster does — and since joining and removing are themselves explicit operator commands, nothing about `auto` is silent. Every transition is announced in the command's output, in `cluster status`, and in logs:

```
cluster is now 3 nodes; profile 1+1 → 2+1; new writes tolerate one node loss
```

Downgrades are part of the ladder by necessity: removing a node from a 6-node `4+2` cluster leaves 5, and `4+2` cannot place on 5 nodes — without the step down to `3+2`, writes would block.

**Pinning** is for operators who want durability parameters to never move without their say-so: `hamster cluster set-profile 4+2` pins; `hamster cluster set-profile auto` returns to the ladder. Under a pin, a `remove-node` that would make the pinned profile unplaceable is refused until the operator re-pins lower — the cluster never maneuvers itself into refusing all writes.

Changing the profile (by ladder or by pin) is **a layout transition, not a rewrite**: new writes use the new profile immediately; existing objects keep their recorded parameters, and their shards map onto the new layout's assignments and move if needed — never re-encoded by rebalance. Bringing *existing* objects up to a better profile is repair's job:

## Repair: one system, two task types

Repair is a continuously running background system with one job statement: **make every object's on-disk reality match its target — all shards present, at the active profile.** Its work queue is fed by node loss, failed-write gaps (shards skipped at ack), scrub findings (later), membership changes, and profile changes. Internally it performs two kinds of task:

- **Rebuild** — shards are missing; reconstruct them from any `k` survivors at the object's *existing* parameters and write them to healthy capacity. (A disk died; restore the spread.)
- **Re-encode** — the object sits below the *active* profile; read it, encode at the new parameters, write the new shards, commit one metadata transaction updating that version's parameters/partition/checksums, then delete the old shards. (The cluster grew; old data should benefit too.)

The operator sees one system: a progress line in `cluster status` ("repair: 312 GB queued, 14 MB/s"), and a `hamster cluster repair` verb for control — trigger a full sweep, pause, throttle. There is no separate command to remember; upgrading the profile automatically queues the re-encode work, throttled so foreground traffic wins.

Re-encode carries constraints that rebuild does not, which is why it lands one release later (see [ROADMAP.md](ROADMAP.md)) with its own ADR:

- **Identity is untouchable.** Same version ID, same content, same ETag, same timestamps, same lock state. Re-encode changes how bytes are stored, never what the object is — a precisely scoped exception to the "records never change after commit" principle of [ADR-0014](adr/0014-metadata-keyspace-design.md).
- **Crash-safe ordering**: new shards durable → metadata flips → old shards deleted. A crash anywhere leaves the old or the new fully readable, never neither.
- **It must work on COMPLIANCE-locked objects** — often exactly the data that most wants full durability — and the simulation harness must prove re-encode can never delete the last readable copy of anything.

Until re-encode ships, a profile upgrade protects new writes only, and `cluster status` reports how much data sits below the active profile — the gap is visible, never silent.

## What an acknowledgment promises

The write path ([ARCHITECTURE.md](ARCHITECTURE.md)) encodes, writes shards directly to the partition's nodes, then commits metadata through Raft. The rule for when the PUT may be acknowledged:

- **Healthy path: all `k+m` shards durable before ack.** No trailing writes, no background completion for the common case.
- **Degraded path: shards targeted at nodes the cluster has marked down may be skipped, with a hard floor of `k+1` durable shards.** Every skipped shard is queued for repair in the same breath as the ack.
- **Below the floor, the write is refused** (the S3 `SlowDown`/503 family) rather than accepted with zero tolerance. An acknowledged write that the very next disk failure can destroy is a lie, and Hamster does not tell it.

So the durability statement at ack time is: **every acknowledged object tolerates at least one node loss immediately, and the full `m` once repair catches up.** For `2+1` and `1+1` the floor equals `k+m` — writes need every node up, which is the honest cost of small clusters (the alternative is acking writes that one failure erases). `1+0` is the documented exception: its floor is its everything.

The simulation harness checks this per object, tracking each object's actual budget at ack (`durable − k`) rather than assuming the healthy-path `m` — see [SIMULATION.md](SIMULATION.md), invariant 1.

## Small objects: replication is `k=1` erasure coding

Erasure-coding a 4 KiB object into `4+2` produces six ~1 KiB shards: six disk writes, six network hops, and any read touches four nodes to retrieve four fragments. For small objects the math inverts — replication is cheaper and faster.

Hamster handles this without a second storage path: objects below a size threshold are written with **`k=1, m` = the active profile's `m`** — and Reed-Solomon with one data shard *is* replication; each "parity" shard is a full copy. So a small object under the `4+2` profile is stored as `1+2`: three full copies on three distinct nodes, same node-loss tolerance as its big siblings, one-fetch reads.

No special cases anywhere downstream: placement, repair, the metadata record, and the simulation harness all see ordinary `k+m` parameters that happen to have `k=1`. The starting threshold is **128 KiB**, recorded implicitly by each object's own parameters, so tuning the threshold later changes new writes only and confuses nothing.

**The capacity honesty.** A small object pays the replication multiplier — `m+1` full copies, so 3.0× under `4+2`, not the profile's nominal 1.5× — and a workload made *entirely* of small objects pays it across the board. State that plainly rather than letting the table above read as a whole-cluster promise. It is still the cheaper option: erasure-coding a 4 KiB object into six ~1 KiB shards burns a full 4 KiB filesystem block per shard (24 KiB of real disk — *worse* than three copies) while tripling the operation count, so below the threshold the nominal EC overhead is fiction and replication wins on both capacity and speed. The overhead column is a claim about objects large enough for EC to mean something; in mixed workloads, large objects dominate capacity and the blended overhead approaches nominal.

For workloads genuinely dominated by small objects, the fix that earns EC economics is **packing** — aggregating many small objects into containers that are themselves erasure coded. That is a deliberate post-v1 candidate, not a v0 feature: see the open question below.

## Large objects: stripes

Objects are encoded in fixed-size **stripes** so a multi-gigabyte PUT streams through the write buffer with bounded memory — encode a stripe, write its shards, move on — and a ranged GET decodes only the stripes the range touches. The stripe layout and the self-describing shard file format are decided and built: [ADR-0026](adr/0026-stripe-and-shard-layout.md) (contiguous 256 KiB slices per shard, zero-padded final stripe, versioned protobuf shard headers). The metadata schema was already agnostic, recording per-object checksums and parameters either way.

## Deploying on real hardware

### One process per disk

A Hamster node is one process with one data directory. A machine with several disks runs **one node per volume mount** — same static binary, separate processes, separate ports:

```sh
hamster node init --data /mnt/disk1
hamster node init --data /mnt/disk2
hamster node init --data /mnt/disk3
```

This follows from the load-bearing invariant that *the failure domain is the node, not the disk*: a disk dying is a node dying, which placement, repair, and the durability math already understand. Hamster deliberately does not manage redundancy between a single node's disks — that would reinvent per-disk erasure inside the node, a second placement axis the design rejected. A user who prefers one process per machine can RAID/LVM the disks into one volume and run a single node on it, delegating disk redundancy to the OS; that is supported and sometimes right. But for a multi-server deployment, no-RAID node-per-disk is the recommendation: cross-*server* erasure coding is strictly stronger than per-server RAID (RAID5 survives one disk inside a box; nothing in the box survives the box), repair works disk-at-a-time instead of array-at-a-time, and there is no write hole.

Many nodes per machine is also why Raft membership does not mean Raft *voting*: voters are capped and zone-spread, and additional nodes join as learners ([ADR-0017](adr/0017-raft-voter-cap-learners.md)) — a 15-node or 100-node cluster keeps a 5-member quorum.

### Failure domains above the node: hosts and zones

With several nodes per machine, "never two shards on one node" is no longer enough — placement could stack an object's shards on one server's disks, and a server loss would exceed the budget. So nodes carry two labels ([ADR-0016](adr/0016-failure-domain-hierarchy.md)):

- **`host`** — detected automatically (machine identity); the five processes on one OVH box share it with zero configuration.
- **`zone`** — an operator label for the domain above the machine, defaulting to the host. Set it with `-zone` at `cluster init`/`join`, to whatever a correlated failure means in your world: an AWS availability zone (`-zone us-east-1a` — note AZs, like `us-east-1a`/`1b`/`1c`, not regions like `us-east-1`), a rack, a room.

The hard invariant stays node-level — never two shards of one object on the same node, always enforceable. Above it, placement *spreads*: shards distributed as evenly as possible across zones, then hosts, then nodes, and `cluster status` reports the achieved tolerance at each level. Three servers × five disks at `4+2` places two shards per server: any whole server can die (exactly `m=2` shards per object) with everything readable, and any two disks anywhere can die in a healthy cluster. A single-box deployment has one host and one zone — fine, and *stated* in status rather than hidden.

One cluster is one region. Raft quorum and shard writes are synchronous, so stretching a cluster across regions (us-east-1 to us-east-2) buys every write the inter-region round trip; spreading across AZs within a region is the intended wide case. Multi-region is a future replication feature between clusters, not a stretched cluster.

### Downtime versus loss

Two very different events hide inside "a node failed," and confusing them makes clusters look more fragile than they are:

**A node down with its disk intact loses nothing, ever.** A power cut, a reboot, a crashed process, every node in the cluster turned off at once — turn them back on and the cluster resumes. Each node recovers its local state from disk (the WAL replays; torn tails from mid-write crashes are tolerated by design — the simulation harness's crash schedules exist to prove exactly this), Raft re-forms quorum, and the API returns with no operator action. While too many nodes are down the cluster is *unavailable* — below Raft quorum no API call answers, below the ack floor writes are refused — but unavailability is never loss, and recovery from it is "plug it back in." There is no scenario where restarting intact nodes requires re-joining, repair, or recovery ceremony. (A 3-node cluster with two nodes powered off is not a disaster; it is a cluster waiting for electricity.)

**A destroyed disk permanently loses that node's shards, and the budget for that is `m`.** Each object tolerates losing up to `m` of its shards (per its own recorded parameters); repair rebuilds missing shards from any `k` survivors onto healthy capacity. The clock that matters is the repair window: while shards are missing, the remaining budget is smaller, and an object that loses more than `m` shards before repair catches up is gone — not damaged, not recoverable-with-effort: below `k` shards the information no longer exists. That boundary is not a Hamster limitation; it is the arithmetic every erasure-coded and every replicated system lives by. Redundancy is purchased capacity and `m` is the amount purchased — the only levers that move it are a wider profile, faster repair, and spreading shards across failure domains that don't fail together.

When loss does exceed the budget, what Hamster owes the operator is an honest accounting: metadata replicates separately through Raft and routinely survives data losses it cannot repair (quorum intact, or [`cluster recover`](#replacing-dead-hardware) on a survivor), so the cluster can produce an authoritative inventory of exactly which objects are unrecoverable. For the audit-shaped user, "these objects were lost" is a categorically better answer than "we are not sure what we had."

### Replacing dead hardware

- **Quorum intact** (any cluster of 3+ nodes — a dead disk among 15 doesn't blink): repair starts rebuilding the lost node's shards onto surviving capacity immediately, with no operator action. Replacement, whenever convenient: `hamster node init --data /mnt/newdisk`, `hamster cluster join`, `hamster cluster remove-node <dead-id>`. Rebalance flows partitions onto the fresh disk.
- **Quorum lost** (the 2-node cluster with a destroyed member, or a majority gone): the survivor cannot even commit the membership change to eject the dead node — that commit needs the quorum that died. `hamster cluster recover`, run on a surviving node, handles exactly this: it refuses to run if it can still reach the missing peers (the split-brain guard), demands confirmation naming the dead members, then rewrites local Raft membership to the survivors (the established `--force-new-cluster` maneuver from etcd). Quorum resumes at the new size; the replacement then joins *normally* and repair re-mirrors. For `1+1` this is provably safe: quorum-of-two means the survivor holds every acknowledged write.

## The growth story, end to end

The path a real deployment takes, with no step requiring a migration or a remembered command:

1. **One node.** `hamster node init --data ./data` — profile `1+0`, objects whole, S3 API fully functional. Nothing about this mode is a special case to outgrow.
2. **Three nodes** (or three disks). Two `hamster cluster join`s; auto-profile announces `1+0 → 1+1 → 2+1` along the way. New writes spread across three nodes and survive a node loss; Raft now has real quorum. Repair re-encodes the old single-copy data up to `2+1` in the background (once re-encode ships; until then `cluster status` reports the gap).
3. **Three servers × five disks.** Twelve more joins; auto-profile steps to `4+2`; zone spreading puts two shards per server. Any server can die outright; any two disks can die; voters stay at five, spread one-per-server first.

Each step is operator-commanded, announced as it happens, and additive in every format it touches.

## Open questions

- The small-object threshold (128 KiB to start) — settle with v0.3 benchmarks; per-object parameters mean changing it is free.
- Small-object packing (post-v1): aggregate many small objects into containers that are erasure coded as units, so small-object-heavy workloads get real `k+m` economics instead of the replication multiplier. Nothing in the metadata schema forbids it — a packed location is just another shape of data-plane address on a `VersionEntry` — but it brings compaction (deletes leave holes in packs), read indirection, and interactions with versioning and object lock that each need their own design. Only worth building for workloads *dominated* by small objects; mixed workloads already approach nominal overhead because large objects dominate capacity.
- Scrub and repair pacing, to settle with benchmarks in the hardening phase (v0.9): a full sweep reads and hashes every byte the cluster stores, which unthrottled will contend with foreground I/O — the known cost of every scrubbing store. To measure and tune: the throttle policy ("foreground traffic wins" needs a mechanism — rate limit, idle-priority I/O, or both), holder-side hashing moved off the event loop onto the data plane, sweep scheduling (continuous trickle vs. periodic full passes — the scrub interval bounds the window in which rot can accumulate toward `m`), and incremental verification state so a restarted sweep resumes instead of restarting.
- The re-encode task's detailed design (crash-safety proof, throttling policy, the scoped exception to record immutability) — its own ADR alongside the v0.4 work.
- Whether profiles ever become per-bucket (S3 storage classes shaped) — nothing in the metadata schema forbids it (parameters are per-object already), but v0 is one active profile, cluster-wide, on purpose.
- ~~Stripe size and shard file format~~ — settled in [ADR-0026](adr/0026-stripe-and-shard-layout.md): contiguous 256 KiB slices, self-describing shard files, slice size recorded per shard so retuning stays free.
- Deeper failure-domain hierarchies (disk < host < rack < AZ as four levels) if real deployments want them — additive on the two-level design.
