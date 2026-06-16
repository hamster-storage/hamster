# How Hamster compares

A self-hosted, S3-compatible object store is not a new idea. This page is an
honest map of where Hamster sits next to the systems people already know, so you
can tell quickly whether it fits your problem — or whether one of the others is
the better tool today.

Two ground rules for this page:

- **It names names, and it stays fair.** Comparing against real systems is more
  useful than comparing against strawmen. Every system mentioned here is good at
  what it was built for; the point is *fit*, not winners. Where Hamster is behind,
  this page says so plainly.
- **It is dated and it is honest about status.** Hamster is at **v0 and not
  production ready** — much of what is described elsewhere in these docs is the
  design being built, not a shipped guarantee. The other projects move too. Treat
  everything here as true to the best of our knowledge in **mid-2026**, and verify
  current state before you make a decision on it.

## The short version

Hamster aims at a specific gap: the operator who runs **real workloads with
compliance-shaped needs** — retention, object lock, WORM, audit (HIPAA, SEC
17a-4 territory) — but does **not** have a storage platform team. The existing
open-source field tends to make you pick one or the other:

- the **heavyweight distributed stores** give you every knob and scale to
  petabytes, but ask for real operational investment to run safely;
- the **lightweight single-node and simple stores** are a joy to operate, but
  leave out the durability or compliance machinery regulated data needs;
- the **feature-racing rewrites** are moving fast and fully open, but the
  load-bearing pieces (distributed mode, key management, WORM) are often still
  stabilizing.

Hamster's bet is the **missing middle**: one static binary, erasure-coded
durability, and first-class versioning/object-lock — with correctness held to a
deterministic simulation harness rather than hoped for. It is younger and
narrower than any system below. That is the trade.

| | Hamster | Heavyweight distributed (e.g. Ceph) | The archived community store (MinIO origin) | Breadth-first Apache rewrite (e.g. RustFS) | Simple single-node stores (e.g. Garage, SeaweedFS) |
|---|---|---|---|---|---|
| **Deploy unit** | one static binary, no external services | many daemons (MON/OSD/MGR…), often orchestrated | single Go binary | single binary | single/few binaries |
| **Durability model** | Reed–Solomon erasure coding, node-level failure domain | replication or EC, very flexible | erasure coding | erasure coding | replication (EC varies) |
| **Placement** | weighted rendezvous + fixed partitions + failure-domain hierarchy | CRUSH (straw2 ≈ weighted rendezvous) + placement groups | deterministic, sets-based | similar lineage | hash/replication-based |
| **Object lock / WORM** | GOVERNANCE + **no-override COMPLIANCE**, legal holds (v0.6) | yes (RGW) | yes | not yet, at time of writing | limited / no |
| **Versioning** | yes (v0.5) | yes | yes | partial | limited |
| **Correctness story** | deterministic simulation harness in CI from v0.1 | huge production track record | large production track record | conventional tests | conventional tests |
| **License** | Apache 2.0, permissive-only deps, line drawn early | LGPL/various | community edition **archived**; vendor steers to commercial | Apache 2.0 | varied permissive |
| **Maturity** | **v0, not production ready** | very mature | mature (community edition frozen) | young, fast-moving | mature for their niche |
| **Operational weight** | minimal by design | substantial | low | low | low |

The rest of this page is the reasoning behind those rows.

## vs. heavyweight distributed stores (Ceph as the example)

Ceph is the reference point for "a real distributed storage system." It is
genuinely more capable than Hamster in almost every dimension that isn't
operational simplicity: it does block, file, and object on one substrate; it
scales to thousands of nodes and exabytes; its RADOS Gateway already supports S3
versioning and object lock; and it has a deep production track record Hamster
will not match for years. If you have the operational maturity to run it, or a
workload that needs that scale and flexibility, Ceph is the safer bet.

**Where Hamster's design differs — and it's mostly about who has to run it:**

- **Placement is the same family.** Ceph's CRUSH (in its modern `straw2` bucket
  form) is mathematically a *weighted rendezvous hash* over a failure-domain
  hierarchy, mapping objects to placement groups and groups to devices. Hamster
  uses exactly that core — weighted rendezvous over a node→host→zone hierarchy
  ([ADR-0016](adr/0016-failure-domain-hierarchy.md)), with a fixed **partition**
  layer playing the role of Ceph's placement groups
  ([ADR-0004](adr/0004-partitioned-placement.md)). The difference is scope:
  Hamster ships *one* bucket algorithm, freezes the partition count for good
  (critical invariant #7), and holds placement to **fixed-point integer
  arithmetic with no floating-point** — so a cluster that mixes CPU architectures
  (trivial with `CGO_ENABLED=0` cross-builds) can never split placement through
  platform-divergent transcendental math. It is CRUSH's proven core, deliberately
  pared down.
- **One binary vs. a cluster of daemons.** Ceph is monitors, OSDs, managers, and
  usually an orchestrator. Hamster is a single static binary with the metadata
  (Raft) and data planes built in and mutual TLS bootstrapped from the first node
  — nothing external to stand up, no ZooKeeper/etcd/database. That is the entire
  positioning: the compliance-shaped operator *without* a platform team.
- **Correctness by simulation.** Hamster's durability, repair, placement, and
  consensus run under a deterministic simulation harness that injects partitions,
  crashes, and reordering on seeded schedules
  ([ADR-0009](adr/0009-deterministic-simulation-testing.md), critical invariant
  #5). This is a different *kind* of evidence than a long production history — it
  is reproducible and runs on every change — but it is not a substitute for
  Ceph's scale of real-world exposure, and we don't claim it is.

**Pick Ceph if** you need multi-protocol storage, very large scale, or maximum
flexibility and you can operate it. **Pick Hamster if** you want erasure-coded
durability and compliance controls without running a distributed-storage platform.

## vs. the archived community store (the MinIO origin story)

Hamster exists because of a specific event: in 2026 the widely-used open-source,
single-binary S3 store **archived its community edition** and steered users toward
a commercial product, pulling features out of the freely-available console along
the way. That is the factual origin of this project — not a knock on the
engineering, which was excellent and which Hamster's single-binary,
erasure-coded shape openly takes after.

The architectural lineage is close: one Go binary, erasure coding, deterministic
placement, the full S3 surface. The community edition is also a *mature* system
with object lock, versioning, and encryption that Hamster is still building toward.
So the comparison here is not mainly about features — it is about **governance and
trajectory**:

- **The license line, drawn early and outward-only.** Hamster is Apache 2.0 with
  a permissive-only dependency rule ([ADR-0011](adr/0011-permissive-only-dependencies.md)),
  and the open/paid boundary is committed *in advance*: **data-security features —
  encryption, key management, TLS, object lock, integrity — are never gated**, and
  features never move from open to paid. The whole reason to be wary of a
  community edition being archived is the worry that the line will move inward
  later; Hamster's answer is to write the line down before there's anything to
  monetize, where it can only move in the open direction.
- **Correctness you can re-run.** The deterministic simulation harness is the
  differentiator Hamster leans on while it is young: durability and consistency
  are tested under injected failure, reproducibly, rather than rendered as a
  feature list.

**The honest caveat:** the archived community edition is a battle-tested system
and Hamster is v0. If you need something proven *today* and the license trajectory
is acceptable to you, that is a real and reasonable choice. Hamster is the bet that
a permissive, simulation-tested store with the compliance core built in is worth
following as it matures.

## vs. the breadth-first Apache rewrites (RustFS as the example)

A newer wave of fully-open (Apache 2.0) rewrites — RustFS is the most visible as
of mid-2026 — answered the same archival event by racing to reproduce the broad
feature surface: replication, bucket notifications, multi-tenancy, a console, a
wide S3 API. Fully-open Apache 2.0 is now **table stakes** in this space, not a
differentiator, and these projects deserve credit for moving fast in the open.

Hamster deliberately goes the other way — **depth-first, not breadth-first**:

- At the time of writing, the breadth-first rewrites tend to have their
  load-bearing pieces — distributed mode, lifecycle, key management/KMS — still
  labeled as stabilizing, and no object-lock/WORM story. Hamster treats those
  exact pieces as the *product*: erasure-coded distribution, repair, placement,
  and a tested no-override COMPLIANCE lock are the early releases, and the broad
  surface (events, multi-tenancy, a console) comes later.
- The durable differentiator Hamster is investing in is **evidence of
  correctness** — the simulation harness and honest, per-release status notes —
  rather than feature-count or small-file benchmark headlines, which are
  explicitly *not* a goal (see [CLAUDE.md](../CLAUDE.md)).

These projects may well close the depth gap; that is the nature of fast-moving
young software. The bet is that compliance-grade durability is harder to retrofit
than breadth is to add, so building it first is the more defensible order.
(Implementation language is not part of the story — Hamster being Go and a rival
being Rust says nothing about fitness for this job.)

## vs. simpler single-node and lightweight stores (Garage, SeaweedFS, …)

There is a healthy ecosystem of lightweight S3-compatible stores built for ease
and specific shapes — geo-distributed small clusters, masses of small files,
homelab simplicity. They are excellent at what they target and are often simpler
than Hamster for those cases.

The gap Hamster fills relative to them is the **compliance and durability core**:
first-class versioning, object lock with a genuinely-unbreakable COMPLIANCE mode,
WORM retention, and Reed–Solomon erasure coding with self-healing repair — the
machinery regulated data needs and that simpler stores usually, and reasonably,
leave out. If you don't need that machinery, a lighter store may be the better
fit. If you do, Hamster is trying to give it to you *without* stepping up to a
heavyweight platform.

## Where Hamster is honestly behind

So this page isn't a sales sheet, the current limits, plainly:

- **It is v0 and not production ready.** On-disk and on-wire formats may still
  change between v0 releases. Don't trust real regulated data to it yet.
- **No per-user authorization (IAM/bucket policies).** Every credential is
  full-access in v0.x. This has a real consequence for object lock: GOVERNANCE
  mode's bypass is *not* access-controlled the way AWS gates it behind an IAM
  permission — see [S3-API.md](S3-API.md). (COMPLIANCE mode is unaffected; it has
  no bypass at all.)
- **Not certified or assessed for any retention regulation.** Hamster implements
  the WORM *mechanism* SEC 17a-4(f)/FINRA 4511/CFTC 1.31 ask for, which is
  necessary but not sufficient, and it has not been assessed by anyone.
- **Narrower S3 surface and smaller scale, today.** Some operations are still
  leader-only on the cluster path, multipart/copy aren't on the cluster data path
  yet, and Hamster has nothing like the scale record of the mature systems above.
- **Young, with a small track record.** The simulation harness is strong evidence,
  but it is not the same as years of production exposure across many operators.

If any of those is disqualifying for you right now, one of the systems above is
the right call — and that's a fine outcome. Hamster is built in the open so you
can watch it close these gaps and decide when it's earned your data.

## See also

- [ARCHITECTURE.md](ARCHITECTURE.md) — how the request paths, metadata/data
  separation, and erasure coding actually work.
- [ADR-0004](adr/0004-partitioned-placement.md) — partitioned placement and the
  weighted-rendezvous design (the CRUSH comparison above, in detail).
- [ADR-0009](adr/0009-deterministic-simulation-testing.md) — the simulation
  harness that backs the correctness claims.
- [ROADMAP.md](ROADMAP.md) — what is shipped and what is planned.
