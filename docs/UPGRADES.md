# Upgrading a Hamster cluster

Hamster upgrades **one node at a time, with no downtime** — the cluster keeps
serving reads and writes throughout. This is the operator's procedure and the
guarantees behind it. The design is [ADR-0034](adr/0034-rolling-upgrade-machinery.md).

> **v0 caveat.** On-disk and on-wire formats may still change between v0 releases,
> so a given v0.x → v0.y step is not guaranteed format-compatible yet — check the
> release notes. The machinery described here is what makes upgrades safe and is
> what the v1 compatibility promise ([ADR-0010](adr/0010-v1-compatibility-policy.md))
> will stand on. The single-node `serve` preview has one version and nothing to
> roll; this is a cluster procedure.

## How it works

Hamster does **not** install or swap binaries — that is your deployment system's
job, and it differs by how you run Hamster (a new container image, a package
upgrade, a new Helm chart, a fresh binary on a VPS). What Hamster owns is the
**safety machinery** around that swap:

- **Version advertisement.** Each node advertises its binary version and a declared
  *protocol generation*. The cluster's *effective generation* is the minimum across
  live members and rolls forward automatically — etcd-style, no manual finalize —
  once the last node has upgraded. `cluster status` shows both.
- **The health interlock.** `cluster can-stop <node>` answers whether taking a node
  down right now is safe: Raft quorum survives without it, no *other* node is down,
  and no data migration is in flight. It exits `0` (safe) or `1` (not).

An upgrade is just a **maintenance reboot in which the binary changed** — the same
brief-outage the erasure coding already tolerates, plus the `can-stop` check before
each step.

## The one rule: one version at a time

Upgrade (and downgrade) **one release at a time** — v0.9 → v0.10 → v0.11, never
v0.9 → v0.11 in a single hop. Expand-then-contract format changes span exactly two
adjacent generations, so skipping one can skip a step a later version assumes
happened. This is the same constraint as a Kubernetes minor-version upgrade.
`cluster status` flags a generation skew greater than one step.

## The procedure

For each node, one at a time:

```sh
# 1. Is it safe to take this node down? (exits non-zero if not)
hamster cluster can-stop -data-dir <dir> -node <nodeID>

# 2. If safe, stop the node and replace its binary with your deployment mechanism
#    (see below), then start it again from the SAME data directory.

# 3. Confirm it rejoined and its version rolled, before moving to the next node:
hamster cluster status -data-dir <dir>
#    - the node is back among the expected voters
#    - its VERSION/GEN columns show the new build
#    - no node shows STATE "down"
```

Only move to the next node once the upgraded one is fully back. When the **last**
node lands, the effective generation rolls forward on its own — `cluster status`
shows it advance and the "upgrade in progress" note clears.

### Swapping the binary, by deployment mode

The stop/swap/start in step 2 is whatever your environment already does to replace
a process:

- **Binary on a VM/VPS (systemd).** Install the new binary, then
  `systemctl restart hamster` for that node. (A `systemctl stop`, replace, `start`
  is equivalent.)
- **Docker.** Pull the new image and recreate the one container, pointing at the
  same data volume: `docker compose up -d --no-deps <node>` (or `docker stop` /
  `docker run` the new tag against the same volume).
- **Kubernetes.** Roll the node's pod to the new image — e.g. one `StatefulSet`
  pod at a time, or set the image and let the rollout replace pods. Gate each step
  on `cluster can-stop` (a `preStop` hook or an external roller that checks it)
  so the rollout proceeds only from full health.

In every case the node keeps its **data directory** across the swap — its identity,
certificates, and replicated state live there, so it rejoins as the same member.

## Rollback

If an upgraded node misbehaves, roll it **back one generation** the same way:
stop it, put the previous binary back, start it. A node on the older release reads
everything the newer one wrote, because format changes are additive — a newer node
only ever *adds* fields an older node ignores. Roll back the same node-at-a-time,
and never more than one generation below the rest of the cluster.

## What the machinery guarantees

Proven by the end-to-end upgrade suite (`TestClusterRollingUpgrade`,
[SIMULATION.md](SIMULATION.md)): a three-node cluster rolled node by node under a
live read/write workload keeps every object readable and bit-intact, keeps a
COMPLIANCE-locked object WORM across the upgrade, and rolls the effective
generation forward only once the final node has upgraded.

The interlock is **advisory**: it informs your decision but never refuses a
shutdown — a node must always be stoppable in an emergency. In a genuine outage you
take a node down regardless; the erasure coding tolerates it and repair rebuilds
whatever it missed when it returns.
