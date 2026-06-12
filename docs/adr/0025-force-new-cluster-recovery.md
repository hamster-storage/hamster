# ADR-0025: `cluster recover` is an offline force-new-cluster, and the local log wins

## Status

Accepted, implemented (`raftnode.ForceNewCluster`, `hamster cluster recover`).

## Context

Raft tolerates a *minority* of voters failing. When a majority is permanently gone — dead disks, not a reboot — quorum can never re-form: the survivors hold correct, fully-durable data they can never extend, and no amount of waiting fixes it. Every consensus system needs an explicit exit from this state (etcd has `--force-new-cluster`), and the v0.2 roadmap promises one.

The dangerous part is not the mechanism but the judgment call: "the others are gone forever" is the operator's claim, and if it is wrong — the missing nodes come back — there are now two clusters with diverging histories accepting writes under the same name. The design must make the destructive step explicit, loud, and as safe as a fundamentally unsafe operation can be.

## Decision

1. **Recovery is offline.** `hamster cluster recover` rewrites a *stopped* survivor's data directory (it refuses if the node's transport port is bound). The cluster is down anyway; mutating Raft state under a live process buys nothing and invites races. A crash mid-recovery falls back to the previous state — the rewrite is one rotation frame, atomic by the same torn-frame argument as snapshot rotation — and rerunning finishes the job.

2. **The local log wins, all of it.** Every entry in the survivor's log — the committed prefix *and* the tail past its commit point — becomes the new cluster's history. The tail may contain writes the old cluster never acknowledged (they get resurrected), but it may equally contain writes the dead majority committed and acknowledged that this survivor had not yet learned were committed. Local truth is the only truth left; dropping the tail could lose acknowledged data, which is the worse failure for a store whose first job is durability. Same call etcd made.

3. **The result is a single-voter cluster.** The rewrite is one log frame: a snapshot at the survivor's last index whose ConfState is the survivor alone, hard state commit pointed at that index. On the next start it elects itself immediately and grows again through ordinary token joins. Removed members' Raft IDs are never reissued (the ID counter jumps past them).

4. **The operator must say the scary thing.** Without `-force`, the command prints what recovery means — including that the other members' data directories hold a competing history and must never run again — and exits. There is no interactive prompt to fat-finger through and no way to run it by accident from a script that didn't spell out `-force`.

5. **Known limitation, stated where it bites:** if the CA key (init node) is among the dead, the recovered cluster cannot mint join tokens and therefore cannot grow. The command warns about exactly this. CA portability/recovery is future work alongside CA rotation ([ADR-0022](0022-cluster-mtls.md)).

## Consequences

- The dark scenario has a tested exit: simulation schedules cover full recovery (committed data intact, leads alone, accepts writes, grows by join) and tail resurrection (an isolated leader's unacknowledged entry survives recovery); the cluster package proves the operational flow over real TLS, including the running-node refusal and Raft-ID non-reuse.
- Resurrected tail writes are an anomaly clients can observe (a write that appeared to fail later exists). Accepted: it is disaster recovery, the alternative loses acknowledged writes, and the operator was told.
- Old members returning is *not* handled — it is forbidden, in the command's own output. Membership-bound certificate revocation (ADR-0022, pending) will eventually let the cluster enforce what today is an instruction.

## Alternatives considered

- **Online recovery (a live node demotes its dead peers).** Changing the voter set without quorum *is* the unsafe act; doing it under a running consensus instance multiplies the states to reason about. Rejected — etcd's offline precedent is the practiced path.
- **Drop the uncommitted tail (keep only provably committed entries).** Loses any write the dead majority committed that the survivor hadn't learned about — silently violating acknowledged durability to avoid resurrecting unacknowledged writes. The wrong trade for a store. Rejected.
- **Automatic recovery after a timeout.** A network partition long enough to trip the timeout would split-brain the cluster by design. Rejected without much agonizing.
- **Majority-of-survivors recovery (rebuild from several remaining nodes, picking the longest log).** More salvage in multi-survivor disasters, but it adds a log-comparison protocol for a case the single-survivor form already unblocks (recover one, rejoin the rest fresh). Deferred, not rejected.
