# ADR-0017: Raft voters capped at five; all other nodes join as learners

## Status

Accepted, implementation in progress: the consensus layer (`internal/raftnode`) implements learner admission, the five-voter cap, and automatic promotion of a caught-up learner when a voter seat is vacant — simulation-tested through join, growth-to-seven, and remove-voter schedules. Two pieces wait on later machinery: zone-aware voter selection needs the cluster layout (v0.4), and replacing a voter that *stays down* (as opposed to one that is removed) needs the health/failure-detection work — until then a dead voter holds its seat until an operator removes it.

## Context

Hamster's metadata is one Raft group ([ADR-0005](0005-metadata-badgerdb-raft.md)), and the deployment guidance recommends one node per disk ([ADR-0016](0016-failure-domain-hierarchy.md)). Three servers with five disks each is a fifteen-member group; an ambitious homelab is a hundred. Raft quorum cost grows with every voter — each commit waits on a majority, elections involve everyone, and practice (etcd) tops out at five or seven voters. Fifteen voting members is past sensible; a hundred is absurd. But every node still needs the metadata: any node serves any S3 request, and reads resolve against local replicated state.

`etcd-io/raft` supports this natively: **learners** replicate the log without voting or counting toward quorum.

## Decision

- **Voting membership is capped at five nodes.** All other nodes join the Raft group as **learners**: they replicate metadata fully and serve all API traffic identically to voters — they just don't vote.
- **Voter selection is automatic and zone-aware**: voters are chosen spread across zones, then hosts, first ([ADR-0016](0016-failure-domain-hierarchy.md)), so quorum itself survives a server or zone loss. With three servers × five disks, the five voters land on at least all three servers.
- **Promotion is automatic**: when a voter is removed or stays down past a threshold, a healthy learner (again zone-spread) is promoted through an ordinary Raft configuration change. Demotion likewise when topology shifts.
- Clusters of five or fewer nodes: everyone votes; the cap is invisible.

## Consequences

- Metadata commit latency is a five-way quorum regardless of cluster size — a 100-node cluster has the same consensus critical path as a 5-node one. The wild home build works.
- The remaining scaling frontier is leader fan-out: the leader replicates the log to every member, learners included. Metadata entries are small and the write rate is metadata-ops only (never object data — first invariant), so this carries to low hundreds of nodes; beyond that is multi-raft territory, already the planned evolution.
- Strongly consistent reads keep working from any node: read-index goes through the leader and its voter quorum, exactly as before. Learners add no consistency caveats.
- Voter placement and promotion become testable logic in the simulation harness — including the schedule where a zone holding two voters dies and promotion must restore quorum resilience.
- `cluster status` shows each node's role; operators can see where quorum lives.

## Alternatives considered

- **Every node votes.** Simplest membership story; quorum latency and election storms grow with the cluster, and a fifteen-voter group is already outside practiced Raft deployment wisdom. Rejected.
- **Storage-only nodes outside the Raft group entirely.** Less replication fan-out, but those nodes can't serve metadata locally — every S3 request they handle adds a hop, and "any node serves any request" gets a footnote. Metadata is small; replicating it everywhere is the cheaper simplicity. Rejected for v0, worth revisiting with multi-raft.
- **Operator-designated voters.** Nobody should babysit quorum placement by hand, and the failure mode (all voters accidentally on one server) is exactly what ADR-0016 exists to prevent. Automatic with status visibility; a manual override can arrive later if anyone demonstrates a need. Rejected as the default.
- **A separate coordination tier (dedicated metadata nodes).** That is SeaweedFS's shape — capable, but it reintroduces roles and contradicts "every node runs the same binary the same way." Rejected ([ADR-0002](0002-single-binary-no-external-dependencies.md)).
