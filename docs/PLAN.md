# Execution plan

What is being worked **now and next**, in priority order — the middle altitude
between [ROADMAP.md](ROADMAP.md) (high-level milestones per version) and the
ADRs (the reasoning behind each decision). A specific behavioral gap is tracked
by a named test, not retyped here; this file records *order and priority*, and
points at the test or ADR that holds the detail.

This is the **front line only**. Phases are pruned the moment they land — a
completed item's record survives in git history, in its now-green test, and in
the shipped ADR/doc. This file is not an archive and not a TODO graveyard: if a
line here is done, delete it.

## Now / next — v0.6 object lock

v0.5 versioning has **landed**. The S3 surface that exposes the version-list
model the metadata has carried since v0.1 (invariant 3,
[ADR-0006](adr/0006-versioning-and-object-lock.md)) is complete on both the
single-node gateway and the cluster data path: per-bucket versioning config
(Enabled/Suspended), `x-amz-version-id` on writes, `versionId` on GET/HEAD/DELETE
(with the null version and delete-marker semantics), `ListObjectVersions` (paged,
delimiter-grouped, `IsLatest`), and permanent version delete that frees the
version's shards. No schema change. Proven by gateway and meta unit tests, a
cluster e2e over the erasure-coded path, and the real `aws` CLI versioning surface
under `task compat`. MFA Delete is out of scope and cross-version shard sharing is
deferred — both recorded in [S3-API.md](S3-API.md).

The next front line is **v0.6 object lock** — GOVERNANCE and COMPLIANCE retention
and legal holds ([ADR-0006](adr/0006-versioning-and-object-lock.md)). As with
versioning, much is already in the metadata: the lock fields on `VersionEntry`,
the apply-layer guards (`ApplyUpdateRetention`, `ApplyUpdateLegalHold`, and the
`lockedAt` check that `ApplyDeleteVersion` already honors — invariant 4), and the
rule that an object-lock bucket cannot suspend versioning. So v0.6 is again mostly
the **S3 surface**, gateway-first: `PutObjectRetention`/`GetObjectRetention`,
`PutObjectLegalHold`/`GetObjectLegalHold`, `Put/GetObjectLockConfiguration`, the
`x-amz-object-lock-*` headers on PUT, the bypass-governance path
(`x-amz-bypass-governance-retention`, already plumbed through `DeleteVersion`),
and creating a bucket with object lock enabled (today refused). The hard floor:
no code path may delete or shorten retention on a COMPLIANCE-locked version — the
hook v0.5 left clean closes here. A survey of what apply already enforces vs. what
the surface must add is the first step.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.7
encryption at rest, v0.8 upgrade machinery. They are pulled into the section above
as they become the front line.

CA custody, external/operator PKI, and CA rotation — including recovery from a
lost CA key via a multi-CA trust bundle ([ADR-0029](adr/0029-ca-custody-and-issuance.md),
[ADR-0022](adr/0022-cluster-mtls.md)) — ride **v0.7**, not the versioning work:
[ADR-0022](adr/0022-cluster-mtls.md) pairs CA rotation with KEK rotation, so it is
a keys problem. See the v0.7 note in [ROADMAP.md](ROADMAP.md).
