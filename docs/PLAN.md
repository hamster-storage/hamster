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

v0.5 versioning shipped (v0.5.0). Object lock builds directly on it: WORM
retention and legal holds over the versioned keyspace
([ADR-0006](adr/0006-versioning-and-object-lock.md)) — the compliance heart of
Hamster's positioning, and the release where invariant 4 stops being a promise
and becomes a tested guarantee.

As with versioning, the survey for this milestone found the load-bearing half
already built and Raft-ready:

- `VersionEntry` carries `RetentionMode` (None/Governance/Compliance),
  `RetainUntilUnixMS`, and `LegalHold`.
- `lockedAt(at, bypassGovernance)` is the guard: a legal hold blocks always;
  COMPLIANCE blocks until retain-until **regardless of bypass**; GOVERNANCE
  blocks unless bypass. `ApplyDeleteVersion` already calls it, so the hard floor
  (invariant 4 — no path deletes or shortens a COMPLIANCE-locked version) is
  already enforced where deletes happen.
- `ApplyUpdateRetention` (strengthen-only under COMPLIANCE; GOVERNANCE weakens
  only with bypass) and `ApplyUpdateLegalHold` exist, with codecs and dispatch.
- `ApplyPutObject` already accepts the lock fields and rejects them on a
  non-lock bucket; `ApplyCreateBucket` already takes `ObjectLockEnabled`; the
  gateway already threads `x-amz-bypass-governance-retention` into
  `DeleteVersion` and maps `ErrObjectLocked` → `403 AccessDenied`.

So v0.6 is again the **S3 surface**, gateway-first then cluster, in passes:

1. **Object-lock bucket creation + configuration.** Accept
   `x-amz-bucket-object-lock-enabled: true` on CreateBucket (today refused) —
   which enables versioning on that bucket, since object lock requires it — and
   serve `PutObjectLockConfiguration`/`GetObjectLockConfiguration` over the
   `?object-lock` subresource, including the optional bucket **default
   retention** applied to new objects.

2. **Per-object retention.** `PutObjectRetention`/`GetObjectRetention` over the
   `?retention` object subresource (driving the existing `ApplyUpdateRetention`),
   and the `x-amz-object-lock-mode` / `x-amz-object-lock-retain-until-date` PUT
   headers (plus any bucket default) flowing into `PutObject`. GET/HEAD surface
   the lock headers.

3. **Legal hold.** `PutObjectLegalHold`/`GetObjectLegalHold` over the
   `?legal-hold` subresource and the `x-amz-object-lock-legal-hold` PUT header,
   driving `ApplyUpdateLegalHold`.

4. **Onto the cluster `-s3` path.** Thread the lock fields through `coord.Put`
   into the `PutObject` proposal, add the retention/legal-hold proposals to the
   `clusterMetadata` adapter, and surface the lock headers on cluster reads —
   the same gateway, leader-only mutations.

5. **Verification.** Gateway and meta units; a cluster e2e; the real `aws` CLI
   under `task compat` (`put-object-lock-configuration`, `put-object-retention`,
   `put-object-legal-hold`, and the get variants); and — the load-bearing one —
   a simulation/invariant test that drives **every** delete-or-shorten path
   against a COMPLIANCE-locked version and proves refusal, including under a
   crash schedule. Invariant 4 is the thing this release exists to guarantee, so
   it gets an adversarial test, not just a happy-path one.

**Open design question (settle in pass 1):** the bucket *default retention* of
`PutObjectLockConfiguration` needs somewhere to live. `BucketConfig` carries
`ObjectLockEnabled` but no default rule, so this is an additive `BucketConfig`
field (mode + a duration), versioned per invariant 2 — a small schema addition,
the first of v0.6. Decide whether to store the duration in the S3 shape
(days/years) and apply it at PUT time, versus materializing an absolute date
onto the bucket; the former matches S3's `GetObjectLockConfiguration` round trip,
which argues for storing days/years.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.7
encryption at rest, v0.8 upgrade machinery. They are pulled into the section above
as they become the front line.

CA custody, external/operator PKI, and CA rotation — including recovery from a
lost CA key via a multi-CA trust bundle ([ADR-0029](adr/0029-ca-custody-and-issuance.md),
[ADR-0022](adr/0022-cluster-mtls.md)) — ride **v0.7**, not the versioning work:
[ADR-0022](adr/0022-cluster-mtls.md) pairs CA rotation with KEK rotation, so it is
a keys problem. See the v0.7 note in [ROADMAP.md](ROADMAP.md).
