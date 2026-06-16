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

## Now / next — v0.5 full versioning API

The metadata has modeled every key as an ordered list of versions since v0.1
(invariant 3, [ADR-0006](adr/0006-versioning-and-object-lock.md)), so v0.5
changes **no schema** — and more than the model is already in place. The apply
layer carries the whole state machine: `SetBucketVersioning` (Enabled/Suspended),
delete markers in `ApplyDeleteObject`, permanent `ApplyDeleteVersion`, and the
null-version handling a suspended bucket needs. The version-aware reads
(`GetVersion`, `ListVersions`) exist, and version-aware shard reclaim
(`Coordinator.DeleteShards`, fed by the data IDs an apply reports freed) already
runs on the cluster path. v0.5 is therefore almost entirely the **S3 HTTP surface
that exposes all this** — the gateway first, then the `cluster run -s3` path.

Passes, in order — each its own focused change:

1. **Bucket versioning config.** `PutBucketVersioning` / `GetBucketVersioning` —
   the `?versioning` subresource: route it on `serveBucket`, parse and emit the
   `<VersioningConfiguration>` XML, drive the existing `SetBucketVersioning`
   proposal. Enabled and Suspended only (S3 never returns to Unversioned once
   enabled); apply already refuses Suspend on an object-lock bucket.

2. **versionId on object operations.** `GetObject` / `HeadObject` / `DeleteObject`
   with `?versionId=`, the `x-amz-version-id` response header on PUT/GET/DELETE,
   and delete-marker semantics: a versioned DELETE with no id drops a marker (apply
   does this), with an id permanently removes that one version (`ApplyDeleteVersion`
   plus the wired `DeleteShards` reclaim); a GET of a delete marker is `405` with
   `x-amz-delete-marker: true`; a GET of a missing version is `404`; a suspended
   bucket's PUT overwrites the null version. The `deleteObject` handler already
   captures `vid` and the freed-data-ID reclaim is scaffolded — this finishes and
   tests the surface.

3. **ListObjectVersions.** The `?versions` subresource: a new paged meta read over
   the version keyspace — `ListObjectVersions(bucket, prefix, keyMarker,
   versionIdMarker, delimiter, max)`, with `ScanVersions` as the primitive — and
   the `<ListVersionsResult>` envelope with interleaved `<Version>` and
   `<DeleteMarker>` entries, the `IsLatest` flag, `CommonPrefixes` under a
   delimiter, and key-marker + version-id-marker pagination.

4. **Onto the cluster `-s3` path.** Expose passes 1–3 on `cluster run -s3` (the
   `s3Server` / `clusterMetadata` adapters and the coordinator): a by-version
   GET/HEAD resolves a specific `VersionEntry`'s DataID exactly as a current-version
   read does; a permanent version delete reclaims shards through the already-wired
   `Coordinator.DeleteShards`. Mutations stay leader-only. Confirm the background
   scrubber and repair sweep — already version-aware, they walk every
   `VersionEntry` — converge with many versions per key.

5. **Verification.** Extend `task compat` with the real `aws` CLI versioning
   surface (`put-bucket-versioning`, `list-object-versions`, get/delete by
   `version-id`, the delete-marker dance); a `task e2e` versioning suite over a real
   cluster; meta reference-model coverage for the new paged read (byte-identical
   restart equivalence preserved); and a simulation schedule proving a permanent
   version delete frees shards and leaves no readable lie under crashes.

Decisions to record as the work lands (in [S3-API.md](S3-API.md), or a new ADR if
a real decision is made):

- **MFA Delete is out of scope** — an S3 legacy feature, not a compliance lever;
  object lock (v0.6) is the WORM mechanism, not MFA delete.
- **No shard sharing between versions** — each version stores independent shards
  (CopyObject is already read-and-rewrite). Refcounted sharing is a later storage
  optimization, never a v0.5 semantic.
- **Object-lock interaction is v0.6's** — the retention/legal-hold apply methods
  already exist in meta, but the guard that stops a permanent `DeleteVersion` from
  removing a locked version lands with the lock surface in v0.6. v0.5 must leave
  that hook clean: COMPLIANCE has no override path (invariant 4).

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.6
object lock (GOVERNANCE/COMPLIANCE, legal holds — building directly on the
versioning the v0.5 surface exposes), v0.7 encryption at rest, v0.8 upgrade
machinery. They are pulled into the section above as they become the front line.

CA custody, external/operator PKI, and CA rotation — including recovery from a
lost CA key via a multi-CA trust bundle ([ADR-0029](adr/0029-ca-custody-and-issuance.md),
[ADR-0022](adr/0022-cluster-mtls.md)) — ride **v0.7**, not the versioning work:
[ADR-0022](adr/0022-cluster-mtls.md) pairs CA rotation with KEK rotation, so it is
a keys problem. See the v0.7 note in [ROADMAP.md](ROADMAP.md).
