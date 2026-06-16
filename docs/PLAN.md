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

## Now / next — v0.7 encryption at rest

v0.6 object lock shipped. WORM retention and legal holds are served on both the
single-node gateway and the cluster path — `Put/GetObjectLockConfiguration` with
a days/years bucket default, `Put/GetObjectRetention`,
`Put/GetObjectLegalHold`, the `x-amz-object-lock-*` PUT/response headers, and
bucket creation with object lock — and **invariant 4 is now a tested guarantee**:
an adversarial meta test drives every delete-or-shorten path against a
COMPLIANCE-locked version and proves refusal, with a gateway test and a cluster
e2e enforcing the same `403` end to end and the real `aws` CLI exercising the
surface ([ADR-0006](adr/0006-versioning-and-object-lock.md)).

The next front line is **v0.7 encryption at rest** — envelope encryption over the
framed object stream ([ADR-0021](adr/0021-envelope-encryption-at-rest.md)) with a
pluggable key source and the SSE-S3 surface (`x-amz-server-side-encryption:
AES256` on PUT/HEAD/GET). Unlike versioning and object lock, the metadata model is
*not* already half-built for this: it is a data-path change, so it must clear the
deterministic simulation harness (invariant 5) the way the EC path did. The
[DATA-STREAM.md](DATA-STREAM.md) design already reserves encryption as a per-chunk
transform in the chunk → compress → encrypt → frame → EC order, and the stream
header carries the reserved flag — so the framing is ready; v0.7 fills in the
transform, the key management, and the API surface. A survey of what the stream
layer reserves vs. what the key source and SSE headers must add is the first step.

v0.7 also carries the **CA custody and rotation** work
([ADR-0029](adr/0029-ca-custody-and-issuance.md),
[ADR-0022](adr/0022-cluster-mtls.md)): [ADR-0022](adr/0022-cluster-mtls.md) pairs
CA rotation with KEK rotation, so it is a keys problem that belongs with
encryption, not the data-path milestones before it.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.8
upgrade machinery, v0.9 zero-downtime rolling upgrades. They are pulled into the
section above as they become the front line.
