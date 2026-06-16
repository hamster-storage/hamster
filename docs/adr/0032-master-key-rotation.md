# ADR-0032: Master key rotation — metadata-only DEK rewrap with per-version key fingerprints

## Status

Accepted

## Context

[ADR-0021](0021-envelope-encryption-at-rest.md) established envelope encryption
at rest: each object gets a per-object data key (DEK) that encrypts its bytes
before erasure coding, and the DEK is wrapped under a cluster key-encryption key
(KEK) and stored as an additive `VersionEntry` field. That ADR closed by naming
the debt this one pays: *"A KEK rotation story is now owed: rewrap all DEKs under
a new KEK (a metadata-only scan — object data is untouched, because only the
wrapping changes). Scheduled with the implementation, designed before v1."*

Rotation is not optional for the workloads Hamster targets. A master key has a
lifetime: it is rotated on a schedule by policy, and it is rotated *now* when it
may be compromised. The envelope design exists precisely so this is cheap — the
master key wraps the DEKs, not the object bytes, so rotating it rewrites only the
small wrapped keys and never re-encodes a single shard. [ADR-0022](0022-cluster-mtls.md)
pairs CA rotation with KEK rotation as one keys problem and one release (v0.8);
this ADR settles the **KEK** half. CA custody and rotation (the multi-CA trust
bundle, pluggable issuance) build on [ADR-0029](0029-ca-custody-and-issuance.md)
and get their own ADR when that track starts.

The design pressure that shapes the decision: a rotation must be **observable and
provably complete**. The operational hazard is not the rewrap itself — it is
knowing *when it is safe to retire the old key*. Retire it one object too early
and that object is unreadable, durably and correctly (the erasure coding will
faithfully preserve ciphertext no one can open). So the design must answer, at any
moment, "which objects are still protected by the old key?" — cheaply, without
trying to unwrap the whole keyspace.

## Decision

**Master key rotation is a metadata-only DEK rewrap sweep, made observable by a
per-version KEK fingerprint.**

1. **Rewrap, never re-encrypt.** A rotation walks the keyspace, unwraps each
   version's DEK under the old KEK, rewraps it under the new KEK (the wrap nonce
   stays the version ID, as [ADR-0021](0021-envelope-encryption-at-rest.md)), and
   commits the new `WrappedDEK` through an additive metadata proposal. Object
   bytes, shards, and shard checksums are never touched — only the ~60-byte
   wrapped key alongside each version changes. This keeps the whole sweep off the
   data path and trivially COMPLIANCE-safe: the lock state and the protected bytes
   are untouched, so a rewrite cannot delete or weaken a locked version (invariant
   #4).

2. **Each version records the fingerprint of the KEK that wrapped it.** An
   additive `VersionEntry` field holds a short (64-bit) **KEK fingerprint** — a
   domain-separated, preimage-resistant hash prefix of the key material, computed
   at key load (`keys.LoadKEK`) and cached on the loaded KEK. The fingerprint
   identifies the key by its *content*, so it is self-describing: given an object
   and a set of candidate keys, the right one is known without trial. A
   fingerprint reveals nothing usable about the key (preimage resistance), so it
   is safe to store in replicated metadata and show in status.

3. **The posture records the current KEK fingerprint.** The replicated
   `EncryptionPosture` singleton ([ADR-0021](0021-envelope-encryption-at-rest.md))
   gains an additive *current KEK fingerprint* and, during a rotation, a *rotating-
   to fingerprint*. New encrypted writes stamp the current fingerprint. A node
   whose loaded KEK fingerprint does not match the posture's current fingerprint
   **refuses to write encrypted objects** — a misconfiguration guard v0.7 lacks:
   today two nodes that load *different* master keys would each silently wrap under
   their own key and reads would fail confusingly later; the fingerprint catches
   split-key misconfiguration at write time. (Reads stay posture-free and
   fingerprint-directed: a node serves any version it has a matching KEK for.)

4. **Precise, observable progress.** Because every version carries its wrapping
   fingerprint, the count of versions still on the *old* fingerprint is a cheap,
   exact progress signal — the rotation is a converging number, surfaced in
   `cluster status` and a natural future metric. Completion is *provable*: zero
   versions remain on the old fingerprint. Only then is it safe to retire the old
   key, and the operator (or an automated gate) can see exactly that.

5. **One rotation at a time.** A second rotation is refused while one is open
   (mirroring the one-layout-op-at-a-time rule). A rotation runs to completion —
   zero stragglers on the old fingerprint — before the next may begin, so the
   cluster never holds more than two KEKs and key custody stays bounded. The new
   key reaches the cluster on the leader for the sweep's duration; standing nodes
   continue to load their key from `cluster run -master-key-file`.

6. **A leader-only sweep, resumable and crash-safe.** The rewrap runs like
   `Optimize` and the background scrubber: leader-only, under the shared single-
   flight guard, paced over the keyspace. A crash mid-rotation loses nothing — each
   rewrap is an independent committed proposal, and the sweep resumes by scanning
   for versions still on the old fingerprint. It stands aside while a layout
   transition is open, the same as the scrubber. Proven under the deterministic
   simulation harness (invariant #5): rotate and every object still decrypts; crash
   mid-rotation and the resumed sweep converges; a COMPLIANCE-locked version is
   rewrapped without its lock or bytes changing.

7. **The CLI surface.** `cluster rotate-key -new-master-key-file <path>` loads the
   new key on the leader, advances the posture to *rotating-to* its fingerprint,
   and drives the sweep; `cluster status` shows the rotation in flight and the
   straggler count. Both keys are loaded only for the window. The new-key source
   reuses the `--master-key-file` shape ([ADR-0021](0021-envelope-encryption-at-rest.md):
   raw/hex/base64), so passphrase/KDF and `--master-key-command` sources extend it
   additively later.

**Back-compatibility (invariant #2).** A `VersionEntry` written by v0.7 carries no
fingerprint field; an absent fingerprint means *wrapped under the cluster's
founding KEK*. A posture with no current fingerprint (an upgraded v0.7 cluster)
has it established lazily from the leader's loaded KEK on the first v0.8 encrypted
write or rotation. The first rotation stamps fingerprints onto every previously
unmarked version as it rewraps them, so the legacy "absent = founding" state is
transient and self-clearing. New code always reads the old format.

## Consequences

- The rotation debt from [ADR-0021](0021-envelope-encryption-at-rest.md) is paid,
  and paid cheaply: rotating the master key is metadata-only, off the data path,
  and never re-encodes a shard — exactly what the envelope split was *for*.
- Key custody on a compromised key has a clear, observable endpoint: rotate, watch
  the old-fingerprint count fall to zero, retire the old key knowing — not hoping —
  that nothing still depends on it.
- The misconfiguration guard is independent value beyond rotation: a node that
  loads the wrong master key is refused at write time instead of corrupting the
  cluster's readability silently.
- Per-version fingerprints make key lineage an auditable, queryable fact — on
  brand for a compliance-shaped store, and the foundation for a future
  observability surface (a histogram of versions per KEK fingerprint is the
  rotation's progress and the cluster's key-custody state at a glance).
- The simulator gains rotation schedules: crash mid-rewrap (resume converges),
  rotation racing a layout transition (rotation yields), a fingerprint-mismatched
  node refused at write, and a COMPLIANCE-locked version rewrapped lock-intact.
- One additive `VersionEntry` field is spent on every encrypted version forever
  for state that is only *interesting* during the rare rotation window. This is the
  accepted cost; a few bytes next to the 60-byte wrapped DEK is negligible, and it
  is what buys observability and the misconfig guard.

## Alternatives considered

- **Try-both unwrap, no per-version id.** During a rotation a node holds the old
  and new KEK and tries the new first; GCM's authentication tag makes a wrong-key
  unwrap fail cleanly, so this is *safe* — it is not a guess. It was rejected not
  for correctness but for opacity: it cannot answer "which objects still need the
  old key?" without trial-unwrapping the entire keyspace, so it cannot cheaply
  prove a rotation complete or a key safe to retire, and it offers no audit or
  metrics surface. The whole operational value is in the question try-both can't
  answer.
- **A monotonic generation counter instead of a content fingerprint.** A counter
  needs an external counter→key registry to mean anything, ties key identity to
  rotation ordering, and is not self-describing — an object stamped "gen 3" is
  opaque without the registry. A content fingerprint identifies the key from the
  key itself, with no registry to keep or lose. Rejected.
- **Re-encrypting object bytes on rotation (fresh DEK + re-shard).** Enormous I/O,
  drags the rotation onto the data path and through erasure coding, and risks the
  COMPLIANCE invariant by rewriting shards of locked objects — all to rotate a key
  the envelope design specifically arranged never to require it. The DEK layer
  exists so the master key rotates without touching bytes. Rejected as defeating
  the point.
- **Lazy rotation: rewrap each object the next time it is read/written.** Never
  converges — a cold object that is never read keeps the old key alive
  indefinitely, so the old key can never be retired and there is no completion
  signal. The exact opposite of the observable property this ADR is built around.
  Rejected.
- **Replicating both KEKs through Raft so any node rotates autonomously.** Same
  objection as [ADR-0029](0029-ca-custody-and-issuance.md) on the CA key — the
  master key must have the smallest footprint in the system, never the largest.
  The KEK stays operator-supplied and in memory only; the sweep is leader-driven
  with the new key loaded only for the window. Rejected.
