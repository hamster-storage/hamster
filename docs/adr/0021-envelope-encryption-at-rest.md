# ADR-0021: Envelope encryption at rest, encrypt-then-EC, pluggable key source

## Status

Proposed (design only; implementation scheduled with the [roadmap](../ROADMAP.md), after the framed stream of [DATA-STREAM.md](../DATA-STREAM.md) exists)

## Context

Hamster's target user has compliance-shaped needs — HIPAA and SEC 17a-4 territory — where encryption at rest is effectively table stakes, and S3 clients expect the `x-amz-server-side-encryption` surface to exist. At the same time, the strongest counter-forces in the design all push back on naive approaches: repair and rebalance must keep working without touching plaintext ([ERASURE-CODING.md](../ERASURE-CODING.md)), the simulator must drive the encrypted write path deterministically, the single-binary promise forbids requiring an external KMS, and a large share of real workloads (restic, kopia, rclone crypt) arrive already encrypted client-side.

There is also an operational truth that shapes the defaults: server-side encryption creates a new way to lose data. A user who loses the master key has lost every object, durably and correctly — the erasure coding will faithfully preserve ciphertext no one can read.

## Decision

**Envelope encryption, ordered encrypt-then-erasure-code, over the framed object stream:**

1. **Per-object data keys (DEKs).** Each encrypted object gets a fresh random 256-bit key; chunks are AES-256-GCM under that key with the chunk index as nonce (safe because the DEK is never reused; deterministic under simulation because the DEK is the only random input). Key generation is injected like every other entropy source — `crypto/rand` in production, seeded in the simulator.
2. **The DEK is wrapped by the cluster key-encryption key (KEK)** and stored, wrapped, as an additive `VersionEntry` field. One metadata read yields everything a GET needs; shards and shard checksums are ciphertext, so **repair, rebalance, scrub, and re-encode never need keys**.
3. **The KEK comes from a pluggable key source**, chosen at `cluster init`:
   - `--master-key-file` — a key Hamster generates, or one the user generated themselves: bring-your-own-key is the file case, not a separate feature;
   - a passphrase, run through a KDF (argon2id);
   - `--master-key-command` — an arbitrary command whose stdout is the KEK. This is the integration point for AWS KMS, AWS Secrets Manager, GCP, Vault, and anything else: the user's own CLI fetches or unwraps the key, and Hamster imports **zero** cloud SDKs. Native SDK integrations are not planned; any future exception must justify its dependency tree against [ADR-0011](0011-permissive-only-dependencies.md).
4. **Encryption is opt-in at init, enable-only thereafter.** Off by default; `cluster status` and the console state the posture plainly. Enabling on an existing cluster encrypts new writes and reports the mixed state honestly (a background rewrite can ride the repair re-encode machinery later). **Disabling once enabled is refused** — quietly weakening the at-rest posture is the compliance-wrong direction, and a cluster that must abandon encryption can do so the honest way: a new cluster and a copy.
5. **The S3 surface is SSE-S3 shaped**: `x-amz-server-side-encryption: AES256` reported on writes and HEADs when the cluster encrypts. SSE-KMS and SSE-C are out of scope for the first cut (the frame and DEK machinery leave room for SSE-C later).
6. **Every part of this is in the open-source binary.** Hamster has no enterprise edition, and if it ever grows one, encryption and key management will not be behind it: the project's origin story is what happens to users when core capabilities sit on the wrong side of a fence. Security is not an upsell.

## Consequences

- The compliance story becomes real: at-rest encryption with customer-controlled keys, no cloud account required, no external service required, key sourcing flexible enough for any secret manager via the command hook.
- Defaults stay honest about the trade: default-off is not a performance hedge, it is respect for the key-loss footgun — a homelab user who never reads the docs cannot brick their data by losing a key they did not know existed. Users who enable encryption made a decision and know a key exists. (Performance is a footnote, not a reason: AES-GCM with hardware AES moves multiple GB/s per core and sits next to erasure coding and the network in the profile. The honest reasons to leave encryption off are the key burden and workloads that already encrypt client-side.)
- The wrapped DEK rides the existing metadata path — replicated by Raft like every `VersionEntry` field, no separate key database to operate or lose.
- A KEK rotation story is now owed: rewrap all DEKs under a new KEK (a metadata-only scan — object data is untouched, because only the wrapping changes). Scheduled with the implementation, designed before v1.
- The simulator gains failure schedules it must cover: crash between blob write and metadata commit for encrypted objects, KEK unavailable at startup (refuse to serve encrypted reads, loudly), GCM tag corruption on read and scrub.

## Alternatives considered

- **EC-then-encrypt (per-shard encryption on each storage node).** Repair and re-encode would need keys on every node, the property "storage nodes know nothing" evaporates, and key exposure multiplies by the cluster size. Rejected — this single consideration fixes the pipeline order.
- **Default-on encryption.** Credible-sounding for a compliance product, but it silently hands every casual deployment a key to lose. AWS can default SSE on because AWS holds the keys; self-hosted cannot. Rejected.
- **Requiring an external KMS.** Breaks the single-binary, no-external-services promise for every user to serve the minority who have a KMS. The command hook serves that minority without the dependency. Rejected.
- **Importing cloud SDKs for native KMS integrations.** The AWS SDK alone is a dependency tree larger than Hamster; each integration helps one cloud's users and ships to everyone. The exec hook is cloud-neutral and zero-dependency. Rejected for v0/v1; revisit only with evidence the hook is insufficient.
- **Filesystem-level encryption only (LUKS, ZFS native encryption) and no application-layer story.** Genuinely fine for some homelabs and documented as such, but it protects disks, not objects: no SSE surface for clients, nothing travels with replication or backup, no per-object posture in `cluster status`. Insufficient as the answer, acknowledged as an option.
- **Encrypting inside BadgerDB only (metadata) and leaving object data plain.** Backwards — the object data is the asset. Badger's native encryption may still be used for the metadata side as a complement; it does not substitute.
