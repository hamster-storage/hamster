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

v0.6 object lock shipped (v0.6.0). The front line is now **encryption at rest** —
envelope encryption, encrypt-then-EC, over the framed object stream, fully
designed in [ADR-0021](adr/0021-envelope-encryption-at-rest.md) (per-object DEKs,
the DEK wrapped by a cluster KEK from a pluggable source, opt-in/enable-only, an
SSE-S3 surface). The design is decided; v0.7 is execution.

**This one is genuinely different from v0.5/v0.6.** Versioning and object lock were
mostly "expose what the metadata already models." Encryption is not pre-built — a
survey of the tree found:

- `internal/stream` already reserves `flagEncrypted` in the frame header, but a
  frame with it set is *refused* today, and the size arithmetic assumes identity
  frames (`stored == plaintext`). The framing slot is ready; the transform and the
  encrypted-size math (a per-chunk GCM tag) are not.
- No crypto/key machinery exists at all (`internal/certs` is mTLS only): no
  AES-GCM transform, no DEK or KEK, no key source, no SSE surface, and no
  `VersionEntry` encryption field.
- The pipeline order is fixed (ADR-0021, [DATA-STREAM.md](DATA-STREAM.md)): chunk →
  compress → encrypt → frame → EC. So encryption is a per-chunk transform *inside*
  the stream layer; EC, repair, scrub, and rebalance see ciphertext and never need
  a key — the property that keeps storage nodes key-free.

Because it touches the read/write path, v0.7 must clear the deterministic
simulation harness (invariant 5) the way the EC path did. Passes, data-path-first
(the S3 surface is last, not first). Three passes have landed:

- **Pass 1 — the `internal/stream` AES-256-GCM transform.** Encrypted frames
  round-trip, are golden-pinned, and reject every single-byte tamper and the wrong
  key; the size/cover arithmetic accounts for the per-chunk tag.
- **Pass 2 — `internal/keys`: DEK lifecycle and the KEK source.** DEK generation
  from an injected entropy `io.Reader`; DEK wrap/unwrap under the KEK (stdlib
  AES-256-GCM, the wrap nonce derived from the object's unique version ID); one KEK
  source, `--master-key-file` (raw/hex/base64), behind a "source returns a 32-byte
  KEK" abstraction. No new dependency.
- **Pass 3 — encryption in the coordinator + the metadata field.** `coord` PUT mints
  a DEK, encrypts through the stream transform, wraps the DEK under the node's KEK,
  and commits the wrapped DEK + algorithm in additive `VersionEntry` fields (20/21)
  carried by the `PutObject` proposal (16/17). GET unwraps and decrypts; a missing
  KEK refuses loudly. Repair/scrub/re-encode are untouched — they re-shard
  ciphertext and never see a key (`ApplyReEncodeObject` preserves the encryption
  fields). Proven under the sim: encrypted round-trip with on-disk ciphertext
  (A/B-checked against a plaintext write), no-KEK read refusal, encrypted
  determinism, repair healing an encrypted object's lost shard key-free, and a
  coordinator crash mid-encrypted-PUT committing nothing.

Remaining:

1. **Cluster posture and key availability.** `cluster init` key-source flags; a
   replicated encryption posture (on/off + algorithm) so every node agrees;
   enable-only (disabling refused, ADR-0021); `cluster status` reports it; a node
   that cannot load its KEK at startup refuses encrypted reads *loudly* rather than
   serving ciphertext or failing quietly. The KEK is never stored or transmitted.

2. **The SSE-S3 surface.** `x-amz-server-side-encryption: AES256` on PUT/HEAD/GET
   when the cluster encrypts; the request header accepted; SSE-KMS and SSE-C
   refused honestly (the DEK machinery leaves room for SSE-C later).

3. **KEK rotation, verification, docs.** KEK rotation as a metadata-only rewrap
   scan (rewrap every DEK under a new KEK — object bytes untouched). Verification:
   stream golden/tamper, key-source units, the coord sim schedules above, a cluster
   e2e (an encrypted cluster, the posture in `status`, the SSE header, a read after
   a node restart that must reload its KEK), and the `aws` CLI SSE round-trip under
   `compat`. Docs: DATA-STREAM.md (encryption wired), S3-API.md (SSE surface);
   ADR-0021 moves Proposed → Accepted, with a KEK-rotation ADR if the flow makes a
   real decision.

**Open design questions — settled 2026-06-16:**

- **Single-node `serve` scope → cluster-only.** `hamster serve` (the single-node
  dev preview) does *not* encrypt in v0.7; encryption is a cluster feature. The
  stream/key machinery is shared, so `serve` support can be added later cheaply if
  wanted, but the compliance story lives on the cluster and that is where v0.7 puts
  it.
- **Replicated posture → meta singleton.** The is-encrypted/algorithm posture lives
  in a stored meta singleton, mirroring the stored cluster layout
  ([ADR-0028](adr/0028-stored-cluster-layout.md)): replicated through Raft so every
  node converges, kept orthogonal to placement (a layout change never touches
  encryption state). The KEK is never part of it — only the posture.
- **Cluster KEK distribution → operator-provisioned, never in `join`.** The
  operator configures the *same* key source on every node (same file, passphrase,
  or command); each node loads the KEK independently at startup; a joining node that
  cannot produce the cluster's KEK cannot serve encrypted reads. The KEK is never
  stored on disk by Hamster and never transits the cluster — `join` does **not**
  carry it. Provisioning the key source is an out-of-band operator responsibility,
  like any other secret.

**The other half of v0.7's "keys" — CA custody and rotation**
([ADR-0029](adr/0029-ca-custody-and-issuance.md),
[ADR-0022](adr/0022-cluster-mtls.md)): making issuance pluggable (operator/external
PKI), and lost-CA-key recovery via a multi-CA trust bundle.
[ADR-0022](adr/0022-cluster-mtls.md) pairs CA rotation with KEK rotation, so it
belongs here as a keys problem. This is a second track that may split into v0.8 if
encryption at rest alone fills v0.7 — decide once the encryption passes are scoped.

## Later versions

The headline feature of each later release is in [ROADMAP.md](ROADMAP.md): v0.8
upgrade machinery, v0.9 zero-downtime rolling upgrades. They are pulled into the
section above as they become the front line.
