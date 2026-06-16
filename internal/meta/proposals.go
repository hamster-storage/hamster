package meta

// Proposals are the inputs to apply: one S3 mutation each, carrying every
// input apply needs — including the proposing node's clock reading and the
// gateway-minted version ID — because apply reads no ambient state
// (METADATA.md principle 4). The gateway fills these from its seam.Clock
// and World rand; apply trusts the fields and validates the semantics.
// The wire encoding for the Raft log lives in proposal_codec.go
// (EncodeProposal/DecodeProposal); field numbers match the Proposal
// envelope in METADATA.md and are pinned by golden tests.

// CreateBucket creates a bucket. ObjectLockEnabled implies versioning,
// enabled at creation and never suspendable (S3 semantics).
type CreateBucket struct {
	ProposedAtUnixMS  int64
	Bucket            string
	ObjectLockEnabled bool
}

// DeleteBucket deletes an empty bucket. Any version row — including a bare
// delete marker — makes the bucket non-empty.
type DeleteBucket struct {
	ProposedAtUnixMS int64
	Bucket           string
}

// SetBucketVersioning moves a bucket between VersioningEnabled and
// VersioningSuspended. Unversioned is not a reachable target: once
// versioning is enabled, S3 buckets never return to it.
type SetBucketVersioning struct {
	ProposedAtUnixMS int64
	Bucket           string
	State            VersioningState
}

// SetObjectLockConfiguration is S3 PutObjectLockConfiguration: it sets (or, with
// mode RetentionNone, clears) the bucket's default retention rule, applied to new
// objects that arrive without their own retention (ADR-0006). The duration is in
// the S3 shape — exactly one of Days or Years — never an absolute date; the
// per-object retain-until is computed at PUT time. Object lock must already be
// enabled on the bucket.
type SetObjectLockConfiguration struct {
	ProposedAtUnixMS      int64
	Bucket                string
	DefaultRetentionMode  RetentionMode
	DefaultRetentionDays  uint32
	DefaultRetentionYears uint32
}

// PutObject commits one object version. The data-plane facts (size, ETag,
// checksums, partition, EC parameters) are inputs: the shards are already
// durable when this proposal is made — the metadata commit is the
// linearization point (docs/ARCHITECTURE.md).
type PutObject struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string

	// VersionID is minted by the gateway and names the already-durable
	// data (it becomes the entry's DataID verbatim). Apply may bump the
	// committed VersionID for ordering; the data address never moves.
	VersionID    VersionID
	Size         int64
	ETag         []byte
	ContentType  string
	UserMetadata map[string]string

	Partition      uint64
	ECDataShards   uint32
	ECParityShards uint32
	ObjectChecksum []byte
	ShardChecksums [][]byte

	// Object lock applied at write time (x-amz-object-lock-* headers).
	RetentionMode     RetentionMode
	RetainUntilUnixMS int64
	LegalHold         bool

	// Encryption at rest (ADR-0021): set when the coordinator encrypted the
	// object, carrying the algorithm and the DEK wrapped under the cluster
	// KEK. Zero/empty for a plaintext write. KEKFingerprint names the KEK the
	// DEK was wrapped under (ADR-0032), so rotation can find versions still on
	// an old key; zero for a plaintext write.
	EncAlgorithm   EncAlgorithm
	WrappedDEK     []byte
	KEKFingerprint uint64
}

// PutResult reports the committed version ID, after any monotonicity bump,
// and the data addresses the commit displaced (the prior null version of an
// unversioned or suspended write) — the caller reclaims those.
type PutResult struct {
	VersionID       VersionID
	ReplacedDataIDs []VersionID
}

// DeleteObject is S3 DeleteObject without a version ID: on an unversioned
// bucket it removes the object; on a versioned bucket it inserts a delete
// marker (the null delete marker, under suspension).
type DeleteObject struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string
	VersionID        VersionID // minted for the delete marker, if one is created
}

// DeleteObjectResult reports what the delete did. S3 DELETE is idempotent:
// both fields false on a no-op is success, not an error.
type DeleteObjectResult struct {
	Removed       bool      // an unversioned object row was removed
	MarkerCreated bool      // a delete marker was inserted
	MarkerID      VersionID // its ID, after any monotonicity bump
	// RemovedDataIDs are the data addresses the delete freed — the removed
	// unversioned row's, or the null version a suspended-mode marker
	// replaced. The caller reclaims them.
	RemovedDataIDs []VersionID
}

// DeleteVersion is S3 DeleteObject with a version ID: it destroys one
// version row, subject to the lock check — which lives here, inside
// deterministic apply, with no time-of-check gap (METADATA.md).
type DeleteVersion struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string
	VersionID        VersionID
	BypassGovernance bool // x-amz-bypass-governance-retention; never affects COMPLIANCE or legal holds
}

// DeleteVersionResult reports whether a row was removed. A missing version
// is idempotent success.
type DeleteVersionResult struct {
	Removed bool
}

// UpdateRetention is S3 PutObjectRetention. COMPLIANCE retention may only
// strengthen: same mode, same-or-later date. GOVERNANCE may strengthen
// freely and weaken only with bypass.
type UpdateRetention struct {
	ProposedAtUnixMS  int64
	Bucket            string
	Key               string
	VersionID         VersionID
	Mode              RetentionMode
	RetainUntilUnixMS int64
	BypassGovernance  bool
}

// ReEncodeObject rewrites a committed version's erasure-coded representation to
// a new storage profile (ADR-0004, ADR-0015): a physical re-representation, not
// a content edit — the object's bytes, ObjectChecksum, Size, ETag, and object-
// lock fields are unchanged, and it stays the same version. Only the data-
// addressing and EC fields move: the new shards live under DataID at the new
// k+m. The coordinator writes the new shards durably before proposing and drops
// the old ones only after this commits. Used to step data down to a smaller
// profile when a cluster shrinks (and up as it grows). COMPLIANCE-safe: it never
// deletes the object or shortens retention.
type ReEncodeObject struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string
	VersionID        VersionID
	DataID           VersionID
	ECDataShards     uint32
	ECParityShards   uint32
	ShardChecksums   [][]byte
}

// UpdateLegalHold is S3 PutObjectLegalHold. Holds toggle freely by their
// own rules, independent of retention.
type UpdateLegalHold struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string
	VersionID        VersionID
	Hold             bool
}

// RegisterNode records a cluster member's failure-domain labels (ADR-0016)
// and capacity weight (ADR-0004) in the replicated store. The join issuer
// proposes it as part of admission; the layout reconcile composes the
// labeled layout from these rows, so any leader — not only the issuer that
// accumulated the labels on its local disk — can build a complete one.
// Idempotent by node ID: a re-registration with changed labels replaces the
// row, so a reconciling leader that retransmits converges every replica.
type RegisterNode struct {
	ProposedAtUnixMS int64
	NodeID           string
	Host             string
	Zone             string
	Capacity         uint32
}

// SetNodeDraining sets (or clears) the drain flag on an already-registered
// member (ADR-0004). It mutates only that flag, leaving the node's labels and
// capacity intact, so an operator can mark a node for removal without knowing
// its recorded labels. Apply refuses an unknown node ID.
type SetNodeDraining struct {
	ProposedAtUnixMS int64
	NodeID           string
	Draining         bool
}

// SetNodeReplacedBy records (or clears) which node is taking an existing
// member's place (ADR-0004). NodeID is the outgoing node; ReplacedBy is the
// incoming one (empty clears the pairing). It mutates only that field. Apply
// refuses an unknown node ID.
type SetNodeReplacedBy struct {
	ProposedAtUnixMS int64
	NodeID           string
	ReplacedBy       string
}

// SetClusterLayout installs a new cluster-layout generation (ADR-0028) —
// the replicated placement basis, not an object mutation. It is a
// compare-and-set: apply accepts it only when Version is exactly one
// greater than the stored layout's (the first install is Version 1), so a
// reconciling leader that retransmits, or two proposals that race, converge
// every replica to the same layout instead of clobbering each other.
// Nodes is the labeled member set placement spreads over (ADR-0016);
// Members is the older unlabeled form (v0.4 pass 1). New proposers set
// Nodes. PartitionCount is the cluster constant (ADR-0004), fixed at the
// first install.
type SetClusterLayout struct {
	ProposedAtUnixMS int64
	Version          uint64
	PartitionCount   uint32
	Members          []string
	Nodes            []LayoutNode
	// Previous is the member set the layout is migrating away from (ADR-0004):
	// set when opening a transition, empty when closing it or in steady state.
	Previous []LayoutNode
}

// SetEncryptionPosture sets the cluster's encryption-at-rest posture
// (ADR-0021): the algorithm new writes use. It is enable-only — apply
// refuses a move from an encrypting algorithm back to EncNone, so a cluster
// that has started encrypting never silently stops. Setting the same
// algorithm again is idempotent. Only the posture is replicated; the KEK is
// never part of this.
//
// KEKFingerprint establishes the cluster's current KEK fingerprint (ADR-0032)
// when enabling: the leader supplies its loaded KEK's fingerprint, so the
// posture knows which key new writes wrap under. Apply sets it if unset (the
// fresh-enable and the upgraded-v0.7 lazy-establishment cases) and otherwise
// requires it to match — a mismatch (a node holding the wrong master key)
// is refused. Zero leaves the current fingerprint untouched.
type SetEncryptionPosture struct {
	ProposedAtUnixMS int64
	Algorithm        EncAlgorithm
	KEKFingerprint   uint64
}

// BeginKEKRotation opens a master-key rotation (ADR-0032): it records the new
// KEK fingerprint the rewrap sweep is moving every version to. Apply requires
// the cluster to be encrypting, the current fingerprint to be established and
// to match From (a stale-leader guard), the target to differ from it, and no
// rotation already open (one at a time) — re-proposing the same target is
// idempotent. The sweep then rewraps each version (RewrapDEK) and closes the
// rotation (CompleteKEKRotation) once none remain on the old key.
type BeginKEKRotation struct {
	ProposedAtUnixMS int64
	FromFingerprint  uint64
	ToFingerprint    uint64
}

// RewrapDEK rewrites one version's wrapped DEK under a new KEK (ADR-0032): the
// leader unwraps the DEK under the old KEK and rewraps it under the new one,
// and this commits the result. Only WrappedDEK and KEKFingerprint change — the
// object's bytes, shards, checksums, and object-lock fields are untouched, so
// it is COMPLIANCE-safe (it can run on a locked version). Idempotent: rewrapping
// a version already on the new fingerprint is a no-op success.
type RewrapDEK struct {
	ProposedAtUnixMS int64
	Bucket           string
	Key              string
	VersionID        VersionID
	WrappedDEK       []byte
	KEKFingerprint   uint64
}

// CompleteKEKRotation closes an open rotation (ADR-0032): it advances the
// posture's current fingerprint to the rotated-to key and clears the
// rotating-to marker. The leader proposes it only after a sweep finds no
// version left on the old fingerprint — apply trusts that determination (like
// closing a layout transition) and only guards that ToFingerprint matches the
// open rotation. Idempotent: completing an already-closed rotation whose
// current equals ToFingerprint is a no-op success.
type CompleteKEKRotation struct {
	ProposedAtUnixMS int64
	ToFingerprint    uint64
}
