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
}
