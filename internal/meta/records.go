package meta

import (
	"maps"
	"slices"
	"time"
)

// currentFormatVersion stamps every new record, per the additive-formats
// invariant (CLAUDE.md, ADR-0008). The Go structs here mirror the protobuf
// sketches in docs/METADATA.md; the wire/disk encodings land with
// persistence.
const currentFormatVersion = 1

// Kind distinguishes the two things a version entry can be.
type Kind uint8

// Version entry kinds.
const (
	KindObject Kind = iota
	KindDeleteMarker
)

// RetentionMode is the object-lock retention mode (ADR-0006).
type RetentionMode uint8

// Retention modes. COMPLIANCE has no override path, structurally
// (CLAUDE.md invariant 4); GOVERNANCE yields to an authorized bypass.
const (
	RetentionNone RetentionMode = iota
	RetentionGovernance
	RetentionCompliance
)

// EncAlgorithm names how a version's bytes are encrypted at rest
// (ADR-0021). It is recorded per version, so a cluster holds a mix of
// encrypted and plaintext objects and a read always knows which it has —
// the active cluster posture governs only new writes, never what an
// existing object is.
type EncAlgorithm uint8

// Encryption algorithms. EncNone is an unencrypted object (the value of
// every object written before encryption was enabled).
const (
	EncNone EncAlgorithm = iota
	EncAES256GCM
)

// VersioningState is a bucket's versioning configuration. Unversioned is
// the pre-first-enable state; once enabled, a bucket only moves between
// Enabled and Suspended.
type VersioningState uint8

// Versioning states.
const (
	Unversioned VersioningState = iota
	VersioningEnabled
	VersioningSuspended
)

// VersionEntry is one row under v/ — one version of one key, the unit of
// truth (METADATA.md principle 1). Immutable after commit except the lock
// fields, which may only strengthen.
type VersionEntry struct {
	FormatVersion uint32
	VersionID     VersionID

	// DataID names this version's data on the data plane: shards (and the
	// single-node blob) are written under the gateway-minted ID *before*
	// the metadata commit, so if apply bumps VersionID for ordering, the
	// data keeps the name it was durably written under. DataID is that
	// minted ID — an address, not an ordering identity; it is never
	// bumped. Zero for delete markers, which carry no data.
	DataID VersionID

	Kind          Kind
	Size          int64
	CreatedUnixMS int64
	ETag          []byte
	ContentType   string
	UserMetadata  map[string]string

	// Shard addressing: the partition is the location (METADATA.md).
	Partition      uint64
	ECDataShards   uint32
	ECParityShards uint32
	ObjectChecksum []byte
	ShardChecksums [][]byte

	// Object lock (ADR-0006).
	RetentionMode     RetentionMode
	RetainUntilUnixMS int64
	LegalHold         bool

	// Encryption at rest (ADR-0021). When EncAlgorithm is not EncNone, the
	// object's bytes are encrypted and WrappedDEK holds the per-object data
	// key wrapped under the cluster KEK — one metadata read yields the
	// wrapped key a GET needs. The shards and ShardChecksums are ciphertext,
	// so repair, scrub, rebalance, and re-encode never see WrappedDEK.
	EncAlgorithm EncAlgorithm
	WrappedDEK   []byte

	// KEKFingerprint identifies the KEK that wrapped WrappedDEK (ADR-0032):
	// the keys-package content fingerprint as a big-endian integer. Master-key
	// rotation rewraps WrappedDEK under a new KEK and restamps this; the count
	// of versions still on the old fingerprint is the rotation's provable
	// progress. Zero means "none recorded" — a version wrapped under the
	// cluster's founding KEK, before any rotation. Additive (invariant 2),
	// meaningful only when EncAlgorithm is not EncNone.
	KEKFingerprint uint64

	// NullVersion marks the entry the API renders as version "null":
	// writes to unversioned and suspended-versioning buckets. At most one
	// per key.
	NullVersion bool

	// Parts is the multipart assembly, in part order: a completed multipart
	// upload's data stays where the parts were written, and a read
	// concatenates them. Empty for whole PUTs (DataID is the one address)
	// and delete markers. When set, DataID is zero, ETag holds the raw
	// composite MD5 (ADR-0019: hex plus "-N" on the wire), and
	// ObjectChecksum is empty — integrity is per part.
	Parts []PartRef

	// unknown holds raw bytes of fields a newer writer added that this
	// version of the code does not know. The codec preserves them across a
	// rewrite (ADR-0008: old code never sheds new fields). Same on every
	// record type below.
	unknown []byte
}

// PartRef is one slice of a multipart object's data: the address it was
// written under and the facts a read needs to fetch and verify it.
type PartRef struct {
	DataID   VersionID
	Size     int64
	Checksum []byte // SHA-256 of the part's bytes

	unknown []byte
}

// DataIDs returns every data-plane address the entry's bytes live at:
// nothing for a delete marker, the single DataID for a whole PUT, the
// per-part addresses for a multipart object. This is what reclaim and GC
// must walk — forgetting a part would leak its blob.
func (e VersionEntry) DataIDs() []VersionID {
	if len(e.Parts) > 0 {
		ids := make([]VersionID, len(e.Parts))
		for i, p := range e.Parts {
			ids[i] = p.DataID
		}
		return ids
	}
	if e.DataID.IsZero() {
		return nil
	}
	return []VersionID{e.DataID}
}

// clone returns a copy sharing no mutable state with the original.
func (e VersionEntry) clone() VersionEntry {
	e.ETag = slices.Clone(e.ETag)
	e.ObjectChecksum = slices.Clone(e.ObjectChecksum)
	e.WrappedDEK = slices.Clone(e.WrappedDEK)
	e.UserMetadata = maps.Clone(e.UserMetadata)
	e.unknown = slices.Clone(e.unknown)
	if e.ShardChecksums != nil {
		sc := make([][]byte, len(e.ShardChecksums))
		for i, s := range e.ShardChecksums {
			sc[i] = slices.Clone(s)
		}
		e.ShardChecksums = sc
	}
	if e.Parts != nil {
		ps := make([]PartRef, len(e.Parts))
		for i, p := range e.Parts {
			p.Checksum = slices.Clone(p.Checksum)
			ps[i] = p
		}
		e.Parts = ps
	}
	return e
}

// lockedAt reports whether object lock forbids destroying this entry at
// proposal time atUnixMS. Legal holds and unexpired COMPLIANCE retention
// admit no bypass — there is deliberately no parameter that overrides
// them. Unexpired GOVERNANCE retention yields to an authorized bypass.
func (e VersionEntry) lockedAt(atUnixMS int64, bypassGovernance bool) bool {
	if e.LegalHold {
		return true
	}
	if e.RetainUntilUnixMS <= atUnixMS {
		return false
	}
	switch e.RetentionMode {
	case RetentionCompliance:
		return true
	case RetentionGovernance:
		return !bypassGovernance
	}
	return false
}

// retainUntilFromDefault computes an object's absolute retain-until from a
// bucket default rule's duration and the write's proposal time (ADR-0006). Days
// add as calendar days, years as calendar years (AddDate), matching how S3
// resolves Days/Years defaults. Deterministic: a pure function of its inputs, so
// every replica derives the same date.
func retainUntilFromDefault(atUnixMS int64, days, years uint32) int64 {
	return time.UnixMilli(atUnixMS).UTC().AddDate(int(years), 0, int(days)).UnixMilli()
}

// CurrentRecord is one row under c/ — the derived listing row for a key
// whose newest version is a live object. It denormalizes what a listing
// needs so ListObjects is a pure scan (METADATA.md).
type CurrentRecord struct {
	FormatVersion uint32
	VersionID     VersionID
	Size          int64
	ETag          []byte
	CreatedUnixMS int64
	// PartCount is the multipart part count, zero for whole PUTs: listings
	// must render the composite ETag with its "-N" suffix (ADR-0019).
	PartCount uint32

	unknown []byte
}

func currentRecordFor(e VersionEntry) CurrentRecord {
	return CurrentRecord{
		FormatVersion: currentFormatVersion,
		VersionID:     e.VersionID,
		Size:          e.Size,
		ETag:          slices.Clone(e.ETag),
		CreatedUnixMS: e.CreatedUnixMS,
		PartCount:     uint32(len(e.Parts)),
	}
}

// BucketConfig is one row under b/.
type BucketConfig struct {
	FormatVersion     uint32
	Name              string
	CreatedUnixMS     int64
	Versioning        VersioningState
	ObjectLockEnabled bool

	// DefaultRetention is the bucket's object-lock default retention rule
	// (ADR-0006), set by PutObjectLockConfiguration and applied to new objects
	// that arrive without their own retention. The duration is kept in the S3
	// shape — days or years, never an absolute date — so GetObjectLockConfiguration
	// round-trips what the operator set; the absolute retain-until is computed per
	// object at PUT time. Mode RetentionNone means no default; at most one of Days
	// or Years is non-zero.
	DefaultRetentionMode  RetentionMode
	DefaultRetentionDays  uint32
	DefaultRetentionYears uint32

	unknown []byte
}

// UploadRecord is one row under u/ — an in-progress multipart upload. It
// carries what CompleteMultipartUpload needs to build the version entry;
// the parts accumulate as sibling PartRecord rows under the same upload ID.
type UploadRecord struct {
	FormatVersion uint32
	UploadID      VersionID
	CreatedUnixMS int64
	ContentType   string
	UserMetadata  map[string]string

	unknown []byte
}

// PartRecord is one uploaded part of an in-progress multipart upload. Its
// data is already durable under DataID when the row commits — the same
// write-then-commit order as PutObject. Re-uploading a part number
// replaces the row; the displaced blob is the caller's to reclaim.
type PartRecord struct {
	FormatVersion  uint32
	PartNumber     uint32
	DataID         VersionID
	Size           int64
	ETag           []byte // MD5, matched against CompleteMultipartUpload's part list
	Checksum       []byte // SHA-256 of the part's bytes
	UploadedUnixMS int64

	unknown []byte
}

// LayoutNode is one member of a ClusterLayout with its failure-domain
// labels (ADR-0016): the node ID, its host (machine identity — processes on
// one box share it), and its zone (the domain above the machine, an AZ or a
// rack, defaulting to the host). Placement spreads shards across zones, then
// hosts, then nodes. Strings, so meta stays free of the seam.
//
// Weight is the node's relative capacity (ADR-0004): a higher-weight node
// holds proportionally more partitions within the spread. Zero means equal,
// so a layout written before this field existed reads as an unweighted (equal)
// cluster — the field is additive (invariant 2) and encoded only when nonzero.
// Draining marks a node the operator is removing (ADR-0004): placement demotes
// it below every active node, so new writes avoid it while existing shards stay
// readable (reconstructed from survivors) until repair migrates them off.
// Additive (invariant 2), encoded only when true.
type LayoutNode struct {
	ID       string
	Host     string
	Zone     string
	Weight   uint32
	Draining bool
}

// NodeRecord is one row under s/node/<id> — a cluster member's replicated
// registration: its failure-domain labels (ADR-0016) and capacity weight
// (ADR-0004), committed through Raft. It is the registry the layout reconcile
// composes from. Before this record the registry lived only on the join
// issuer's local disk, so only that node could build a complete labeled
// layout; replicating it lets any leader compose one. The issuer proposes a
// member's record as part of admission; the row then persists like any
// committed state.
//
// Strings, so meta stays free of the seam. Idempotent by node ID — a
// re-registration with changed labels replaces the row.
type NodeRecord struct {
	FormatVersion uint32
	NodeID        string
	Host          string
	Zone          string
	Capacity      uint32
	// Draining is operator-set (ADR-0004): the node is being removed, so
	// placement steers new writes away from it and repair migrates its shards
	// off. Additive (invariant 2), encoded only when true.
	Draining bool

	// ReplacedBy names the node taking this one's place (ADR-0004): set when an
	// operator replaces this node with a fresh one. Once the replacement is a
	// cluster member, placement drops this node entirely (not merely demoted as
	// Draining does) and the same-size swap keeps the storage profile unchanged —
	// repair migrates this node's shards to its replacement, then it is evicted.
	// Additive (invariant 2), encoded only when set.
	ReplacedBy string

	// LeafCAFingerprint names the CA that signed this member's current node
	// certificate (ADR-0033): the certs-package CA fingerprint. A CA rotation
	// reissues every member from the new CA and restamps this; the count of
	// members still on the old CA is the rotation's provable progress (the CA
	// analogue of the per-version KEK fingerprint). Zero means none recorded —
	// a member admitted before CA fingerprints existed. Additive (invariant 2).
	LeafCAFingerprint uint64

	// BinaryVersion is the member's release string for display (ADR-0034), e.g.
	// "v0.11.0" or "v0.11.0-rc.1". The leader's version monitor keeps it current
	// across an in-place upgrade (SetNodeVersion). Empty means none recorded.
	// Additive (invariant 2).
	BinaryVersion string

	// Generation is the member's declared protocol generation (ADR-0034): the
	// monotonic integer the binary owns, advanced only by a coordinated format
	// change. The cluster's effective generation is the minimum across live
	// members, etcd-style. Zero means none recorded (treated as "behind" so the
	// effective generation never claims a roll the member has not confirmed).
	// Additive (invariant 2).
	Generation uint32

	unknown []byte
}

// ClusterLayout is the singleton row under s/layout — the replicated,
// versioned placement basis (ADR-0004, ADR-0028). It names the ordered
// member set that the placement function ranks over, so every node and
// every restart computes the same partition→node assignment from a
// committed fact rather than from whoever happens to be in the cluster at
// the moment a read or write arrives.
//
// Version is a monotonic generation: a new layout is installed by proposing
// Version = prior + 1 (the first install is Version 1), which makes layout
// changes a compare-and-set and gives a future rebalance (v0.4) the old→new
// pair it needs to track a migration. PartitionCount is the cluster
// constant (ADR-0004: fixed at creation, never resized) carried so the
// record is self-contained.
//
// Nodes (field 5, ADR-0016) is the labeled member set placement spreads
// over. Members (field 4) is the original unlabeled form from v0.4 pass 1,
// kept for decode of older data; EffectiveNodes resolves whichever is
// present. New writers populate Nodes.
//
// Previous (field 6, ADR-0004) is the member set the layout is migrating away
// from while a rebalance is in flight: empty in steady state, set to the prior
// Nodes when a transition opens. Because shard addressing is positional and
// derived from the member set, a change to Nodes relocates shards; carrying the
// old set lets reads dual-read (place.Locate) and repair migrate shards
// old→new. The transition closes by installing a layout with Previous empty.
type ClusterLayout struct {
	FormatVersion  uint32
	Version        uint64
	PartitionCount uint32
	Members        []string
	Nodes          []LayoutNode
	Previous       []LayoutNode

	unknown []byte
}

// EncryptionPosture is the singleton row under s/enc — the cluster's
// replicated encryption-at-rest posture (ADR-0021). Algorithm is EncNone
// when the cluster does not encrypt (the default, and the absence of the
// row) or EncAES256GCM when it does. The posture governs only new writes;
// each version records its own algorithm, so existing objects are
// unaffected by a change. It is enable-only: ApplySetEncryptionPosture
// refuses a move back to EncNone (a downgrade), so a cluster never silently
// stops encrypting. The KEK is never part of this row — only the posture.
//
// CurrentKEKFingerprint and RotatingToKEKFingerprint track master-key
// rotation (ADR-0032), both keys-package content fingerprints as big-endian
// integers. CurrentKEKFingerprint is the KEK new writes wrap under — a node
// whose loaded KEK does not match refuses encrypted writes (the split-key
// guard). RotatingToKEKFingerprint is non-zero only while a rotation is open:
// it names the new KEK the rewrap sweep is moving every version to; the
// rotation closes (and this returns to zero, with Current advanced) once no
// version remains on the old fingerprint. Both additive (invariant 2), zero
// on a posture written before rotation existed — established lazily then.
type EncryptionPosture struct {
	FormatVersion            uint32
	Algorithm                EncAlgorithm
	CurrentKEKFingerprint    uint64
	RotatingToKEKFingerprint uint64

	unknown []byte
}

// TrustedCA is one CA certificate in the cluster's trust bundle (ADR-0033):
// its content fingerprint (the certs-package CA fingerprint) and its PEM. Only
// public certificate material — the CA private key is never replicated
// (ADR-0029).
type TrustedCA struct {
	Fingerprint uint64
	CertPEM     []byte
}

// TrustBundle is the singleton row under s/trust — the replicated set of CA
// certificates every node trusts for inter-node mTLS (ADR-0033, ADR-0022),
// plus which CA new node certificates are issued under. Generational like the
// cluster layout (ADR-0028): installed compare-and-set on Version. During a CA
// rotation the bundle holds both the old and new CA at once (dual trust);
// IssuerFingerprint names the CA leaves are now signed by, and the rotation
// closes by installing a generation that drops the retired CA. Only
// certificates are replicated; the CA private key never is (ADR-0029).
type TrustBundle struct {
	FormatVersion     uint32
	Version           uint64
	CAs               []TrustedCA
	IssuerFingerprint uint64

	unknown []byte
}

// CertPEMs returns every trusted CA certificate's PEM, for building a node's
// mTLS trust pool (certs.PoolFromCAs).
func (b TrustBundle) CertPEMs() [][]byte {
	out := make([][]byte, len(b.CAs))
	for i, c := range b.CAs {
		out[i] = c.CertPEM
	}
	return out
}

// HasCA reports whether the bundle trusts the CA with the given fingerprint.
func (b TrustBundle) HasCA(fingerprint uint64) bool {
	for _, c := range b.CAs {
		if c.Fingerprint == fingerprint {
			return true
		}
	}
	return false
}

// EffectiveNodes returns the layout's labeled member set: Nodes when present
// (current writers), else the legacy Members IDs with host and zone
// defaulted to the ID — so a pass-1 layout (or any unlabeled one) reads as a
// cluster where every node is its own host and zone, which spreads exactly
// as the bare rendezvous ranking did.
func (l ClusterLayout) EffectiveNodes() []LayoutNode {
	if len(l.Nodes) > 0 {
		return l.Nodes
	}
	out := make([]LayoutNode, len(l.Members))
	for i, id := range l.Members {
		out[i] = LayoutNode{ID: id, Host: id, Zone: id}
	}
	return out
}
