package meta

import (
	"maps"
	"slices"
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
// record is self-contained. Members are raw node-ID strings so this package
// imports nothing of the seam; the cluster layer maps them to seam.NodeID.
type ClusterLayout struct {
	FormatVersion  uint32
	Version        uint64
	PartitionCount uint32
	Members        []string

	unknown []byte
}
