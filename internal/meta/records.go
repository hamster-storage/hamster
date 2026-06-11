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
}

// clone returns a copy sharing no mutable state with the original.
func (e VersionEntry) clone() VersionEntry {
	e.ETag = slices.Clone(e.ETag)
	e.ObjectChecksum = slices.Clone(e.ObjectChecksum)
	e.UserMetadata = maps.Clone(e.UserMetadata)
	if e.ShardChecksums != nil {
		sc := make([][]byte, len(e.ShardChecksums))
		for i, s := range e.ShardChecksums {
			sc[i] = slices.Clone(s)
		}
		e.ShardChecksums = sc
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
}

func currentRecordFor(e VersionEntry) CurrentRecord {
	return CurrentRecord{
		FormatVersion: currentFormatVersion,
		VersionID:     e.VersionID,
		Size:          e.Size,
		ETag:          slices.Clone(e.ETag),
		CreatedUnixMS: e.CreatedUnixMS,
	}
}

// BucketConfig is one row under b/.
type BucketConfig struct {
	FormatVersion     uint32
	Name              string
	CreatedUnixMS     int64
	Versioning        VersioningState
	ObjectLockEnabled bool
}
