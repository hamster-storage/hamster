// Package meta implements Hamster's metadata model: the version-list
// keyspace and the deterministic apply logic designed in docs/METADATA.md
// and ADR-0014.
//
// The Store is a deterministic state machine. Every mutation enters as a
// proposal that carries all of its inputs — the proposing node's time and
// the freshly minted version ID included — so apply computes bit-identical
// state on every replica and every replay. The package owns no clock, no
// randomness, and no I/O; it imports nothing of the seam. It is built to
// sit behind Raft and inside the simulation harness unchanged.
//
// The keyspace lives in an in-memory sorted KV: encoded keys (settled
// design, ADR-0014) mapping to typed records. Durability is the Persister
// seam (persist.go): each apply commits its encoded row changes (codec.go,
// ADR-0023) as one atomic transaction — BadgerDB in production, fakes in
// tests — before the mutation becomes visible, and rolls back if the commit
// fails. A Store must be owned by a single event loop (seam.Loop); it does
// no locking of its own.
package meta

import "errors"

// Errors returned by apply. These are deterministic outcomes — every
// replica rejects the same proposal the same way — and they map onto S3
// error codes at the API layer.
var (
	ErrInvalidBucketName      = errors.New("invalid bucket name")
	ErrBucketExists           = errors.New("bucket already exists")
	ErrNoSuchBucket           = errors.New("no such bucket")
	ErrBucketNotEmpty         = errors.New("bucket not empty")
	ErrInvalidObjectKey       = errors.New("invalid object key")
	ErrNoSuchVersion          = errors.New("no such version")
	ErrObjectLocked           = errors.New("version is protected by object lock")
	ErrInvalidVersioningState = errors.New("invalid versioning state change")
	ErrInvalidRetention       = errors.New("invalid retention")
	ErrNoSuchUpload           = errors.New("no such multipart upload")
	ErrUploadExists           = errors.New("multipart upload ID already exists")
	ErrInvalidPartNumber      = errors.New("part number out of range")
	ErrInvalidPart            = errors.New("part not found or ETag mismatch")
	ErrInvalidPartOrder       = errors.New("part list is not in ascending order")
	ErrPartTooSmall           = errors.New("part below the minimum size")
)

// Multipart limits, S3 parity (docs/S3-API.md). Apply enforces both: the
// minimum applies to every completed part except the last.
const (
	MinPartSize   = 5 << 20
	MaxPartNumber = 10_000
)

// Store holds one replica's metadata state.
type Store struct {
	kv      *txKV
	persist Persister
}

// NewStore returns an empty metadata store. Without a Persister it is
// purely in-memory — what the simulation harness and the reference-model
// tests run against.
func NewStore() *Store {
	return &Store{kv: newTxKV()}
}
