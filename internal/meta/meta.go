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
// The keyspace lives in an in-memory sorted KV with the same shape BadgerDB
// will have: encoded keys (settled design, ADR-0014), typed record values
// (protobuf encoding lands with persistence). A Store must be owned by a
// single event loop (seam.Loop); it does no locking of its own.
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
)

// Store holds one replica's metadata state.
type Store struct {
	kv *memKV
}

// NewStore returns an empty metadata store.
func NewStore() *Store {
	return &Store{kv: newMemKV()}
}
