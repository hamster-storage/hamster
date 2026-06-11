package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"time"
)

// VersionID is a 16-byte object version identifier (ADR-0007). New IDs are
// UUIDv7, so they sort by creation time; after an apply-time monotonicity
// bump (Next) an ID may no longer decode as a valid UUIDv7 timestamp.
// Version IDs are opaque, ordered 128-bit values that start life as
// UUIDv7 — never parse time back out of one (METADATA.md keeps
// created_unix_ms explicit for exactly this reason).
type VersionID [16]byte

// NewVersionID mints a UUIDv7 (RFC 9562) from explicit inputs: the
// caller's clock reading and PRNG, typically the gateway node's
// seam.Clock and World rand. It deliberately reads no ambient time or
// randomness — determinism is a feature (CLAUDE.md), which is why
// google/uuid's NewV7 is not used here (ADR-0007).
func NewVersionID(t time.Time, rng *rand.Rand) VersionID {
	var id VersionID
	ms := uint64(t.UnixMilli())
	id[0] = byte(ms >> 40)
	id[1] = byte(ms >> 32)
	id[2] = byte(ms >> 24)
	id[3] = byte(ms >> 16)
	id[4] = byte(ms >> 8)
	id[5] = byte(ms)
	binary.BigEndian.PutUint16(id[6:8], uint16(rng.Uint64()))
	binary.BigEndian.PutUint64(id[8:16], rng.Uint64())
	id[6] = id[6]&0x0f | 0x70 // version 7
	id[8] = id[8]&0x3f | 0x80 // variant 10
	return id
}

// Compare orders IDs as big-endian 128-bit integers — for UUIDv7 values,
// creation order.
func (v VersionID) Compare(o VersionID) int {
	return bytes.Compare(v[:], o[:])
}

// Next returns the ID incremented as a 128-bit big-endian integer: the
// apply-time monotonicity bump from METADATA.md ("commit order beats clock
// order"). The result always sorts immediately after v.
func (v VersionID) Next() VersionID {
	for i := len(v) - 1; i >= 0; i-- {
		v[i]++
		if v[i] != 0 {
			break
		}
	}
	return v
}

// IsZero reports whether the ID is the all-zero value, which no minted or
// bumped ID ever is.
func (v VersionID) IsZero() bool {
	return v == VersionID{}
}

// String renders the standard UUID text form, for logs and errors.
func (v VersionID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", v[0:4], v[4:6], v[6:8], v[8:10], v[10:16])
}
