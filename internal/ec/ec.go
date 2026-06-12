// Package ec is the erasure-coding engine (docs/ERASURE-CODING.md): the
// storage profiles ([ADR-0015]), the stripe codec that turns a framed
// object stream into k+m shard files and back ([ADR-0026]), and shard
// reconstruction for repair. Reed-Solomon math is klauspost/reedsolomon
// ([ADR-0013]).
//
// The unit of encoding is the stripe: sliceSize bytes per shard, k slices
// of frame data plus m of parity, encoded as the frame streams through —
// memory stays bounded no matter the object size. The bytes being sharded
// are an opaque framed stream (docs/DATA-STREAM.md); this package never
// sees plaintext structure, which is why repair works without keys.
//
// Encoding is pure computation — deterministic output, no clocks, no
// randomness, I/O only through the readers and writers the caller hands
// in — so it runs under the simulation harness with no seam.
//
// [ADR-0013]: ../../docs/adr/0013-klauspost-reedsolomon.md
// [ADR-0015]: ../../docs/adr/0015-storage-profiles.md
// [ADR-0026]: ../../docs/adr/0026-stripe-and-shard-layout.md
package ec

import "fmt"

// Profile is a named, tested k+m configuration ([ADR-0015]): Data shards
// carry the bytes, Parity shards buy node-loss tolerance. The set is
// deliberately small — a profile is tested durability, not arithmetic —
// and only ever grows.
//
// [ADR-0015]: ../../docs/adr/0015-storage-profiles.md
type Profile struct {
	Name         string
	Data, Parity int
}

// Profiles is the v0 profile set, ordered by the node count it needs.
var Profiles = []Profile{
	{Name: "1+0", Data: 1, Parity: 0},
	{Name: "1+1", Data: 1, Parity: 1},
	{Name: "2+1", Data: 2, Parity: 1},
	{Name: "3+2", Data: 3, Parity: 2},
	{Name: "4+2", Data: 4, Parity: 2},
}

// AutoProfile is the auto-policy ladder: the profile a cluster of n nodes
// uses when the operator has not pinned one (docs/ERASURE-CODING.md).
func AutoProfile(nodes int) Profile {
	switch {
	case nodes <= 1:
		return Profiles[0] // 1+0
	case nodes == 2:
		return Profiles[1] // 1+1
	case nodes <= 4:
		return Profiles[2] // 2+1
	case nodes == 5:
		return Profiles[3] // 3+2
	default:
		return Profiles[4] // 4+2
	}
}

// ProfileByName resolves a profile by its name, for pinning.
func ProfileByName(name string) (Profile, error) {
	for _, p := range Profiles {
		if p.Name == name {
			return p, nil
		}
	}
	return Profile{}, fmt.Errorf("ec: unknown profile %q (profiles are a tested set, not free parameters)", name)
}

// SmallObjectThreshold is the size below which objects are stored with
// k=1 — and Reed-Solomon with one data shard is replication, each parity
// shard a full copy. Below this line EC's nominal overhead is fiction
// (filesystem block floors, read fan-out) and replication is both cheaper
// and faster. Recorded implicitly by each object's own parameters, so
// retuning changes new writes only.
const SmallObjectThreshold = 128 << 10

// Nodes is how many nodes the profile needs: one per shard, because the
// failure domain is the node and no node ever holds two shards of one
// object.
func (p Profile) Nodes() int { return p.Data + p.Parity }

// Params is the k+m an object of the given plaintext size is written
// with under this profile: the profile's own parameters, except small
// objects drop to k=1 with the profile's parity (the small-object rule).
func (p Profile) Params(plaintextSize int64) (k, m int) {
	if plaintextSize < SmallObjectThreshold && p.Data > 1 {
		return 1, p.Parity
	}
	return p.Data, p.Parity
}
