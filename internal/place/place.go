// Package place is the v0.3 placement function ([ADR-0027]): which
// partition an object belongs to, and which nodes hold a partition's
// shards. Placement is derived — a pure function of the data ID and the
// member set — until the stored, versioned cluster layout arrives with
// rebalance in v0.4 ([ADR-0004]).
//
// Both mappings are permanent in effect but not in consequence: every
// object records its partition and EC parameters in its own VersionEntry,
// so changing either algorithm affects new writes only.
//
// Pure computation — no clocks, no randomness, no I/O — so it runs under
// the simulation harness with no seam.
//
// [ADR-0004]: ../../docs/adr/0004-partitioned-placement.md
// [ADR-0027]: ../../docs/adr/0027-v03-distributed-data-path.md
package place

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"slices"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
)

// DefaultPartitionCount is the partition count a new cluster records in
// its ClusterConfig. Fixed at creation and never resized (ADR-0004):
// generous overprovisioning is the one-time decision that bounds maximum
// useful cluster size, and empty partitions cost almost nothing.
const DefaultPartitionCount = 4096

// mix is murmur3's 64-bit finalizer. FNV-1a alone must not be used for
// scores or bucketing: it has no avalanche — inputs differing in one
// trailing byte hash exactly one FNV-prime apart, which turns a rendezvous
// "ranking" into the input order. The finalizer spreads every input bit
// across the output. Pinned by golden test, like every persistent choice.
func mix(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Partition maps a data ID to its partition: mixed FNV-1a 64 over the 16
// ID bytes, modulo count. Recorded in the object's VersionEntry at commit —
// the stored value is the location, this function only mints it.
func Partition(id meta.VersionID, count uint32) uint64 {
	if count == 0 {
		panic("place: partition count is zero")
	}
	h := fnv.New64a()
	h.Write(id[:])
	return mix(h.Sum64()) % uint64(count)
}

// Layout is a resolved snapshot of the stored cluster layout (ADR-0028):
// the placement basis a single operation reads once, so an object's
// partition and its node ranking are computed from one generation, never
// two. The cluster layer builds it from meta.ClusterLayout (mapping the
// stored node-ID strings to seam.NodeID); v0.3's live-membership getter is
// gone — placement is a committed fact now.
type Layout struct {
	Version        uint64
	PartitionCount uint32
	Members        []seam.NodeID
}

// Nodes returns the first width nodes of this layout's ranking for the
// partition — the same rendezvous ranking as the package-level Nodes,
// sourced from a committed layout's member set.
func (l Layout) Nodes(partition uint64, width int) ([]seam.NodeID, error) {
	return Nodes(partition, l.Members, width)
}

// Nodes returns the first width nodes of the partition's rendezvous
// ranking: members ordered by descending FNV-1a 64 of (partition, member),
// ties broken by member ID. A ranking of distinct members is a
// permutation, so no node appears twice — the node-distinct invariant
// holds by construction. Shard i of every object in the partition lives
// on the i-th returned node; an object written narrower than the active
// profile (the small-object rule) uses a prefix of the same ranking.
//
// The result is deterministic in the *set* of members: callers may pass
// them in any order. Duplicate member IDs are a caller bug and are
// refused, because a duplicate would silently void the invariant.
func Nodes(partition uint64, members []seam.NodeID, width int) ([]seam.NodeID, error) {
	if width <= 0 {
		return nil, fmt.Errorf("place: width %d is not positive", width)
	}
	if len(members) < width {
		return nil, fmt.Errorf("place: %d shards do not fit on %d nodes (one shard per node, ADR-0016)", width, len(members))
	}

	type ranked struct {
		id    seam.NodeID
		score uint64
	}
	rank := make([]ranked, len(members))
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], partition)
	for i, m := range members {
		h := fnv.New64a()
		h.Write(key[:])
		h.Write([]byte(m))
		rank[i] = ranked{id: m, score: mix(h.Sum64())}
	}
	slices.SortFunc(rank, func(a, b ranked) int {
		switch {
		case a.score != b.score:
			if a.score > b.score {
				return -1
			}
			return 1
		case a.id < b.id:
			return -1
		case a.id > b.id:
			return 1
		default:
			return 0
		}
	})
	// Equal IDs hash to equal scores, so duplicates sort adjacent.
	for i := 1; i < len(rank); i++ {
		if rank[i].id == rank[i-1].id {
			return nil, fmt.Errorf("place: duplicate member %q", rank[i].id)
		}
	}

	out := make([]seam.NodeID, width)
	for i := range out {
		out[i] = rank[i].id
	}
	return out, nil
}
