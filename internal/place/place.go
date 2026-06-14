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
	"math/bits"
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

// Node is one member of a layout with its failure-domain labels (ADR-0016):
// the host is the machine identity (processes on one box share it), the zone
// an operator label for the domain above the machine (an AZ, a rack),
// defaulting to the host. Placement spreads shards across zones, then hosts,
// then nodes.
//
// Weight is the node's relative capacity (ADR-0004): a node with twice the
// weight holds about twice the partitions, within the failure-domain spread. A
// zero weight means equal (weight 1), so an unweighted cluster — the default,
// and every layout written before this field existed — ranks exactly as it did
// without weighting.
// Draining marks a node the operator is removing (ADR-0004): placement demotes
// it below every active node, so it is chosen only to fill width when too few
// active nodes remain. New writes thus avoid a draining node while its existing
// shards stay readable (any k of the width survive) until repair migrates them.
// A cluster with no draining node ranks exactly as it did before this field.
type Node struct {
	ID       seam.NodeID
	Host     string
	Zone     string
	Weight   uint32
	Draining bool
}

// Layout is a resolved snapshot of the stored cluster layout (ADR-0028):
// the placement basis a single operation reads once, so an object's
// partition and its node ranking are computed from one generation, never
// two. The cluster layer builds it from meta.ClusterLayout; v0.3's
// live-membership getter is gone — placement is a committed fact now.
type Layout struct {
	Version        uint64
	PartitionCount uint32
	Members        []Node
	// Previous, when non-empty, is the member set a rebalance is migrating away
	// from (ADR-0004): the layout is mid-transition. Shard addressing is
	// positional and derived from the member set, so changing the set relocates
	// where a shard lives; Previous lets a reader dual-read every shard from
	// wherever it currently sits (its new home, or its old one if repair has not
	// migrated it yet). Empty in steady state.
	Previous []Node
}

// Nodes returns this layout's placement for the partition at the given
// width: the rendezvous ranking spread across zones, then hosts, then nodes
// (ADR-0016), with the node-distinct floor held by construction. It resolves
// the new (target) member set — where new writes go and shards migrate to.
// See spread.
func (l Layout) Nodes(partition uint64, width int) ([]seam.NodeID, error) {
	return spread(partition, l.Members, width)
}

// Locate resolves a partition's placement for a read during a possible
// transition (ADR-0004): newNodes is the target placement (Members), and
// oldNodes is the prior placement (Previous) when a transition is open, else
// nil. A reader fetches shard i from newNodes[i], falling back to oldNodes[i] —
// so an object written before the transition is found at its old home until
// repair migrates it, and one written after at its new home. In steady state
// (no Previous) this is exactly Nodes with a nil oldNodes.
func (l Layout) Locate(partition uint64, width int) (newNodes, oldNodes []seam.NodeID, err error) {
	newNodes, err = spread(partition, l.Members, width)
	if err != nil || len(l.Previous) == 0 {
		return newNodes, nil, err
	}
	oldNodes, err = spread(partition, l.Previous, width)
	if err != nil {
		return nil, nil, err
	}
	return newNodes, oldNodes, nil
}

// spread selects width nodes for a partition: the rendezvous ranking
// (descending score, tie broken by ID) filled greedily so each pick lands on
// the least-loaded zone, then host, then highest-ranked remaining node. The
// result is a pure function of (partition, member set, width) with three
// properties placement depends on:
//
//   - Node-distinct (invariant 8): each member is pickable once, so no node
//     ever holds two shards of an object — the hard floor, unchanged.
//   - Prefix-stable: pick i depends only on picks 0..i-1, never on width, so
//     a narrow width is a prefix of a wide one (the small-object rule and
//     ranged reads rely on this).
//   - Spread (ADR-0016 objective): shards land on as many distinct zones,
//     then hosts, as the cluster allows. A single-zone, single-host cluster
//     has every load tied, so selection falls back to rank and the result is
//     exactly the bare rendezvous ranking — no behavior change there.
func spread(partition uint64, members []Node, width int) ([]seam.NodeID, error) {
	if width <= 0 {
		return nil, fmt.Errorf("place: width %d is not positive", width)
	}
	if len(members) < width {
		return nil, fmt.Errorf("place: %d shards do not fit on %d nodes (one shard per node, ADR-0016)", width, len(members))
	}

	type ranked struct {
		node   Node
		weight uint64
		negln  uint64 // -ln(score/2^64), Q32 fixed point; the weighted key's denominator
		score  uint64 // the bare rendezvous score, the equal-weight tiebreak
	}
	rank := make([]ranked, len(members))
	for i, m := range members {
		w := uint64(m.Weight)
		if w == 0 {
			w = 1
		}
		s := rendezvousScore(partition, m.ID)
		rank[i] = ranked{node: m, weight: w, negln: negLn(s), score: s}
	}
	slices.SortFunc(rank, func(a, b ranked) int {
		// Weighted rendezvous, log method: key = weight / -ln(h), highest
		// wins. cmpKey compares by cross-multiplying in 128-bit integers, so
		// it is exact and identical on every node (no float, which could
		// diverge across the CGO_ENABLED=0 cross-builds a cluster may mix).
		if c := cmpKey(a.weight, a.negln, b.weight, b.negln); c != 0 {
			return c
		}
		// Equal keys fall back to the bare rendezvous score (descending): the
		// fixed-point -ln can tie distinct scores, and for an equal-weight
		// cluster this restores the exact pre-weighting ranking, leaving the
		// pass-2 goldens intact.
		switch {
		case a.score != b.score:
			if a.score > b.score {
				return -1
			}
			return 1
		case a.node.ID < b.node.ID:
			return -1
		case a.node.ID > b.node.ID:
			return 1
		default:
			return 0
		}
	})
	// Equal IDs hash to equal scores, so duplicates sort adjacent.
	for i := 1; i < len(rank); i++ {
		if rank[i].node.ID == rank[i-1].node.ID {
			return nil, fmt.Errorf("place: duplicate member %q", rank[i].node.ID)
		}
	}

	zoneLoad := make(map[string]int, len(rank))
	hostLoad := make(map[string]int, len(rank))
	picked := make([]bool, len(rank))
	out := make([]seam.NodeID, 0, width)
	for len(out) < width {
		// Ascending scan with strict-better replacement makes the rank index
		// the final tiebreaker: among equally-loaded domains, the higher-
		// ranked (smaller index) node wins, preserving the rendezvous spread.
		best := -1
		for i := range rank {
			if picked[i] {
				continue
			}
			if best == -1 {
				best = i
				continue
			}
			// A draining node is demoted below every active one (ADR-0004): an
			// active candidate always beats a draining best, regardless of
			// domain load, so draining nodes fill width only as a last resort.
			// Among nodes of equal drain status the spread is unchanged.
			bd, cd := rank[best].node.Draining, rank[i].node.Draining
			if cd != bd {
				if !cd {
					best = i
				}
				continue
			}
			bz, bh := zoneLoad[rank[best].node.Zone], hostLoad[rank[best].node.Host]
			cz, ch := zoneLoad[rank[i].node.Zone], hostLoad[rank[i].node.Host]
			if cz < bz || (cz == bz && ch < bh) {
				best = i
			}
		}
		n := rank[best].node
		picked[best] = true
		out = append(out, n.ID)
		zoneLoad[n.Zone]++
		hostLoad[n.Host]++
	}
	return out, nil
}

// rendezvousScore is the per-(partition,node) rendezvous weight: FNV-1a 64
// over the partition and the node ID, finalized through the murmur3 mixer so
// every input bit avalanches (see mix). Shared by spread and the bare Nodes.
func rendezvousScore(partition uint64, id seam.NodeID) uint64 {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], partition)
	h := fnv.New64a()
	h.Write(key[:])
	h.Write([]byte(id))
	return mix(h.Sum64())
}

// ln2Q32 is ln(2) in Q32 fixed point: round(ln(2) * 2^32).
const ln2Q32 = 2977044472

// negLn returns -ln(h / 2^64) as a Q32 fixed-point integer (the value times
// 2^32) — the denominator of the weighted-rendezvous key, weight / -ln(h).
// Pure integer arithmetic: the bit length gives the integer part of log2 and
// log2-by-squaring gives the mantissa's fraction, scaled by ln2. No float, so
// it is bit-identical on every platform — placement, which every node must
// agree on, depends on that. Monotonic in h (a larger score gives a smaller
// -ln), so with equal weights the ranking is the bare score order.
func negLn(h uint64) uint64 {
	if h == 0 {
		h = 1
	}
	bl := bits.Len64(h) // 1..64; the integer part of log2(h) is bl-1
	// Normalize the mantissa to Q32, value in [1<<32, 2<<32).
	var mant uint64
	if bl-1 <= 32 {
		mant = h << (32 - (bl - 1))
	} else {
		mant = h >> ((bl - 1) - 32)
	}
	// Fractional bits of log2(mantissa), MSB first, by repeated squaring.
	var frac uint64
	x := mant
	for i := 0; i < 32; i++ {
		hi, lo := bits.Mul64(x, x)
		x = hi<<32 | lo>>32 // (x*x) >> 32, keeping Q32
		frac <<= 1
		if x >= 2<<32 {
			x >>= 1
			frac |= 1
		}
	}
	// -log2(x) = 64 - log2(h) = (65-bl) - frac, in Q32 (always positive: x<1).
	negLog2 := uint64(65-bl)<<32 - frac
	hi, lo := bits.Mul64(negLog2, ln2Q32)
	res := hi<<32 | lo>>32 // * ln2, back to Q32
	if res == 0 {
		res = 1 // keep the key's denominator positive (x≈1 rounds here)
	}
	return res
}

// cmpKey orders two weighted-rendezvous keys for a descending sort: -1 when
// a's key (wa/na) is larger than b's (so a sorts first), +1 when smaller, 0
// when equal. It cross-multiplies — wa*nb vs wb*na, both denominators positive
// — in 128-bit integers, so the comparison is exact and platform-independent.
func cmpKey(wa, na, wb, nb uint64) int {
	hiA, loA := bits.Mul64(wa, nb)
	hiB, loB := bits.Mul64(wb, na)
	switch {
	case hiA != hiB:
		if hiA > hiB {
			return -1
		}
		return 1
	case loA != loB:
		if loA > loB {
			return -1
		}
		return 1
	default:
		return 0
	}
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
	for i, m := range members {
		rank[i] = ranked{id: m, score: rendezvousScore(partition, m)}
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
