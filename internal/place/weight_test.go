package place

import (
	"fmt"
	"slices"
	"testing"

	"github.com/hamster-storage/hamster/internal/seam"
)

// weightedFlat builds a single-domain layout (one zone, one host) with the
// given per-node weights, so spread reduces to the pure weighted rendezvous
// ranking — the clean setting to measure the weighting itself.
func weightedFlat(weights ...uint32) Layout {
	nodes := make([]Node, len(weights))
	for i, w := range weights {
		id := seam.NodeID(fmt.Sprintf("n%d", i))
		nodes[i] = Node{ID: id, Host: "box", Zone: "box", Weight: w}
	}
	return layoutOf(nodes)
}

// TestWeightedGolden pins the exact weighted ranking for a fixed heterogeneous
// set — locks the whole fixed-point pipeline like every placement choice.
func TestWeightedGolden(t *testing.T) {
	l := weightedFlat(1, 2, 3, 1, 2, 3) // n0..n5
	got, err := l.Nodes(100, 6)
	if err != nil {
		t.Fatal(err)
	}
	want := []seam.NodeID{"n1", "n2", "n5", "n4", "n3", "n0"}
	if !slices.Equal(got, want) {
		t.Fatalf("weighted Nodes(100, 6) = %v, want %v", got, want)
	}
}

// TestWeightedEqualCollapsesToUnweighted: weight 0 (the default) and any
// uniform weight place identically — and identically to no weighting at all,
// so the pass-2 behavior and every pre-weighting layout are unchanged.
func TestWeightedEqualCollapsesToUnweighted(t *testing.T) {
	ids := testMembers(8)
	base := make([]Node, len(ids)) // weight 0
	uni := make([]Node, len(ids))  // uniform weight 7
	for i, id := range ids {
		base[i] = Node{ID: id, Host: "box", Zone: "box"}
		uni[i] = Node{ID: id, Host: "box", Zone: "box", Weight: 7}
	}
	lb, lu := layoutOf(base), layoutOf(uni)
	for p := uint64(0); p < 512; p++ {
		bare, err := Nodes(p, ids, 6)
		if err != nil {
			t.Fatal(err)
		}
		gb, err := lb.Nodes(p, 6)
		if err != nil {
			t.Fatal(err)
		}
		gu, err := lu.Nodes(p, 6)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(gb, bare) {
			t.Fatalf("partition %d: weight-0 spread %v != bare ranking %v", p, gb, bare)
		}
		if !slices.Equal(gu, bare) {
			t.Fatalf("partition %d: uniform-weight spread %v != bare ranking %v", p, gu, bare)
		}
	}
}

// TestWeightedProportional: in a single domain (pure weighted ranking), each
// node wins first place on a share of partitions proportional to its weight —
// the log method's guarantee, measured over the partition space.
func TestWeightedProportional(t *testing.T) {
	l := weightedFlat(1, 1, 1, 3) // total weight 6; n3 should win ~half
	const parts = 4096
	wins := map[seam.NodeID]int{}
	for p := uint64(0); p < parts; p++ {
		got, err := l.Nodes(p, 1)
		if err != nil {
			t.Fatal(err)
		}
		wins[got[0]]++
	}
	check := func(id seam.NodeID, w int) {
		t.Helper()
		want := float64(parts) * float64(w) / 6.0
		got := float64(wins[id])
		if got < 0.85*want || got > 1.15*want {
			t.Fatalf("node %s (weight %d) won %d first picks, want ~%.0f (±15%%)", id, w, wins[id], want)
		}
	}
	check("n3", 3)
	check("n0", 1)
	check("n1", 1)
	check("n2", 1)
}

// TestWeightedNodeDistinctAndPrefixStable: the hard floor (invariant 8) and
// the prefix property still hold once weights enter the ranking.
func TestWeightedNodeDistinctAndPrefixStable(t *testing.T) {
	nodes := zoned(4, 3) // 12 nodes, 4 zones
	for i := range nodes {
		nodes[i].Weight = uint32(1 + i%3) // weights 1,2,3,1,2,3,...
	}
	l := layoutOf(nodes)
	for p := uint64(0); p < 256; p++ {
		full, err := l.Nodes(p, 6)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[seam.NodeID]bool{}
		for _, id := range full {
			if seen[id] {
				t.Fatalf("partition %d: %q placed twice in %v", p, id, full)
			}
			seen[id] = true
		}
		for w := 1; w < 6; w++ {
			narrow, err := l.Nodes(p, w)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(narrow, full[:w]) {
				t.Fatalf("partition %d width %d: %v not a prefix of %v", p, w, narrow, full)
			}
		}
	}
}

// TestWeightedMinimalDisruption: in a single domain, raising one node's weight
// moves at most one node in each partition's width set — the property that
// lets a weight change ride EC reconstruction and the repair sweep instead of
// waiting for the dedicated rebalance pass (ADR-0004).
func TestWeightedMinimalDisruption(t *testing.T) {
	before := weightedFlat(1, 1, 1, 1, 1, 1, 1, 1)
	after := weightedFlat(1, 1, 1, 3, 1, 1, 1, 1) // n3: 1 -> 3
	const width = 6
	for p := uint64(0); p < 2048; p++ {
		b, err := before.Nodes(p, width)
		if err != nil {
			t.Fatal(err)
		}
		a, err := after.Nodes(p, width)
		if err != nil {
			t.Fatal(err)
		}
		bset := map[seam.NodeID]bool{}
		for _, id := range b {
			bset[id] = true
		}
		moved := 0
		for _, id := range a {
			if !bset[id] {
				moved++
			}
		}
		if moved > 1 {
			t.Fatalf("partition %d: a single weight change moved %d nodes (%v -> %v), want <=1", p, moved, b, a)
		}
	}
}

// TestNegLnMonotonic: -ln(h/2^64) is non-increasing as the rendezvous score h
// grows, so with equal weights the weighted ranking is the bare score order.
// Pure integer math, so this is also the determinism guard against any float
// creeping back into the weighting.
func TestNegLnMonotonic(t *testing.T) {
	hs := []uint64{1, 2, 100, 1 << 20, 1 << 40, 1 << 62, 1 << 63, (1 << 64) - 1}
	prev := negLn(hs[0])
	for _, h := range hs[1:] {
		got := negLn(h)
		if got > prev {
			t.Fatalf("negLn(%d)=%d > previous %d — not monotonic", h, got, prev)
		}
		prev = got
	}
	if negLn(0) != negLn(1) {
		t.Fatalf("negLn(0)=%d, want it equal to negLn(1)=%d", negLn(0), negLn(1))
	}
}
