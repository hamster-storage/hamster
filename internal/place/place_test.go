package place

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
)

func testMembers(n int) []seam.NodeID {
	ms := make([]seam.NodeID, n)
	for i := range ms {
		ms[i] = seam.NodeID(fmt.Sprintf("node-%02d", i))
	}
	return ms
}

// TestGoldenPlacement pins the hash choices. Partition values are recorded
// in committed VersionEntries and rankings locate shards already on disk:
// a change here strands data, so these constants may never drift.
func TestGoldenPlacement(t *testing.T) {
	id := meta.VersionID{0xDA, 0x7A, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
	if got, want := Partition(id, DefaultPartitionCount), uint64(3870); got != want {
		t.Errorf("Partition = %d, want %d", got, want)
	}
	if got, want := Partition(meta.VersionID{}, DefaultPartitionCount), uint64(2058); got != want {
		t.Errorf("Partition(zero ID) = %d, want %d", got, want)
	}

	nodes, err := Nodes(3683, testMembers(6), 6)
	if err != nil {
		t.Fatal(err)
	}
	want := []seam.NodeID{"node-01", "node-02", "node-00", "node-03", "node-04", "node-05"}
	if !slices.Equal(nodes, want) {
		t.Errorf("Nodes = %v, want %v", nodes, want)
	}
}

// TestDeterministicInMemberSet proves the ranking ignores caller order.
func TestDeterministicInMemberSet(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x9A57E4, 1))
	members := testMembers(9)
	want, err := Nodes(7, members, 5)
	if err != nil {
		t.Fatal(err)
	}
	for range 50 {
		shuffled := slices.Clone(members)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		got, err := Nodes(7, shuffled, 5)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("order-dependent placement: %v vs %v", got, want)
		}
	}
}

// TestNodeDistinct sweeps every partition at several cluster sizes and
// demands the hard invariant: no node twice in one assignment.
func TestNodeDistinct(t *testing.T) {
	for _, n := range []int{2, 3, 6, 15} {
		members := testMembers(n)
		width := min(n, 6)
		for p := uint64(0); p < DefaultPartitionCount; p++ {
			nodes, err := Nodes(p, members, width)
			if err != nil {
				t.Fatal(err)
			}
			seen := map[seam.NodeID]bool{}
			for _, id := range nodes {
				if seen[id] {
					t.Fatalf("partition %d on %d nodes: %q placed twice in %v", p, n, id, nodes)
				}
				seen[id] = true
			}
		}
	}
}

// TestPrefixStability: a narrower width is a prefix of a wider one — the
// small-object rule shares placement with full-width siblings.
func TestPrefixStability(t *testing.T) {
	members := testMembers(8)
	for p := uint64(0); p < 256; p++ {
		full, err := Nodes(p, members, 6)
		if err != nil {
			t.Fatal(err)
		}
		for w := 1; w < 6; w++ {
			narrow, err := Nodes(p, members, w)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(narrow, full[:w]) {
				t.Fatalf("partition %d width %d: %v is not a prefix of %v", p, w, narrow, full)
			}
		}
	}
}

// TestBalance: shard slots spread roughly evenly across nodes. Rendezvous
// hashing is uniform in expectation; the bound is loose on purpose — the
// test catches a broken hash, not statistical noise.
func TestBalance(t *testing.T) {
	members := testMembers(10)
	counts := map[seam.NodeID]int{}
	const width = 6
	for p := uint64(0); p < DefaultPartitionCount; p++ {
		nodes, err := Nodes(p, members, width)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range nodes {
			counts[id]++
		}
	}
	mean := DefaultPartitionCount * width / len(members)
	for id, c := range counts {
		if c < mean*7/10 || c > mean*13/10 {
			t.Errorf("node %s holds %d shard slots, mean %d — distribution skewed", id, c, mean)
		}
	}
}

// TestPartitionBalance: data IDs spread roughly evenly across partitions.
func TestPartitionBalance(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x9A57E4, 2))
	counts := make([]int, 64)
	const ids = 64 * 1000
	for range ids {
		var id meta.VersionID
		for i := range id {
			id[i] = byte(rng.UintN(256))
		}
		counts[Partition(id, 64)]++
	}
	for p, c := range counts {
		if c < 700 || c > 1300 {
			t.Errorf("partition %d got %d of %d IDs — distribution skewed", p, c, ids)
		}
	}
}

// TestMinimalMovement: adding one node changes only the assignments that
// rank it into their width — the rendezvous property that keeps future
// rebalances proportional to capacity added (ADR-0004).
func TestMinimalMovement(t *testing.T) {
	before := testMembers(9)
	after := testMembers(10) // adds node-09
	const width = 6
	changed := 0
	for p := uint64(0); p < DefaultPartitionCount; p++ {
		a, err := Nodes(p, before, width)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Nodes(p, after, width)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Equal(a, b) {
			continue
		}
		changed++
		// Any change must be exactly the new node displacing one slot:
		// the survivors keep their relative order.
		if !slices.Contains(b, "node-09") {
			t.Fatalf("partition %d changed without involving the new node: %v -> %v", p, a, b)
		}
		var rest []seam.NodeID
		for _, id := range b {
			if id != "node-09" {
				rest = append(rest, id)
			}
		}
		if !slices.Equal(rest, a[:width-1]) {
			t.Fatalf("partition %d reshuffled survivors: %v -> %v", p, a, b)
		}
	}
	// Expect about width/n of partitions to change; demand well under a
	// mass reshuffle and more than zero.
	if changed == 0 || changed > DefaultPartitionCount*width*2/len(after) {
		t.Errorf("%d of %d partitions changed on one join — expected ~%d",
			changed, DefaultPartitionCount, DefaultPartitionCount*width/len(after))
	}
}

func TestRefusals(t *testing.T) {
	if _, err := Nodes(1, testMembers(3), 4); err == nil {
		t.Error("width 4 on 3 nodes accepted; the node-distinct floor must refuse")
	}
	if _, err := Nodes(1, testMembers(3), 0); err == nil {
		t.Error("width 0 accepted")
	}
	dup := []seam.NodeID{"a", "b", "a"}
	if _, err := Nodes(1, dup, 2); err == nil {
		t.Error("duplicate member accepted; would void the node-distinct invariant")
	}
}
