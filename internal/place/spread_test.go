package place

import (
	"fmt"
	"slices"
	"testing"

	"github.com/hamster-storage/hamster/internal/seam"
)

// zoned builds n nodes laid out as `zones` zones with `perZone` nodes each,
// every node on its own host. So 3×2 is three zones of two machines.
func zoned(zones, perZone int) []Node {
	var out []Node
	for z := 0; z < zones; z++ {
		for h := 0; h < perZone; h++ {
			id := fmt.Sprintf("z%d-n%d", z, h)
			out = append(out, Node{ID: seam.NodeID(id), Zone: fmt.Sprintf("z%d", z), Host: id})
		}
	}
	return out
}

func layoutOf(nodes []Node) Layout {
	return Layout{Version: 1, PartitionCount: DefaultPartitionCount, Members: nodes}
}

// TestSpreadGolden pins the exact spread for a fixed labeled topology — the
// algorithm's selection, locked like every placement choice.
func TestSpreadGolden(t *testing.T) {
	l := layoutOf(zoned(3, 2)) // z0,z1,z2 each with two nodes
	got6, err := l.Nodes(100, 6)
	if err != nil {
		t.Fatal(err)
	}
	want6 := []seam.NodeID{"z0-n1", "z2-n1", "z1-n0", "z1-n1", "z2-n0", "z0-n0"}
	if !slices.Equal(got6, want6) {
		t.Fatalf("spread(100, 6) = %v, want %v", got6, want6)
	}
	got3, err := l.Nodes(100, 3)
	if err != nil {
		t.Fatal(err)
	}
	want3 := []seam.NodeID{"z0-n1", "z2-n1", "z1-n0"}
	if !slices.Equal(got3, want3) {
		t.Fatalf("spread(100, 3) = %v, want %v", got3, want3)
	}
}

// TestSpreadEvenAcrossZones: shards land on as many distinct zones as the
// width allows, then balance within. With three zones of two, a width-3
// object touches three distinct zones; a width-6 object two per zone.
func TestSpreadEvenAcrossZones(t *testing.T) {
	l := layoutOf(zoned(3, 2))
	for p := uint64(0); p < 512; p++ {
		three, err := l.Nodes(p, 3)
		if err != nil {
			t.Fatal(err)
		}
		zones := map[string]int{}
		for _, id := range three {
			zones[zoneOf(l, id)]++
		}
		if len(zones) != 3 {
			t.Fatalf("partition %d width 3 touched %d zones, want 3: %v", p, len(zones), three)
		}

		six, err := l.Nodes(p, 6)
		if err != nil {
			t.Fatal(err)
		}
		zc := map[string]int{}
		for _, id := range six {
			zc[zoneOf(l, id)]++
		}
		for z, c := range zc {
			if c != 2 {
				t.Fatalf("partition %d width 6: zone %s holds %d shards, want 2 (%v)", p, z, c, six)
			}
		}
	}
}

// TestSpreadCappedByZones: a 4+2 object on three zones puts at most two
// shards in any zone — so a whole zone can fail within the m=2 budget, the
// ADR-0016 sweet spot.
func TestSpreadCappedByZones(t *testing.T) {
	l := layoutOf(zoned(3, 2))
	for p := uint64(0); p < 512; p++ {
		nodes, err := l.Nodes(p, 6)
		if err != nil {
			t.Fatal(err)
		}
		zc := map[string]int{}
		for _, id := range nodes {
			zc[zoneOf(l, id)]++
		}
		for z, c := range zc {
			if c > 2 {
				t.Fatalf("partition %d: zone %s holds %d of 6 shards — a zone loss exceeds m=2", p, z, c)
			}
		}
	}
}

// TestSpreadNodeDistinctAndPrefixStable: the hard floor and the prefix
// property hold under labels exactly as for the bare ranking.
func TestSpreadNodeDistinctAndPrefixStable(t *testing.T) {
	l := layoutOf(zoned(4, 3)) // 12 nodes, 4 zones
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

// TestSpreadSingleDomainCollapse: when every node shares one zone and one
// host (a single box, or unlabeled data), spread is exactly the bare
// rendezvous ranking — no behavior change off the failure-domain path.
func TestSpreadSingleDomainCollapse(t *testing.T) {
	ids := testMembers(8)
	flat := make([]Node, len(ids))
	for i, id := range ids {
		flat[i] = Node{ID: id, Host: "box", Zone: "box"}
	}
	l := layoutOf(flat)
	for p := uint64(0); p < 256; p++ {
		got, err := l.Nodes(p, 6)
		if err != nil {
			t.Fatal(err)
		}
		want, err := Nodes(p, ids, 6)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("partition %d: single-domain spread %v != rendezvous %v", p, got, want)
		}
	}
}

func zoneOf(l Layout, id seam.NodeID) string {
	for _, n := range l.Members {
		if n.ID == id {
			return n.Zone
		}
	}
	return ""
}
