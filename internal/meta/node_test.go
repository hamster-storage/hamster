package meta

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"
)

// TestNodeRecordCodecRoundTrip: a full record round-trips, the zero record
// encodes to zero bytes (proto3 omission), and a record without the capacity
// field (4 → here field 5) decodes to capacity 0 — backward compatible
// (ADR-0004, invariant 2).
func TestNodeRecordCodecRoundTrip(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		in := NodeRecord{FormatVersion: 1, NodeID: "n1", Host: "boxA", Zone: "z1", Capacity: 7}
		out, err := unmarshalNodeRecord(marshalNodeRecord(in))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip:\n in: %+v\nout: %+v", in, out)
		}
	})
	t.Run("zero", func(t *testing.T) {
		if len(marshalNodeRecord(NodeRecord{})) != 0 {
			t.Fatal("zero record should encode to zero bytes")
		}
	})
	t.Run("capacity-omitted", func(t *testing.T) {
		// A record written before capacity existed carries no field 5 and reads
		// as capacity 0 (equal); the equal-weight record is byte-identical.
		noCap := marshalNodeRecord(NodeRecord{FormatVersion: 1, NodeID: "n1", Host: "h", Zone: "z"})
		withCap := marshalNodeRecord(NodeRecord{FormatVersion: 1, NodeID: "n1", Host: "h", Zone: "z", Capacity: 5})
		if len(withCap) <= len(noCap) {
			t.Fatalf("capacity record (%d) should exceed equal one (%d)", len(withCap), len(noCap))
		}
		out, err := unmarshalNodeRecord(noCap)
		if err != nil || out.Capacity != 0 {
			t.Fatalf("decode without capacity: %+v err=%v", out, err)
		}
	})
}

// TestNodeRecordGolden pins exact record bytes — this is an on-disk format
// (s/node/<id>), so the layout is a forever contract.
func TestNodeRecordGolden(t *testing.T) {
	// NodeRecord{format 1, id n1, host h, zone z, capacity 5}:
	//   0801          format_version 1
	//   12026e31      field 2 bytes "n1"
	//   1a0168        field 3 bytes "h"
	//   22017a        field 4 bytes "z"
	//   2805          field 5 varint 5 (capacity)
	const want = "080112026e311a016822017a2805"
	got := hex.EncodeToString(marshalNodeRecord(NodeRecord{
		FormatVersion: 1, NodeID: "n1", Host: "h", Zone: "z", Capacity: 5,
	}))
	if got != want {
		t.Fatalf("NodeRecord bytes changed:\n got %s\nwant %s", got, want)
	}
}

// TestRegisterNodeProposalGolden pins the proposal envelope at field 16
// (propRegisterNode) — the slot the design reserved for membership records.
func TestRegisterNodeProposalGolden(t *testing.T) {
	// RegisterNode{id n1, host h, zone z, capacity 5}, at 0:
	//   0801              format_version 1
	//   82010c <12 bytes> field 16 bytes (register_node command)
	//     0a026e31          id "n1"
	//     120168            host "h"
	//     1a017a            zone "z"
	//     2005              capacity 5
	const want = "080182010c0a026e311201681a017a2005"
	got := hex.EncodeToString(EncodeProposal(RegisterNode{
		NodeID: "n1", Host: "h", Zone: "z", Capacity: 5,
	}))
	if got != want {
		t.Fatalf("RegisterNode proposal bytes changed:\n got %s\nwant %s", got, want)
	}
}

func TestRegisterNodeProposalRoundTrip(t *testing.T) {
	p := RegisterNode{ProposedAtUnixMS: 123, NodeID: "n2", Host: "boxB", Zone: "z2", Capacity: 3}
	got, err := DecodeProposal(EncodeProposal(p))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, p) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got, p)
	}
}

// TestApplyRegisterNode: registration is an idempotent upsert keyed by node
// ID — a re-registration replaces the row — and an empty ID is refused.
func TestApplyRegisterNode(t *testing.T) {
	s := NewStore()
	if _, ok := s.Node("n1"); ok {
		t.Fatal("a fresh store has no node records")
	}
	if err := s.ApplyRegisterNode(RegisterNode{NodeID: ""}); err != ErrInvalidNode {
		t.Fatalf("empty node ID: got %v, want ErrInvalidNode", err)
	}
	if err := s.ApplyRegisterNode(RegisterNode{NodeID: "n1", Host: "h1", Zone: "z1", Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyRegisterNode(RegisterNode{NodeID: "n2", Host: "h2", Zone: "z2"}); err != nil {
		t.Fatal(err)
	}
	// Re-register n1 with changed labels: the row is replaced, not duplicated.
	if err := s.ApplyRegisterNode(RegisterNode{NodeID: "n1", Host: "h1b", Zone: "z1b", Capacity: 9}); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Node("n1")
	if !ok || got.Host != "h1b" || got.Zone != "z1b" || got.Capacity != 9 {
		t.Fatalf("after re-register: %+v ok=%v", got, ok)
	}
	all := s.Nodes()
	if len(all) != 2 || all[0].NodeID != "n1" || all[1].NodeID != "n2" {
		t.Fatalf("Nodes() = %+v, want [n1 n2] in ID order", all)
	}
}

// TestApplySetNodeDraining: the drain flag flips on a registered node,
// preserving its labels; an unknown node is refused; clearing works too.
func TestApplySetNodeDraining(t *testing.T) {
	s := NewStore()
	if err := s.ApplySetNodeDraining(SetNodeDraining{NodeID: "ghost", Draining: true}); err != ErrInvalidNode {
		t.Fatalf("draining an unregistered node: got %v, want ErrInvalidNode", err)
	}
	if err := s.ApplyRegisterNode(RegisterNode{NodeID: "n1", Host: "h1", Zone: "z1", Capacity: 7}); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplySetNodeDraining(SetNodeDraining{NodeID: "n1", Draining: true}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Node("n1")
	if !got.Draining || got.Host != "h1" || got.Zone != "z1" || got.Capacity != 7 {
		t.Fatalf("drain flipped but labels/capacity not preserved: %+v", got)
	}
	// Clearing it restores an active node, labels intact.
	if err := s.ApplySetNodeDraining(SetNodeDraining{NodeID: "n1", Draining: false}); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Node("n1"); got.Draining || got.Capacity != 7 {
		t.Fatalf("undrain: %+v", got)
	}
}

// TestNodeRecordPersistRoundTrip proves node rows ride the snapshot path
// (Dump/Restore) byte-identically — what a Raft snapshot ships and a
// restarting node restores — alongside the layout singleton, with no key
// collision between s/node/* and s/layout.
func TestNodeRecordPersistRoundTrip(t *testing.T) {
	s := NewStore()
	if err := s.ApplySetClusterLayout(SetClusterLayout{
		Version: 1, PartitionCount: 4096,
		Nodes: []LayoutNode{{ID: "n1", Host: "h1", Zone: "z1"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyRegisterNode(RegisterNode{NodeID: "n1", Host: "h1", Zone: "z1", Capacity: 2}); err != nil {
		t.Fatal(err)
	}
	rows := s.Dump()

	s2 := NewStore()
	for _, r := range rows {
		if err := s2.Restore(r.Key, r.Value); err != nil {
			t.Fatalf("restore %q: %v", r.Key, err)
		}
	}
	rows2 := s2.Dump()
	if len(rows) != len(rows2) {
		t.Fatalf("row count after restore: %d vs %d", len(rows), len(rows2))
	}
	for i := range rows {
		if rows[i].Key != rows2[i].Key || !bytes.Equal(rows[i].Value, rows2[i].Value) {
			t.Fatalf("row %d differs after restore: %q", i, rows[i].Key)
		}
	}
	if got, ok := s2.Node("n1"); !ok || got.Capacity != 2 {
		t.Fatalf("restored node: %+v ok=%v", got, ok)
	}
	if _, ok := s2.ClusterLayout(); !ok {
		t.Fatal("layout lost across restore")
	}
}
