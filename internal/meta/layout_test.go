package meta

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"slices"
	"testing"
)

func fullClusterLayout() ClusterLayout {
	return ClusterLayout{
		FormatVersion:  1,
		Version:        7,
		PartitionCount: 4096,
		Members:        []string{"n1", "n2", "n3", "n4", "n5", "n6"},
	}
}

func TestClusterLayoutCodecRoundTrip(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		in := fullClusterLayout()
		out, err := unmarshalClusterLayout(marshalClusterLayout(in))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(in, out) {
			t.Fatalf("round trip:\n in: %+v\nout: %+v", in, out)
		}
	})
	t.Run("zero", func(t *testing.T) {
		out, err := unmarshalClusterLayout(marshalClusterLayout(ClusterLayout{}))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ClusterLayout{}, out) {
			t.Fatalf("zero round trip: %+v", out)
		}
		if len(marshalClusterLayout(ClusterLayout{})) != 0 {
			t.Fatal("zero layout should encode to zero bytes (proto3 zero omission)")
		}
	})
}

// TestClusterLayoutGolden pins exact record bytes, hand-verifiable. A change
// here is an on-disk format change: deliberate in v0, a bug after v1. The
// partition value lives in committed records forever, so this must not drift.
func TestClusterLayoutGolden(t *testing.T) {
	// ClusterLayout{format 1, version 3, partitions 4096, members n1,n2}:
	//   0801            field 1 varint 1     (format_version)
	//   1003            field 2 varint 3     (version)
	//   188020          field 3 varint 4096  (partition_count)
	//   22026e31        field 4 bytes "n1"
	//   22026e32        field 4 bytes "n2"
	const want = "0801100318802022026e3122026e32"
	got := hex.EncodeToString(marshalClusterLayout(ClusterLayout{
		FormatVersion: 1, Version: 3, PartitionCount: 4096, Members: []string{"n1", "n2"},
	}))
	if got != want {
		t.Fatalf("ClusterLayout bytes changed:\n got %s\nwant %s", got, want)
	}
}

// TestSetClusterLayoutProposalGolden pins the proposal envelope at the
// reserved field 15 (propSetLayout) — the slot the design held open for this.
func TestSetClusterLayoutProposalGolden(t *testing.T) {
	// SetClusterLayout{version 3, partitions 4096, members n1,n2}, at 0:
	//   0801                  field 1 varint 1   (format_version)
	//   7a0d <13 bytes>       field 15 bytes     (set_layout command)
	//     0803                  field 1 varint 3     (version)
	//     108020                field 2 varint 4096  (partition_count)
	//     1a026e31              field 3 bytes "n1"
	//     1a026e32              field 3 bytes "n2"
	const want = "08017a0d08031080201a026e311a026e32"
	got := hex.EncodeToString(EncodeProposal(SetClusterLayout{
		Version: 3, PartitionCount: 4096, Members: []string{"n1", "n2"},
	}))
	if got != want {
		t.Fatalf("SetClusterLayout proposal bytes changed:\n got %s\nwant %s", got, want)
	}
}

// TestClusterLayoutNodesRoundTrip exercises the labeled member set (ADR-0016,
// field 5).
func TestClusterLayoutNodesRoundTrip(t *testing.T) {
	in := ClusterLayout{
		FormatVersion: 1, Version: 4, PartitionCount: 4096,
		Nodes: []LayoutNode{
			{ID: "n1", Host: "boxA", Zone: "z1"},
			{ID: "n2", Host: "boxA", Zone: "z1"},
			{ID: "n3", Host: "boxB", Zone: "z2"},
		},
	}
	out, err := unmarshalClusterLayout(marshalClusterLayout(in))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip:\n in: %+v\nout: %+v", in, out)
	}
}

// TestClusterLayoutNodesGolden pins the field-5 (labeled node) bytes.
func TestClusterLayoutNodesGolden(t *testing.T) {
	// {format 1, version 3, partitions 4096, node n1/h/z}:
	//   0801            format_version 1
	//   1003            version 3
	//   188020          partition_count 4096
	//   2a0a <10 bytes> field 5 node:
	//     0a026e31        id "n1"
	//     120168          host "h"
	//     1a017a          zone "z"
	const want = "080110031880202a0a0a026e311201681a017a"
	got := hex.EncodeToString(marshalClusterLayout(ClusterLayout{
		FormatVersion: 1, Version: 3, PartitionCount: 4096,
		Nodes: []LayoutNode{{ID: "n1", Host: "h", Zone: "z"}},
	}))
	if got != want {
		t.Fatalf("ClusterLayout node bytes changed:\n got %s\nwant %s", got, want)
	}
}

// TestEffectiveNodes: Nodes when present, else the legacy Members IDs with
// host and zone defaulted to the ID (a pass-1 layout reads as one node per
// host and zone, spreading exactly as the bare rendezvous ranking).
func TestEffectiveNodes(t *testing.T) {
	withNodes := ClusterLayout{Nodes: []LayoutNode{{ID: "a", Host: "h", Zone: "z"}}}
	if got := withNodes.EffectiveNodes(); !reflect.DeepEqual(got, withNodes.Nodes) {
		t.Fatalf("with nodes: %+v", got)
	}
	legacy := ClusterLayout{Members: []string{"a", "b"}}
	want := []LayoutNode{{ID: "a", Host: "a", Zone: "a"}, {ID: "b", Host: "b", Zone: "b"}}
	if got := legacy.EffectiveNodes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy fallback: %+v, want %+v", got, want)
	}
}

// TestSetClusterLayoutNodesRoundTrip: the proposal carries the labeled set.
func TestSetClusterLayoutNodesRoundTrip(t *testing.T) {
	p := SetClusterLayout{
		ProposedAtUnixMS: 123, Version: 2, PartitionCount: 4096,
		Nodes: []LayoutNode{{ID: "n1", Host: "h1", Zone: "z1"}, {ID: "n2", Host: "h2", Zone: "z2"}},
	}
	got, err := DecodeProposal(EncodeProposal(p))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, p) {
		t.Fatalf("round trip:\n got %+v\nwant %+v", got, p)
	}
}

// TestApplySetClusterLayout proves the compare-and-set: the first install is
// version 1, each later one exactly +1, and every off-version or invalid
// proposal is a deterministic refusal that leaves state untouched.
func TestApplySetClusterLayout(t *testing.T) {
	s := NewStore()
	if _, ok := s.ClusterLayout(); ok {
		t.Fatal("a fresh store has no layout")
	}

	// The first install must be version 1.
	if err := s.ApplySetClusterLayout(SetClusterLayout{Version: 2, PartitionCount: 4096, Members: []string{"n1"}}); err != ErrStaleLayout {
		t.Fatalf("first install at v2: got %v, want ErrStaleLayout", err)
	}
	if err := s.ApplySetClusterLayout(SetClusterLayout{Version: 1, PartitionCount: 4096, Members: []string{"n1", "n2", "n3"}}); err != nil {
		t.Fatalf("install v1: %v", err)
	}
	got, ok := s.ClusterLayout()
	if !ok || got.Version != 1 || got.PartitionCount != 4096 || !slices.Equal(got.Members, []string{"n1", "n2", "n3"}) {
		t.Fatalf("after v1: %+v ok=%v", got, ok)
	}

	// A retransmitted v1 (stale) and a gapped v3 are both refused.
	if err := s.ApplySetClusterLayout(SetClusterLayout{Version: 1, PartitionCount: 4096, Members: []string{"x"}}); err != ErrStaleLayout {
		t.Fatalf("re-propose v1: got %v, want ErrStaleLayout", err)
	}
	if err := s.ApplySetClusterLayout(SetClusterLayout{Version: 3, PartitionCount: 4096, Members: []string{"n1"}}); err != ErrStaleLayout {
		t.Fatalf("gap to v3: got %v, want ErrStaleLayout", err)
	}

	// v2 advances cleanly.
	if err := s.ApplySetClusterLayout(SetClusterLayout{Version: 2, PartitionCount: 4096, Members: []string{"n1", "n2", "n3", "n4"}}); err != nil {
		t.Fatalf("advance v2: %v", err)
	}

	// The partition count may never change (ADR-0004), and a layout with no
	// members is invalid.
	if err := s.ApplySetClusterLayout(SetClusterLayout{Version: 3, PartitionCount: 8192, Members: []string{"n1"}}); err != ErrInvalidLayout {
		t.Fatalf("partition-count change: got %v, want ErrInvalidLayout", err)
	}
	if err := s.ApplySetClusterLayout(SetClusterLayout{Version: 3, PartitionCount: 4096, Members: nil}); err != ErrInvalidLayout {
		t.Fatalf("empty members: got %v, want ErrInvalidLayout", err)
	}

	// Every refusal left the committed layout at v2 with four members.
	if got, _ := s.ClusterLayout(); got.Version != 2 || len(got.Members) != 4 {
		t.Fatalf("state after refusals: %+v", got)
	}
}

// TestClusterLayoutPersistRoundTrip proves the layout rides the snapshot path
// (Dump/Restore) byte-identically — what a Raft snapshot ships and a
// restarting node restores.
func TestClusterLayoutPersistRoundTrip(t *testing.T) {
	s := NewStore()
	if err := s.ApplySetClusterLayout(SetClusterLayout{
		Version: 1, PartitionCount: 4096, Members: []string{"n1", "n2", "n3", "n4", "n5", "n6"},
	}); err != nil {
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
	if got, ok := s2.ClusterLayout(); !ok || got.Version != 1 || len(got.Members) != 6 {
		t.Fatalf("restored layout: %+v ok=%v", got, ok)
	}
}
