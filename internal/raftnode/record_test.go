package raftnode

import (
	"maps"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/hamster-storage/hamster/internal/meta"
)

// Snapshot data must round-trip a store exactly: dump, encode, decode,
// dump again, compare.
func TestSnapshotDataRoundTrip(t *testing.T) {
	s := meta.NewStore()
	if err := s.ApplyCreateBucket(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "bkt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyPutObject(meta.PutObject{ProposedAtUnixMS: 2, Bucket: "bkt", Key: "k",
		VersionID: meta.VersionID{1}, Size: 7, ETag: []byte{0xAB}, ObjectChecksum: []byte{0xCD},
		UserMetadata: map[string]string{"a": "1"}}); err != nil {
		t.Fatal(err)
	}

	members := map[uint64]peerInfo{
		1: {node: "n1", dial: "10.0.0.1:7946"},
		2: {node: "n2", dial: "10.0.0.2:7946"},
		7: {node: "node-seven"}, // no dial: the simulator's shape
	}
	removed := map[uint64]struct{}{3: {}, 9: {}}
	restored, restoredMembers, restoredRemoved, err := decodeSnapshotData(encodeSnapshotData(s.Dump(), members, removed))
	if err != nil {
		t.Fatal(err)
	}
	got, want := restored.Dump(), s.Dump()
	if len(got) != len(want) {
		t.Fatalf("restored %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Key != want[i].Key || string(got[i].Value) != string(want[i].Value) {
			t.Fatalf("row %d diverged: %q vs %q", i, got[i].Key, want[i].Key)
		}
	}
	if !maps.Equal(restoredMembers, members) {
		t.Fatalf("members diverged: %v vs %v", restoredMembers, members)
	}
	if !maps.Equal(restoredRemoved, removed) {
		t.Fatalf("removed tombstone diverged: %v vs %v", restoredRemoved, removed)
	}
}

// validLog is the boot rule: a rotated file must open with a snapshot
// frame; only the oldest file may start bare.
func TestValidLog(t *testing.T) {
	bare := encodeRecord(record{hs: raftpb.HardState{Term: 1, Commit: 0, Vote: 1}})
	withSnap := encodeRecord(record{snap: raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 1},
		Data:     encodeSnapshotData(nil, nil, nil),
	}})

	cases := []struct {
		name    string
		records [][]byte
		oldest  bool
		want    bool
	}{
		{"oldest_bare", [][]byte{bare}, true, true},
		{"oldest_empty", nil, true, true},
		{"rotated_with_snapshot", [][]byte{withSnap, bare}, false, true},
		{"rotated_without_snapshot", [][]byte{bare}, false, false},
		{"rotated_empty", nil, false, false},
		{"rotated_garbage_first", [][]byte{{0xFF, 0xFF}}, false, false},
	}
	for _, c := range cases {
		if got := validLog(c.records, c.oldest); got != c.want {
			t.Errorf("%s: validLog = %v, want %v", c.name, got, c.want)
		}
	}
}
