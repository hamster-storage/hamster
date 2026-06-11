package sys

import (
	"bytes"
	"errors"
	"io/fs"
	"slices"
	"testing"
)

// TestDiskContract drives the production Disk through the same sequence the
// simulated disk is tested with, so the two implementations agree on the
// seam.Disk contract (minus crash semantics, which only the simulator can
// model).
func TestDiskContract(t *testing.T) {
	d, err := NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := d.ReadFile("missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadFile on missing file: %v, want fs.ErrNotExist", err)
	}
	if err := d.Remove("missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Remove on missing file: %v, want fs.ErrNotExist", err)
	}
	if err := d.WriteFile("../escape", nil); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("non-local name accepted: %v", err)
	}

	content := []byte("shard bytes")
	for _, step := range []error{
		d.WriteFile("shards/2", []byte("b")),
		d.WriteFile("shards/1", content),
		d.Sync("shards/1"),
		d.WriteFile("gone", []byte("c")),
		d.Sync("gone"),
		d.Remove("gone"),
		d.Sync("gone"),
	} {
		if step != nil {
			t.Fatal(step)
		}
	}

	got, err := d.ReadFile("shards/1")
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("ReadFile after sync: %q, %v", got, err)
	}
	names, err := d.List()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"shards/1", "shards/2"}
	if !slices.Equal(names, want) {
		t.Fatalf("List() = %v, want %v", names, want)
	}
	if _, err := d.ReadFile("gone"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("removed file still readable: %v", err)
	}
}
