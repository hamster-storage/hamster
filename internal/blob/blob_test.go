package blob_test

import (
	"bytes"
	"errors"
	"io/fs"
	"testing"

	"github.com/hamster-storage/hamster/internal/blob"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sys"
)

func TestRoundTrip(t *testing.T) {
	disk, err := sys.NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := blob.NewStore(disk)

	id := meta.VersionID{1, 2, 3}
	if err := s.Put(id, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(id)
	if err != nil || !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("get: %q, %v", got, err)
	}

	if err := s.Remove(id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(id); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("get after remove: %v, want fs.ErrNotExist", err)
	}
	if _, err := s.Get(meta.VersionID{9}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("get of never-written blob: %v, want fs.ErrNotExist", err)
	}
}
