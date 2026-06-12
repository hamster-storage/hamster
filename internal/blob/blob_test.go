package blob_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"

	"github.com/hamster-storage/hamster/internal/blob"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sys"
)

func newStore(t *testing.T) *blob.Store {
	t.Helper()
	disk, err := sys.NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return blob.NewStore(disk)
}

func TestRoundTrip(t *testing.T) {
	s := newStore(t)

	id := meta.VersionID{1, 2, 3}
	size, err := s.Put(id, strings.NewReader("hello"))
	if err != nil || size != 5 {
		t.Fatalf("put: size %d, %v", size, err)
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

// A zero-length object is a real object: Put with an empty source must
// create a blob that Get can find.
func TestPutEmpty(t *testing.T) {
	s := newStore(t)

	id := meta.VersionID{4}
	size, err := s.Put(id, strings.NewReader(""))
	if err != nil || size != 0 {
		t.Fatalf("put: size %d, %v", size, err)
	}
	got, err := s.Get(id)
	if err != nil || len(got) != 0 {
		t.Fatalf("get: %q, %v", got, err)
	}
}

// Put streams in write-buffer-sized pieces; a body larger than one buffer
// must arrive intact and report its full size.
func TestPutLargerThanWriteBuffer(t *testing.T) {
	s := newStore(t)

	want := bytes.Repeat([]byte("0123456789abcdef"), 3<<17) // 6 MiB
	id := meta.VersionID{5}
	size, err := s.Put(id, bytes.NewReader(want))
	if err != nil || size != int64(len(want)) {
		t.Fatalf("put: size %d, %v", size, err)
	}
	got, err := s.Get(id)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("get: %d bytes, %v", len(got), err)
	}
}

// errReader yields some bytes, then fails.
type errReader struct {
	data []byte
	err  error
}

func (r *errReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// A source error mid-stream must surface (wrapped, still classifiable) and
// leave no blob behind.
func TestPutSourceErrorCleansUp(t *testing.T) {
	s := newStore(t)

	cause := errors.New("connection torn")
	id := meta.VersionID{6}
	if _, err := s.Put(id, &errReader{data: []byte("partial"), err: cause}); !errors.Is(err, cause) {
		t.Fatalf("put: %v, want wrapped %v", err, cause)
	}
	if _, err := s.Get(id); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("get after failed put: %v, want fs.ErrNotExist", err)
	}
}

var _ io.Reader = (*errReader)(nil)
