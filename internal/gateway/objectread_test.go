package gateway

import (
	"bytes"
	"io"
	"testing"
)

// patternBytes is a deterministic, position-dependent byte stream so a read at
// any offset can be verified independently of how it was windowed.
func patternBytes(off, n int64) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((off + int64(i)) * 31)
	}
	return b
}

// newPatternReader builds a rangeReader over a size-byte pattern object,
// recording each fetch's length so the test can assert the window bound.
func newPatternReader(size int64, fetches *[]int64) *rangeReader {
	return &rangeReader{
		size: size,
		fetch: func(off, length int64) ([]byte, error) {
			if fetches != nil {
				*fetches = append(*fetches, length)
			}
			return patternBytes(off, length), nil
		},
	}
}

func TestRangeReaderWholeObjectStreamsInBoundedWindows(t *testing.T) {
	// Larger than rangeWindow so a whole-object read must cross window
	// boundaries — proving it streams rather than fetching the object at once.
	const size = rangeWindow*2 + 12345
	var fetches []int64
	r := newPatternReader(size, &fetches)

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if int64(len(got)) != size {
		t.Fatalf("read %d bytes, want %d", len(got), size)
	}
	if !bytes.Equal(got, patternBytes(0, size)) {
		t.Fatal("whole-object read did not reconstruct the object")
	}
	if len(fetches) < 3 {
		t.Fatalf("expected the read to span multiple windows, got %d fetches", len(fetches))
	}
	for i, n := range fetches {
		if n > rangeWindow {
			t.Fatalf("fetch %d pulled %d bytes, exceeding the %d window", i, n, rangeWindow)
		}
	}
}

func TestRangeReaderSeekAndPartialRead(t *testing.T) {
	const size = rangeWindow + 5000
	r := newPatternReader(size, nil)

	// A mid-object range, crossing into the second window.
	const off, length = rangeWindow - 100, 400
	if pos, err := r.Seek(off, io.SeekStart); err != nil || pos != off {
		t.Fatalf("Seek(%d) = %d, %v", off, pos, err)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(buf, patternBytes(off, length)) {
		t.Fatal("range read returned the wrong bytes")
	}

	// Seek to end + read the suffix.
	const suffixStart = size - 50
	if _, err := r.Seek(suffixStart, io.SeekStart); err != nil {
		t.Fatalf("Seek suffix: %v", err)
	}
	suffix, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll suffix: %v", err)
	}
	if !bytes.Equal(suffix, patternBytes(suffixStart, size-suffixStart)) {
		t.Fatal("suffix read returned the wrong bytes")
	}
}

func TestRangeReaderSeekEndDoesNotFetch(t *testing.T) {
	// http.ServeContent calls Seek(0, SeekEnd) to size the body; a HEAD must
	// not pull any object bytes. The size is known up front, so no fetch fires.
	const size = rangeWindow + 1
	var fetches []int64
	r := newPatternReader(size, &fetches)

	if pos, err := r.Seek(0, io.SeekEnd); err != nil || pos != size {
		t.Fatalf("Seek(0, SeekEnd) = %d, %v; want %d", pos, err, size)
	}
	if len(fetches) != 0 {
		t.Fatalf("Seek to end fetched %d times, want 0", len(fetches))
	}
}

func TestRangeReaderReadAtEOF(t *testing.T) {
	r := newPatternReader(10, nil)
	if _, err := r.Seek(10, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	n, err := r.Read(make([]byte, 4))
	if n != 0 || err != io.EOF {
		t.Fatalf("Read at EOF = %d, %v; want 0, EOF", n, err)
	}
}
