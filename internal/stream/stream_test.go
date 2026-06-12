package stream

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
)

// frame writes payload into a frame and returns its bytes, splitting the
// writes at the offsets in cuts to exercise arbitrary Write segmentation.
func frame(t *testing.T, payload []byte, chunkSize int, cuts ...int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, int64(len(payload)), chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	prev := 0
	for _, c := range append(cuts, len(payload)) {
		if _, err := w.Write(payload[prev:c]); err != nil {
			t.Fatal(err)
		}
		prev = c
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if w.FrameSize() != int64(buf.Len()) {
		t.Fatalf("FrameSize %d, frame is %d bytes", w.FrameSize(), buf.Len())
	}
	return buf.Bytes()
}

// readAll reads the whole plaintext back out of a frame.
func readAll(f []byte) ([]byte, error) {
	r, err := NewReader(bytes.NewReader(f), int64(len(f)))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(io.NewSectionReader(r, 0, r.Size()))
}

// TestGoldenFrame pins the exact encoding of a small frame. If this test
// breaks, the change broke compatibility with every frame already on
// disk — that is invariant 2, not a test to update casually.
func TestGoldenFrame(t *testing.T) {
	got := hex.EncodeToString(frame(t, []byte("hamster-ok"), 4))
	const want = "484d46310100040a68616d737465722d6f6b0404021232a863a8eb95bc7060cb6e0f000000"
	if got != want {
		t.Fatalf("golden frame diverged:\n got %s\nwant %s", got, want)
	}
}

// TestRoundTrip drives randomized payloads, chunk sizes, and write
// segmentations through a frame and back, plus the edge sizes around
// chunk boundaries. Seeded: every run is the same run.
func TestRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xA0, 0x57E4))
	sizes := []int{0, 1, 2, 999, 1000, 1001, 1999, 2000, 2001, 10240}
	for range 20 {
		sizes = append(sizes, rng.IntN(64<<10))
	}
	for _, size := range sizes {
		for _, chunk := range []int{1, 7, 1000, DefaultChunkSize} {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(rng.Uint32())
			}
			var cuts []int
			for c := 0; c < size; c += 1 + rng.IntN(size/3+1) {
				cuts = append(cuts, c)
			}
			f := frame(t, payload, chunk, cuts...)
			if want := FrameSize(int64(size), chunk); want != int64(len(f)) {
				t.Fatalf("size %d chunk %d: FrameSize predicts %d, frame is %d bytes", size, chunk, want, len(f))
			}
			got, err := readAll(f)
			if err != nil {
				t.Fatalf("size %d chunk %d: %v", size, chunk, err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("size %d chunk %d: round trip diverged", size, chunk)
			}
			// The same input always yields the same bytes.
			if !bytes.Equal(f, frame(t, payload, chunk)) {
				t.Fatalf("size %d chunk %d: frame encoding is not deterministic", size, chunk)
			}
		}
	}
}

// TestRangeReads checks every read offset against the source bytes, with
// lengths spanning sub-chunk, chunk-straddling, and to-the-end reads —
// the Range request's whole job.
func TestRangeReads(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	payload := make([]byte, 10_000)
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	f := frame(t, payload, 1000)
	r, err := NewReader(bytes.NewReader(f), int64(len(f)))
	if err != nil {
		t.Fatal(err)
	}
	if r.Size() != int64(len(payload)) {
		t.Fatalf("Size %d, want %d", r.Size(), len(payload))
	}
	for off := 0; off <= len(payload); off++ {
		for _, length := range []int{1, 7, 1000, 2500, len(payload) - off} {
			if length < 0 || off+length > len(payload) {
				continue
			}
			p := make([]byte, length)
			n, err := r.ReadAt(p, int64(off))
			if err != nil || n != length {
				t.Fatalf("ReadAt(%d, %d) = %d, %v", off, length, n, err)
			}
			if !bytes.Equal(p, payload[off:off+length]) {
				t.Fatalf("ReadAt(%d, %d) returned wrong bytes", off, length)
			}
		}
	}
	// Past the end is io.EOF, not garbage.
	if n, err := r.ReadAt(make([]byte, 1), int64(len(payload))); err != io.EOF || n != 0 {
		t.Fatalf("read past end = %d, %v; want 0, EOF", n, err)
	}
	if n, err := r.ReadAt(make([]byte, 10), int64(len(payload))-3); err != io.EOF || n != 3 {
		t.Fatalf("read straddling end = %d, %v; want 3, EOF", n, err)
	}
}

// TestEveryByteCorruptionDetected flips every single byte of a frame in
// turn and demands the corruption is caught — at open or at read, but
// never served as data. Every byte of the format is load-bearing.
func TestEveryByteCorruptionDetected(t *testing.T) {
	payload := []byte("the quick brown hamster stuffs its cheeks")
	f := frame(t, payload, 16)
	for i := range f {
		corrupt := bytes.Clone(f)
		corrupt[i] ^= 0xFF
		got, err := readAll(corrupt)
		if err == nil {
			t.Errorf("flipping byte %d of %d went undetected (read %q)", i, len(f), got)
		}
	}
}

// TestTruncationDetected cuts the frame short at every length.
func TestTruncationDetected(t *testing.T) {
	payload := []byte("0123456789abcdef0123456789")
	f := frame(t, payload, 8)
	for n := range len(f) {
		if got, err := readAll(f[:n]); err == nil {
			t.Errorf("truncation to %d of %d bytes went undetected (read %q)", n, len(f), got)
		}
	}
}

// TestWriterContract: the writer refuses every way a frame could lie
// about its size.
func TestWriterContract(t *testing.T) {
	if _, err := NewWriter(io.Discard, -1, 4); err == nil {
		t.Error("negative plaintext size accepted")
	}
	if _, err := NewWriter(io.Discard, 10, 0); err == nil {
		t.Error("zero chunk size accepted")
	}

	w, err := NewWriter(io.Discard, 4, 16)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hello")); err == nil {
		t.Error("write past the declared size accepted")
	}
	if _, err := w.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err == nil {
		t.Error("Close with 2 of 4 declared bytes accepted")
	}
	if _, err := w.Write([]byte("!!")); err == nil {
		t.Error("write after Close accepted")
	}
	if err := w.Close(); err == nil {
		t.Error("second Close accepted")
	}
}

// TestZeroLengthObject: a zero-byte object is a real object with a real
// frame — header, empty index, trailer length.
func TestZeroLengthObject(t *testing.T) {
	f := frame(t, nil, DefaultChunkSize)
	r, err := NewReader(bytes.NewReader(f), int64(len(f)))
	if err != nil {
		t.Fatal(err)
	}
	if r.Size() != 0 {
		t.Fatalf("Size %d, want 0", r.Size())
	}
	if n, err := r.ReadAt(make([]byte, 1), 0); err != io.EOF || n != 0 {
		t.Fatalf("read from empty object = %d, %v; want 0, EOF", n, err)
	}
}

// TestForeignFramesRefused: frames this binary cannot honestly decode —
// newer versions, transforms not yet implemented — are refused with an
// explanation, not misread.
func TestForeignFramesRefused(t *testing.T) {
	// Build a valid frame, then rewrite header fields. The header layout
	// here starts magic(4), version varint, flags varint — single bytes
	// at these sizes.
	base := frame(t, []byte("data"), 4)

	newer := bytes.Clone(base)
	newer[4] = formatVersion + 1
	if _, err := readAll(newer); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Errorf("newer format version: err %v, want a 'newer' explanation", err)
	}

	for _, flag := range []byte{flagCompressed, flagEncrypted} {
		flagged := bytes.Clone(base)
		flagged[5] = flag
		if _, err := readAll(flagged); err == nil || !strings.Contains(err.Error(), "transform") {
			t.Errorf("flag %#x: err %v, want a 'transform' explanation", flag, err)
		}
	}

	if _, err := readAll([]byte("not a frame at all, sorry")); err == nil {
		t.Error("garbage accepted as a frame")
	}
}

// TestSmallObjectFrameOverhead documents the identity frame's cost on a
// small object: a few dozen bytes, the price of one read path.
func TestSmallObjectFrameOverhead(t *testing.T) {
	payload := make([]byte, 4096)
	f := frame(t, payload, DefaultChunkSize)
	overhead := len(f) - len(payload)
	if overhead > 64 {
		t.Fatalf("frame overhead on a 4 KiB object is %d bytes, want a few dozen", overhead)
	}
}

func ExampleWriter() {
	var buf bytes.Buffer
	w, _ := NewWriter(&buf, 11, DefaultChunkSize)
	io.Copy(w, strings.NewReader("hello world"))
	w.Close()

	r, _ := NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	p := make([]byte, 5)
	r.ReadAt(p, 6)
	fmt.Printf("%s\n", p)
	// Output: world
}
