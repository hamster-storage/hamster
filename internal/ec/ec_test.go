package ec

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"math/bits"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/hamster-storage/hamster/internal/meta"
)

func init() {
	// Small slices so a few kilobytes exercise many stripes. Read paths
	// take the slice size from shard headers, so this is write-side only.
	sliceSize = 1024
}

var testID = meta.VersionID{0xDA, 0x7A, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

// encode erasure-codes payload (as its own frame stand-in: the engine
// treats frame bytes as opaque) and returns the shard files and their
// checksums.
func encode(t *testing.T, k, m int, payload []byte) ([][]byte, [][]byte) {
	t.Helper()
	bufs := make([]bytes.Buffer, k+m)
	sinks := make([]io.Writer, k+m)
	for i := range bufs {
		sinks[i] = &bufs[i]
	}
	w, err := NewWriter(testID, k, m, int64(len(payload)), sinks)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	shards := make([][]byte, k+m)
	for i := range bufs {
		shards[i] = bufs[i].Bytes()
	}
	return shards, w.Checksums()
}

// readers wraps shard files as the Reader's input, dropping the indices
// in missing.
func readers(shards [][]byte, missing ...int) []io.ReaderAt {
	rs := make([]io.ReaderAt, len(shards))
	for i, s := range shards {
		rs[i] = bytes.NewReader(s)
	}
	for _, i := range missing {
		rs[i] = nil
	}
	return rs
}

// decodeAll reads the whole payload back through a Reader.
func decodeAll(rs []io.ReaderAt) ([]byte, error) {
	r, err := NewReader(rs)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(io.NewSectionReader(r, 0, r.FrameSize()))
}

func TestProfileLadder(t *testing.T) {
	for _, tc := range []struct {
		nodes int
		want  string
	}{{1, "1+0"}, {2, "1+1"}, {3, "2+1"}, {4, "2+1"}, {5, "3+2"}, {6, "4+2"}, {15, "4+2"}, {100, "4+2"}} {
		if got := AutoProfile(tc.nodes).Name; got != tc.want {
			t.Errorf("AutoProfile(%d) = %s, want %s", tc.nodes, got, tc.want)
		}
	}
	p, err := ProfileByName("4+2")
	if err != nil || p.Data != 4 || p.Parity != 2 || p.Nodes() != 6 {
		t.Fatalf("ProfileByName(4+2) = %+v, %v", p, err)
	}
	if _, err := ProfileByName("13+5"); err == nil {
		t.Error("an untested k+m was accepted as a profile")
	}

	// The small-object rule: below the threshold k drops to 1, parity
	// stays — replication with the profile's tolerance.
	if k, m := p.Params(SmallObjectThreshold - 1); k != 1 || m != 2 {
		t.Errorf("small object under 4+2: %d+%d, want 1+2", k, m)
	}
	if k, m := p.Params(SmallObjectThreshold); k != 4 || m != 2 {
		t.Errorf("threshold object under 4+2: %d+%d, want 4+2", k, m)
	}
	if k, m := Profiles[0].Params(1); k != 1 || m != 0 {
		t.Errorf("small object under 1+0: %d+%d, want 1+0", k, m)
	}
}

// TestGoldenShardHeader pins the shard file front. Breaking this breaks
// every shard already on disk — invariant 2.
func TestGoldenShardHeader(t *testing.T) {
	got := hex.EncodeToString(encodeShard(shardHeader{
		id: testID, index: 5, k: 4, m: 2, sliceSize: 256 << 10, frameSize: 123456,
	}))
	const want = "484d53312200000008011210da7a0102030405060708090a0b0c0d0e1805200428023080801038c0c407"
	if got != want {
		t.Fatalf("golden shard header diverged:\n got %s\nwant %s", got, want)
	}
}

// TestRoundTripAllLossPatterns round-trips every profile at sizes around
// every stripe boundary, then re-reads under every survivable loss
// pattern — every subset of at most m shards missing — and demands the
// exact bytes back. This is the durability claim, enumerated.
func TestRoundTripAllLossPatterns(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 1))
	for _, p := range Profiles {
		k, m := p.Data, p.Parity
		stripe := int(int64(k) * sliceSize)
		for _, size := range []int{1, 100, stripe - 1, stripe, stripe + 1, 3*stripe + 777} {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(rng.Uint32())
			}
			shards, _ := encode(t, k, m, payload)

			// Deterministic: the same input yields identical shards.
			again, _ := encode(t, k, m, payload)
			for i := range shards {
				if !bytes.Equal(shards[i], again[i]) {
					t.Fatalf("%s size %d: shard %d not deterministic", p.Name, size, i)
				}
			}

			// All shard files are the same length (equal slices, equal
			// headers at single-byte indices).
			for i := range shards {
				if len(shards[i]) != len(shards[0]) {
					t.Fatalf("%s size %d: shard %d is %d bytes, shard 0 is %d",
						p.Name, size, i, len(shards[i]), len(shards[0]))
				}
			}

			for mask := 0; mask < 1<<(k+m); mask++ {
				if bits.OnesCount(uint(mask)) > m {
					continue
				}
				var missing []int
				for i := range k + m {
					if mask&(1<<i) != 0 {
						missing = append(missing, i)
					}
				}
				got, err := decodeAll(readers(shards, missing...))
				if err != nil {
					t.Fatalf("%s size %d missing %v: %v", p.Name, size, missing, err)
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("%s size %d missing %v: wrong bytes back", p.Name, size, missing)
				}
			}
		}
	}
}

// TestBeyondToleranceRefused: losing more than m shards is unreadable,
// stated loudly — never garbage.
func TestBeyondToleranceRefused(t *testing.T) {
	payload := bytes.Repeat([]byte("hamster"), 1000)
	shards, _ := encode(t, 4, 2, payload)
	if _, err := decodeAll(readers(shards, 0, 3, 5)); err == nil || !strings.Contains(err.Error(), "unreadable") {
		t.Fatalf("3 of 6 shards missing: err %v, want an 'unreadable' refusal", err)
	}
}

// TestRangeReadsUnderLoss: ReadAt against the original bytes at every
// offset, healthy and degraded.
func TestRangeReadsUnderLoss(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 14))
	payload := make([]byte, 3*4*int(sliceSize)+555) // 3.x stripes at 4+2
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	shards, _ := encode(t, 4, 2, payload)
	for _, missing := range [][]int{nil, {1}, {0, 4}, {4, 5}} {
		r, err := NewReader(readers(shards, missing...))
		if err != nil {
			t.Fatal(err)
		}
		if r.FrameSize() != int64(len(payload)) || r.DataID() != testID {
			t.Fatalf("missing %v: FrameSize %d id %x", missing, r.FrameSize(), r.DataID())
		}
		for off := 0; off < len(payload); off += 211 {
			for _, length := range []int{1, 200, int(sliceSize) + 13, len(payload) - off} {
				if off+length > len(payload) {
					continue
				}
				p := make([]byte, length)
				if n, err := r.ReadAt(p, int64(off)); err != nil || n != length {
					t.Fatalf("missing %v ReadAt(%d,%d) = %d, %v", missing, off, length, n, err)
				}
				if !bytes.Equal(p, payload[off:off+length]) {
					t.Fatalf("missing %v ReadAt(%d,%d): wrong bytes", missing, off, length)
				}
			}
		}
		if n, err := r.ReadAt(make([]byte, 8), int64(len(payload))-3); err != io.EOF || n != 3 {
			t.Fatalf("read straddling end = %d, %v; want 3, EOF", n, err)
		}
	}
}

// TestReconstructRebuildsBitIdentical: repair's rebuild produces the
// exact shard files the writer produced, verified against the metadata
// checksums in both directions.
func TestReconstructRebuildsBitIdentical(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 9))
	payload := make([]byte, 2*4*int(sliceSize)+99)
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	shards, sums := encode(t, 4, 2, payload)

	for _, missing := range [][]int{{2}, {0, 5}, {4, 5}} {
		rebuild := make([]io.Writer, len(shards))
		outs := make([]bytes.Buffer, len(shards))
		for _, i := range missing {
			rebuild[i] = &outs[i]
		}
		if err := Reconstruct(readers(shards, missing...), sums, rebuild); err != nil {
			t.Fatalf("missing %v: %v", missing, err)
		}
		for _, i := range missing {
			if !bytes.Equal(outs[i].Bytes(), shards[i]) {
				t.Fatalf("missing %v: rebuilt shard %d differs from the original", missing, i)
			}
		}
	}
}

// TestReconstructRefusesCorruptSource: a survivor whose bytes do not
// match its recorded checksum must fail the rebuild — repair never
// launders corruption into fresh shards.
func TestReconstructRefusesCorruptSource(t *testing.T) {
	payload := bytes.Repeat([]byte("stash"), 2000)
	shards, sums := encode(t, 4, 2, payload)
	corrupt := make([][]byte, len(shards))
	for i := range shards {
		corrupt[i] = bytes.Clone(shards[i])
	}
	corrupt[1][len(corrupt[1])-7] ^= 0x01 // payload corruption, not header

	rebuild := make([]io.Writer, len(shards))
	rebuild[3] = &bytes.Buffer{}
	err := Reconstruct(readers(corrupt, 3), sums, rebuild)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("corrupt source: err %v, want a checksum refusal", err)
	}

	// Reconstruct refuses a slot that is both source and target.
	rebuild[1] = &bytes.Buffer{}
	if err := Reconstruct(readers(shards, 3), sums, rebuild); err == nil {
		t.Fatal("a shard offered as both source and rebuild target was accepted")
	}

	// And a 1+0 object has nothing to rebuild from.
	one, oneSums := encode(t, 1, 0, payload)
	if err := Reconstruct(readers(one), oneSums, make([]io.Writer, 1)); err == nil ||
		!strings.Contains(err.Error(), "redundancy") {
		t.Fatalf("1+0 rebuild: err %v, want a 'no redundancy' refusal", err)
	}
}

// TestK1ParityIsFullCopy documents the small-object claim from
// docs/ERASURE-CODING.md: Reed-Solomon with one data shard is
// replication — every parity shard's payload is byte-identical to the
// data shard's.
func TestK1ParityIsFullCopy(t *testing.T) {
	payload := bytes.Repeat([]byte("tiny"), 300)
	shards, _ := encode(t, 1, 2, payload)
	// Same index-width headers, so equal prefixes aside; compare payloads.
	_, off, err := decodeShard(bytes.NewReader(shards[0]))
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 3; i++ {
		if !bytes.Equal(shards[i][off:], shards[0][off:]) {
			t.Fatalf("k=1 parity shard %d is not a full copy of the data shard", i)
		}
	}
}

// TestMixedShardSetsRefused: shards from different objects, or a shard
// file sitting at the wrong index, are refused at open.
func TestMixedShardSetsRefused(t *testing.T) {
	a, _ := encode(t, 2, 1, []byte("object a payload"))
	bID := testID
	bID[0] ^= 0xFF
	bufs := make([]bytes.Buffer, 3)
	w, err := NewWriter(bID, 2, 1, 16, []io.Writer{&bufs[0], &bufs[1], &bufs[2]})
	if err != nil {
		t.Fatal(err)
	}
	w.Write([]byte("object b payload"))
	w.Close()

	mixed := readers(a)
	mixed[2] = bytes.NewReader(bufs[2].Bytes())
	if _, err := NewReader(mixed); err == nil || !strings.Contains(err.Error(), "mixed") {
		t.Fatalf("mixed shard sets: err %v, want a 'mixed' refusal", err)
	}

	swapped := readers(a)
	swapped[0], swapped[1] = swapped[1], swapped[0]
	if _, err := NewReader(swapped); err == nil || !strings.Contains(err.Error(), "misplaced") {
		t.Fatalf("swapped shards: err %v, want a 'misplaced' refusal", err)
	}
}

// TestWriterContract: every way the writer could be misused is refused.
func TestWriterContract(t *testing.T) {
	sinks := func(n int) []io.Writer {
		s := make([]io.Writer, n)
		for i := range s {
			s[i] = io.Discard
		}
		return s
	}
	if _, err := NewWriter(testID, 5, 0, 10, sinks(5)); err == nil {
		t.Error("5+0 (striping without parity) accepted")
	}
	if _, err := NewWriter(testID, 4, 2, 0, sinks(6)); err == nil {
		t.Error("an empty frame accepted")
	}
	if _, err := NewWriter(testID, 4, 2, 10, sinks(5)); err == nil {
		t.Error("wrong sink count accepted")
	}
	w, err := NewWriter(testID, 2, 1, 4, sinks(3))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hello")); err == nil {
		t.Error("write past the declared frame size accepted")
	}
	w.Write([]byte("hi"))
	if err := w.Close(); err == nil {
		t.Error("Close with 2 of 4 declared bytes accepted")
	}
	if _, err := w.Write([]byte("!")); err == nil {
		t.Error("write after Close accepted")
	}
}

func ExampleWriter() {
	payload := []byte("six shards, any four reassemble me")
	bufs := make([]bytes.Buffer, 6)
	sinks := make([]io.Writer, 6)
	for i := range bufs {
		sinks[i] = &bufs[i]
	}
	w, _ := NewWriter(testID, 4, 2, int64(len(payload)), sinks)
	w.Write(payload)
	w.Close()

	// Two nodes die; their shards are gone.
	shards := make([]io.ReaderAt, 6)
	for i := range bufs {
		if i != 1 && i != 4 {
			shards[i] = bytes.NewReader(bufs[i].Bytes())
		}
	}
	r, _ := NewReader(shards)
	got := make([]byte, r.FrameSize())
	r.ReadAt(got, 0)
	fmt.Printf("%s\n", got)
	// Output: six shards, any four reassemble me
}
