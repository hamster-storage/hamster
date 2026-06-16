package stream

import (
	"bytes"
	"encoding/hex"
	"io"
	"math/rand/v2"
	"testing"
)

// testDEK is a fixed 32-byte data-encryption key for the encrypted-frame
// tests. Real DEKs are random per object; a fixed one keeps the golden
// deterministic.
func testDEK() []byte {
	dek := make([]byte, DEKLen)
	for i := range dek {
		dek[i] = byte(i + 1)
	}
	return dek
}

// frameEnc writes payload into an AES-256-GCM frame under dek and returns
// its bytes, splitting the writes at cuts like frame does.
func frameEnc(t *testing.T, payload []byte, chunkSize int, dek []byte, cuts ...int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := NewWriter(&buf, int64(len(payload)), chunkSize, dek)
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

// readAllEnc reads the whole plaintext back out of an encrypted frame.
func readAllEnc(f, dek []byte) ([]byte, error) {
	r, err := NewReader(bytes.NewReader(f), int64(len(f)), dek)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(io.NewSectionReader(r, 0, r.Size()))
}

// TestEncryptedGoldenFrame pins the exact encoding of a small AES-256-GCM
// frame. Like TestGoldenFrame, a break here means the on-disk encrypted
// format changed — invariant 2, not a casual update. The ciphertext is
// deterministic: fixed key, the chunk-index nonce, fixed plaintext.
func TestEncryptedGoldenFrame(t *testing.T) {
	got := hex.EncodeToString(frameEnc(t, []byte("hamster-ok"), 4, testDEK()))
	// header HMF1 v1 flags=02(encrypted) chunk=4 plaintext=10, then three
	// sealed chunks (4+16, 4+16, 2+16 stored bytes), then the trailer:
	// stored-length uvarints 14 14 12, three CRC-32C, trailer length 0f.
	const want = "484d46310102040a" +
		"ae18b66ac852b1d7c09bbfcb1f910c42d98bdcc9" +
		"d8ad98917f7d038ad1ddef259acbbb7e0cf36748" +
		"0740b997a29687e9ba412cbf9b7590f70e10" +
		"141412d0bebd368eeb07e83b8cd6c70f000000"
	if got != want {
		t.Fatalf("golden encrypted frame diverged:\n got %s\nwant %s", got, want)
	}
}

// TestEncryptedRoundTrip mirrors TestRoundTrip for encrypted frames:
// randomized payloads, chunk sizes, and write segmentations through a
// frame and back, with FrameSize(encrypted=true) predicting the size.
func TestEncryptedRoundTrip(t *testing.T) {
	dek := testDEK()
	rng := rand.New(rand.NewPCG(0xE1, 0xC0DE))
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
			f := frameEnc(t, payload, chunk, dek, cuts...)
			if want := FrameSize(int64(size), chunk, true); want != int64(len(f)) {
				t.Fatalf("size %d chunk %d: FrameSize predicts %d, frame is %d bytes", size, chunk, want, len(f))
			}
			got, err := readAllEnc(f, dek)
			if err != nil {
				t.Fatalf("size %d chunk %d: %v", size, chunk, err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("size %d chunk %d: round trip diverged", size, chunk)
			}
			// Deterministic: same key, same input, same bytes.
			if !bytes.Equal(f, frameEnc(t, payload, chunk, dek)) {
				t.Fatalf("size %d chunk %d: encrypted frame is not deterministic", size, chunk)
			}
		}
	}
}

// TestEncryptedFrameOverhead documents the encrypted frame's cost over the
// identity frame: exactly one GCM tag per chunk and no more.
func TestEncryptedFrameOverhead(t *testing.T) {
	for _, tc := range []struct {
		size, chunk int
	}{{0, 4}, {1, 4}, {10, 4}, {1000, 256}, {65536, 1024}} {
		plain := FrameSize(int64(tc.size), tc.chunk, false)
		enc := FrameSize(int64(tc.size), tc.chunk, true)
		n := chunkCount(int64(tc.size), int64(tc.chunk))
		// The trailer length uvarints can grow by a byte when the stored
		// chunk size crosses a 0x80 boundary, so the body+tag delta is the
		// exact part to pin; bound the whole delta sanely.
		if enc < plain+gcmTagLen*n {
			t.Errorf("size %d chunk %d: encrypted frame %d < identity %d + %d tags", tc.size, tc.chunk, enc, plain, n)
		}
		if enc > plain+gcmTagLen*n+n+8 {
			t.Errorf("size %d chunk %d: encrypted frame %d exceeds identity %d + tags + slack", tc.size, tc.chunk, enc, plain)
		}
	}
}

// TestEncryptedRangeReads checks random-access reads decrypt correctly:
// only the touched chunks are read, each authenticated, every offset and
// length matching the source.
func TestEncryptedRangeReads(t *testing.T) {
	dek := testDEK()
	rng := rand.New(rand.NewPCG(31, 41))
	payload := make([]byte, 10_000)
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	f := frameEnc(t, payload, 1000, dek)
	r, err := NewReader(bytes.NewReader(f), int64(len(f)), dek)
	if err != nil {
		t.Fatal(err)
	}
	if r.Size() != int64(len(payload)) {
		t.Fatalf("Size %d, want %d", r.Size(), len(payload))
	}
	for off := 0; off <= len(payload); off += 7 {
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
}

// TestEncryptedCover proves Cover(encrypted=true) names enough ranges to
// decrypt the requested plaintext — the network read path's contract for
// encrypted frames, where stored chunks are wider than their plaintext.
func TestEncryptedCover(t *testing.T) {
	dek := testDEK()
	const chunk = 256
	for _, size := range []int{0, 1, chunk - 1, chunk, chunk + 1, 5*chunk + 17} {
		rng := rand.New(rand.NewPCG(uint64(size), 0xEC))
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(rng.UintN(256))
		}
		f := frameEnc(t, data, chunk, dek)

		reads := [][2]int64{{0, int64(size)}, {0, 0}, {int64(size), 10}}
		for range 20 {
			off := rng.Int64N(int64(size) + 1)
			reads = append(reads, [2]int64{off, rng.Int64N(int64(size) - off + 1)})
		}
		for _, rd := range reads {
			off, n := rd[0], rd[1]
			cov := Cover(int64(size), chunk, off, n, true)
			r, err := NewReader(&gappedReaderAt{t: t, frame: f, ranges: cov}, int64(len(f)), dek)
			if err != nil {
				t.Fatalf("size %d read [%d,%d): NewReader: %v", size, off, off+n, err)
			}
			got := make([]byte, n)
			if n > 0 && off+n <= int64(size) {
				if _, err := r.ReadAt(got, off); err != nil {
					t.Fatalf("size %d read [%d,%d): %v", size, off, off+n, err)
				}
				if !bytes.Equal(got, data[off:off+n]) {
					t.Fatalf("size %d read [%d,%d): wrong bytes", size, off, off+n)
				}
			}
		}
	}
}

// TestEncryptedTamperDetected flips every byte of an encrypted frame and
// demands the change is caught — by the CRC, the GCM tag, or a structural
// check — but never served as plaintext. Authenticated encryption makes
// every chunk byte tamper-evident on top of the CRC.
func TestEncryptedTamperDetected(t *testing.T) {
	dek := testDEK()
	payload := []byte("the quick brown hamster stuffs its cheeks full")
	f := frameEnc(t, payload, 16, dek)
	for i := range f {
		corrupt := bytes.Clone(f)
		corrupt[i] ^= 0xFF
		if got, err := readAllEnc(corrupt, dek); err == nil {
			t.Errorf("flipping byte %d of %d went undetected (read %q)", i, len(f), got)
		}
	}
}

// TestWrongKeyRejected: a frame decrypted with the wrong DEK fails
// authentication rather than returning garbage. The CRC passes (the
// ciphertext is intact); the GCM tag is what rejects the wrong key.
func TestWrongKeyRejected(t *testing.T) {
	payload := []byte("secret hamster business, eyes only")
	f := frameEnc(t, payload, 8, testDEK())

	wrong := testDEK()
	wrong[0] ^= 0x01
	if got, err := readAllEnc(f, wrong); err == nil {
		t.Errorf("wrong key accepted, read %q", got)
	}
	// The right key still works.
	got, err := readAllEnc(f, testDEK())
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("right key: %q, %v", got, err)
	}
}

// TestEncryptedConfidentiality is a smoke check that the plaintext does not
// appear verbatim in the frame: the body is ciphertext, not the bytes we
// fed in.
func TestEncryptedConfidentiality(t *testing.T) {
	secret := []byte("CONFIDENTIAL-HAMSTER-PAYLOAD-DO-NOT-LEAK")
	f := frameEnc(t, secret, 8, testDEK())
	if bytes.Contains(f, secret) {
		t.Error("plaintext appears verbatim in the encrypted frame")
	}
}

// TestDEKValidation: a key that is not exactly DEKLen bytes is refused by
// both the writer and the reader, rather than producing a broken frame.
func TestDEKValidation(t *testing.T) {
	for _, bad := range [][]byte{make([]byte, 0), make([]byte, 16), make([]byte, 31), make([]byte, 33)} {
		if _, err := NewWriter(io.Discard, 4, 16, bad); err == nil {
			t.Errorf("NewWriter accepted a %d-byte key", len(bad))
		}
	}
	f := frameEnc(t, []byte("data"), 4, testDEK())
	for _, bad := range [][]byte{make([]byte, 16), make([]byte, 31), make([]byte, 33)} {
		if _, err := NewReader(bytes.NewReader(f), int64(len(f)), bad); err == nil {
			t.Errorf("NewReader accepted a %d-byte key", len(bad))
		}
	}
}

// TestIdentityFrameIgnoresKey: an identity frame read with a key supplied
// is read normally — the header says identity, so the key is unused, not
// an error. (The data path may hold a cluster key while reading objects
// written before encryption was enabled.)
func TestIdentityFrameIgnoresKey(t *testing.T) {
	payload := []byte("plain bytes, no encryption")
	f := frame(t, payload, 8)
	r, err := NewReader(bytes.NewReader(f), int64(len(f)), testDEK())
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(io.NewSectionReader(r, 0, r.Size()))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("identity frame with key supplied: %q, %v", got, err)
	}
}

// TestEncryptedNonceIsChunkIndex documents the property the nonce scheme
// relies on: two chunks of the same plaintext under the same key encrypt
// differently, because the nonce is the chunk index. (If they were equal,
// the nonce was reused — a GCM break.)
func TestEncryptedNonceIsChunkIndex(t *testing.T) {
	dek := testDEK()
	// Two identical chunks: chunkSize 4, payload "aaaaaaaa" → chunk 0 and
	// chunk 1 hold identical plaintext.
	f := frameEnc(t, []byte("aaaaaaaa"), 4, dek)
	r, err := NewReader(bytes.NewReader(f), int64(len(f)), dek)
	if err != nil {
		t.Fatal(err)
	}
	c0, err := r.readChunk(0)
	if err != nil {
		t.Fatal(err)
	}
	c1, err := r.readChunk(1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(c0, c1) {
		t.Fatal("decrypted chunks should match (same plaintext)")
	}
	// But the stored ciphertext for the two chunks must differ.
	stored0 := f[r.offsets[0]:r.offsets[1]]
	stored1 := f[r.offsets[1]:r.offsets[2]]
	if bytes.Equal(stored0, stored1) {
		t.Error("identical plaintext chunks produced identical ciphertext: nonce reuse")
	}
}
