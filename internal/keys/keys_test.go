package keys

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"io"
	"testing"

	"github.com/hamster-storage/hamster/internal/stream"
)

// seqReader is a deterministic entropy source for tests: a counting byte
// stream, so DEK generation is reproducible and inspectable.
type seqReader struct{ n byte }

func (r *seqReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.n
		r.n++
	}
	return len(p), nil
}

// testKEKMaterial is a fixed 32-byte key for the wrap/unwrap tests.
func testKEKMaterial() []byte {
	m := make([]byte, KEKLen)
	for i := range m {
		m[i] = byte(0x40 + i)
	}
	return m
}

func mustKEK(t *testing.T, material []byte) KEK {
	t.Helper()
	k, err := LoadKEK(material)
	if err != nil {
		t.Fatalf("LoadKEK: %v", err)
	}
	return k
}

// testNonce builds a 12-byte wrap nonce from a counter, standing in for
// the version-ID-derived nonce the coordinator will supply.
func testNonce(i byte) []byte {
	n := make([]byte, wrapNonceLen)
	n[0] = i
	return n
}

// TestNewDEK: a DEK is exactly DEKLen bytes drawn from the entropy source,
// deterministic for a deterministic source, and distinct chunk to chunk.
func TestNewDEK(t *testing.T) {
	r := &seqReader{}
	d1, err := NewDEK(r)
	if err != nil {
		t.Fatal(err)
	}
	d2, err := NewDEK(r)
	if err != nil {
		t.Fatal(err)
	}
	if d1 == d2 {
		t.Error("two DEKs from a stream should differ")
	}
	// Reproducible from the same seed.
	r2 := &seqReader{}
	d1again, _ := NewDEK(r2)
	if d1 != d1again {
		t.Error("DEK from the same deterministic source diverged")
	}
	// crypto/rand works as the production source.
	if _, err := NewDEK(rand.Reader); err != nil {
		t.Fatalf("NewDEK(crypto/rand): %v", err)
	}
}

// TestNewDEKShortEntropy: a source that cannot supply a full DEK is an
// error, not a short key.
func TestNewDEKShortEntropy(t *testing.T) {
	if _, err := NewDEK(bytes.NewReader(make([]byte, DEKLen-1))); err == nil {
		t.Error("short entropy accepted")
	}
}

// TestLoadKEKEncodings: raw bytes, hex, and base64 of the same key all
// load to the same working KEK; wrong lengths and garbage are refused.
func TestLoadKEKEncodings(t *testing.T) {
	raw := testKEKMaterial()
	forms := [][]byte{
		raw,
		[]byte(hex.EncodeToString(raw)),
		[]byte(base64.StdEncoding.EncodeToString(raw)),
		[]byte("  " + base64.StdEncoding.EncodeToString(raw) + "\n"), // trimmed
	}
	var wrapped [][]byte
	for _, form := range forms {
		k, err := LoadKEK(form)
		if err != nil {
			t.Fatalf("LoadKEK(%q): %v", form, err)
		}
		w, err := k.Wrap(DEK{1, 2, 3}, testNonce(0))
		if err != nil {
			t.Fatal(err)
		}
		wrapped = append(wrapped, w)
	}
	// Every encoding produced the identical KEK, so identical wraps.
	for i := 1; i < len(wrapped); i++ {
		if !bytes.Equal(wrapped[0], wrapped[i]) {
			t.Errorf("encoding %d produced a different KEK", i)
		}
	}

	for _, bad := range [][]byte{nil, make([]byte, 16), make([]byte, 31), make([]byte, 33), []byte("not a key")} {
		if _, err := LoadKEK(bad); err == nil {
			t.Errorf("LoadKEK accepted bad material of len %d", len(bad))
		}
	}
}

// TestWrapUnwrapRoundTrip: a wrapped DEK unwraps to itself, the wrapped
// blob is exactly WrappedLen, and the wrap is deterministic given the same
// (KEK, DEK, nonce).
func TestWrapUnwrapRoundTrip(t *testing.T) {
	k := mustKEK(t, testKEKMaterial())
	dek := DEK{}
	for i := range dek {
		dek[i] = byte(i * 3)
	}
	w, err := k.Wrap(dek, testNonce(7))
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != WrappedLen {
		t.Fatalf("wrapped len %d, want %d", len(w), WrappedLen)
	}
	got, err := k.Unwrap(w)
	if err != nil {
		t.Fatal(err)
	}
	if got != dek {
		t.Error("unwrapped DEK differs from the original")
	}
	// Deterministic.
	w2, _ := k.Wrap(dek, testNonce(7))
	if !bytes.Equal(w, w2) {
		t.Error("wrap is not deterministic for a fixed nonce")
	}
}

// TestWrapDistinctNonces: the same DEK under different nonces produces
// different ciphertext — the property the version-ID nonce guarantees, and
// what keeps reuse from leaking anything.
func TestWrapDistinctNonces(t *testing.T) {
	k := mustKEK(t, testKEKMaterial())
	dek := DEK{9, 9, 9}
	a, _ := k.Wrap(dek, testNonce(1))
	b, _ := k.Wrap(dek, testNonce(2))
	if bytes.Equal(a, b) {
		t.Error("identical DEK under different nonces produced identical wraps")
	}
	// Both still unwrap correctly.
	for _, w := range [][]byte{a, b} {
		if got, err := k.Unwrap(w); err != nil || got != dek {
			t.Fatalf("unwrap: %v, %v", got, err)
		}
	}
}

// TestWrongKEKRejected: a wrapped DEK does not unwrap under a different
// KEK — authentication fails rather than yielding a garbage key.
func TestWrongKEKRejected(t *testing.T) {
	k := mustKEK(t, testKEKMaterial())
	other := testKEKMaterial()
	other[0] ^= 0xFF
	wrong := mustKEK(t, other)

	w, _ := k.Wrap(DEK{5, 5, 5}, testNonce(0))
	if _, err := wrong.Unwrap(w); err == nil {
		t.Error("wrong KEK unwrapped the DEK")
	}
}

// TestTamperedWrapRejected: flipping any byte of a wrapped DEK fails
// unwrap. The GCM tag authenticates the whole blob, nonce included.
func TestTamperedWrapRejected(t *testing.T) {
	k := mustKEK(t, testKEKMaterial())
	w, _ := k.Wrap(DEK{1, 1, 1}, testNonce(3))
	for i := range w {
		bad := bytes.Clone(w)
		bad[i] ^= 0xFF
		if _, err := k.Unwrap(bad); err == nil {
			t.Errorf("tampering byte %d of the wrap went undetected", i)
		}
	}
}

// TestWrapInputValidation: a bad nonce length at wrap and a bad blob length
// at unwrap are refused.
func TestWrapInputValidation(t *testing.T) {
	k := mustKEK(t, testKEKMaterial())
	for _, n := range [][]byte{nil, make([]byte, wrapNonceLen-1), make([]byte, wrapNonceLen+1)} {
		if _, err := k.Wrap(DEK{}, n); err == nil {
			t.Errorf("wrap accepted a %d-byte nonce", len(n))
		}
	}
	for _, w := range [][]byte{nil, make([]byte, WrappedLen-1), make([]byte, WrappedLen+1)} {
		if _, err := k.Unwrap(w); err == nil {
			t.Errorf("unwrap accepted a %d-byte blob", len(w))
		}
	}
}

// TestZeroKEKRefuses: the zero KEK (a node that never loaded a key) refuses
// to wrap or unwrap rather than operating without a key.
func TestZeroKEKRefuses(t *testing.T) {
	var k KEK
	if k.Loaded() {
		t.Error("zero KEK reports loaded")
	}
	if _, err := k.Wrap(DEK{}, testNonce(0)); err == nil {
		t.Error("zero KEK wrapped")
	}
	if _, err := k.Unwrap(make([]byte, WrappedLen)); err == nil {
		t.Error("zero KEK unwrapped")
	}
}

// TestDEKThroughStream is the end-to-end check that ties this package to
// the stream transform: a DEK is generated, wrapped, unwrapped, and the
// recovered DEK decrypts a frame the original encrypted. This is the real
// path — mint, wrap, store, later unwrap, read.
func TestDEKThroughStream(t *testing.T) {
	k := mustKEK(t, testKEKMaterial())
	dek, err := NewDEK(&seqReader{n: 0x11})
	if err != nil {
		t.Fatal(err)
	}

	// Encrypt an object with the freshly minted DEK.
	payload := []byte("envelope encryption end to end through the stream layer")
	var buf bytes.Buffer
	w, err := stream.NewWriter(&buf, int64(len(payload)), 16, dek.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Wrap the DEK, discard the plaintext DEK, then recover it from the
	// wrapped blob — as a GET would after reading the VersionEntry.
	wrapped, err := k.Wrap(dek, testNonce(0x22))
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := k.Unwrap(wrapped)
	if err != nil {
		t.Fatal(err)
	}

	r, err := stream.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()), recovered.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(io.NewSectionReader(r, 0, r.Size()))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("object did not survive mint → wrap → unwrap → decrypt")
	}
}
