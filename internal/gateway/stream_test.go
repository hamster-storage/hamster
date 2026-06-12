package gateway_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
)

// TestStreamedPutRoundTrip pushes a body several times the write buffer
// through the full HTTP path: the blob must arrive intact and the ETag must
// be the MD5 computed on the streaming pass (ADR-0019).
func TestStreamedPutRoundTrip(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	body := make([]byte, 3<<20+17) // not a whole number of buffers
	rng := rand.New(rand.NewPCG(7, 0))
	for i := range body {
		body[i] = byte(rng.Uint32())
	}

	resp := e.do("PUT", "/bkt/big", body, nil)
	wantETag := md5.Sum(body)
	if etag := resp.Header.Get("ETag"); etag != `"`+hex.EncodeToString(wantETag[:])+`"` {
		t.Fatalf("streamed ETag %s", etag)
	}
	e.expect(resp, 200)
	if got := e.expect(e.do("GET", "/bkt/big", nil, nil), 200); !bytes.Equal(got, body) {
		t.Fatalf("streamed body damaged: %d bytes back, want %d", len(got), len(body))
	}

	// A zero-length object is a real object.
	e.expect(e.do("PUT", "/bkt/empty", nil, nil), 200)
	if got := e.expect(e.do("GET", "/bkt/empty", nil, nil), 200); len(got) != 0 {
		t.Fatalf("empty object came back as %q", got)
	}
}

// TestBodyValidationLeavesNoOrphans pins the cleanup contract of the
// streaming path: validation now runs after the bytes are staged on disk,
// so a failed request must remove them — a rejected PUT leaves nothing.
func TestBodyValidationLeavesNoOrphans(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	// Declared SHA-256 covers different bytes (TestDeclaredHashMismatch
	// proves the status; here we prove the disk stayed clean).
	r, _ := http.NewRequest("PUT", e.srv.URL+"/bkt/k", strings.NewReader("tampered"))
	r.Host = r.URL.Host
	declared := sha256.Sum256([]byte("original"))
	signRequest(r, hex.EncodeToString(declared[:]))
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	if code := e.errorCode(resp, 400); code != "XAmzContentSHA256Mismatch" {
		t.Fatalf("hash mismatch: %s", code)
	}
	if n := e.blobCount(); n != 0 {
		t.Fatalf("%d blobs on disk after rejected payload hash, want 0", n)
	}

	// Content-MD5 of different bytes.
	wrong := md5.Sum([]byte("other"))
	resp = e.do("PUT", "/bkt/k", []byte("payload"),
		map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(wrong[:])})
	if code := e.errorCode(resp, 400); code != "BadDigest" {
		t.Fatalf("md5 mismatch: %s", code)
	}
	if n := e.blobCount(); n != 0 {
		t.Fatalf("%d blobs on disk after rejected Content-MD5, want 0", n)
	}

	// Sanity: the same key still accepts a clean PUT afterwards.
	e.expect(e.do("PUT", "/bkt/k", []byte("payload"), nil), 200)
	if n := e.blobCount(); n != 1 {
		t.Fatalf("%d blobs on disk after clean PUT, want 1", n)
	}
}

// TestControlBodyCap: the XML control bodies (DeleteObjects here) are read
// whole and must be bounded — an oversized one is rejected, not buffered
// without limit.
func TestControlBodyCap(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	huge := bytes.Repeat([]byte("x"), 4<<20+1)
	if code := e.errorCode(e.do("POST", "/bkt?delete", huge, nil), 400); code != "EntityTooLarge" {
		t.Fatalf("oversized control body: %s", code)
	}
}
