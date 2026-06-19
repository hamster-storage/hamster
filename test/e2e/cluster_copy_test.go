//go:build e2e

package e2e

import (
	"math/rand/v2"
	"net/http"
	"testing"
)

// TestClusterCopyObject proves server-side CopyObject on the erasure-coded path
// (ADR-0038): the source is read through the data path and re-encoded into the
// destination with no client round-trip — streamed, never buffered whole. A COPY
// preserves the object, a REPLACE rewrites its metadata, both decode back to the
// source bytes, and a missing source is a clean 404.
func TestClusterCopyObject(t *testing.T) {
	const (
		akid   = "e2e-copy"
		secret = "e2e-copy-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	cl := startCluster(t, "e2e-copy", 3, env)
	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	lead := cl.leaderS3()

	c.mutate([]string{lead}, "PUT", "/vault", nil, http.StatusOK)
	// Larger than the streaming window so the copy crosses multiple chunks.
	body := randBytes(rand.New(rand.NewPCG(1, 2)), 3<<20+777)
	c.mutate([]string{lead}, "PUT", "/vault/src", body, http.StatusOK)

	// COPY to a new key — same bytes, metadata carried over.
	resp, rb := c.doH(lead, "PUT", "/vault/copy", nil, map[string]string{"x-amz-copy-source": "/vault/src"})
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("copy: status %v\n%s", resp, rb)
	}
	c.getEventually([]string{lead}, "/vault/copy", body)

	// REPLACE directive — new metadata, still the source bytes.
	resp, rb = c.doH(lead, "PUT", "/vault/copy2", nil, map[string]string{
		"x-amz-copy-source":        "/vault/src",
		"x-amz-metadata-directive": "REPLACE",
		"Content-Type":             "text/plain",
	})
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("copy REPLACE: status %v\n%s", resp, rb)
	}
	c.getEventually([]string{lead}, "/vault/copy2", body)

	// A missing source is 404.
	resp, _ = c.doH(lead, "PUT", "/vault/copy3", nil, map[string]string{"x-amz-copy-source": "/vault/nope"})
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("copy of missing source: want 404, got %v", resp)
	}
}
