//go:build e2e

package e2e

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestClusterVersioning exercises the v0.5 versioning surface over the real
// erasure-coded cluster data path: per-bucket versioning config, version IDs on
// PUT, by-version GET (which fetches a specific version's shards), the delete
// marker, ListObjectVersions, and permanent version delete (which frees shards).
func TestClusterVersioning(t *testing.T) {
	env := []string{"HAMSTER_ACCESS_KEY_ID=e2e-ver", "HAMSTER_SECRET_ACCESS_KEY=e2e-ver-secret"}
	cl := startCluster(t, "e2e-ver", 3, env)
	c := &s3Client{t: t, akid: "e2e-ver", secret: "e2e-ver-secret", region: "us-east-1"}

	c.mutate(cl.alive(), "PUT", "/vault", nil, http.StatusOK)
	enable := []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)
	c.mutate(cl.alive(), "PUT", "/vault?"+canonicalQuery(map[string]string{"versioning": ""}), enable, http.StatusOK)

	// Two erasure-coded versions of one key (> 128 KiB, so each shards across nodes).
	v1 := make([]byte, 200<<10)
	v2 := make([]byte, 200<<10)
	for i := range v1 {
		v1[i] = byte(i)
		v2[i] = byte(i * 3)
	}
	vid1 := c.writeOK(cl.alive(), "PUT", "/vault/obj", v1, http.StatusOK).Header.Get("x-amz-version-id")
	vid2 := c.writeOK(cl.alive(), "PUT", "/vault/obj", v2, http.StatusOK).Header.Get("x-amz-version-id")
	if vid1 == "" || vid2 == "" || vid1 == vid2 {
		t.Fatalf("version ids %q, %q", vid1, vid2)
	}

	byID := func(vid string) string {
		return "/vault/obj?" + canonicalQuery(map[string]string{"versionId": vid})
	}

	// Current read is v2; each version reads by id through the EC data path.
	c.getEventually(cl.alive(), "/vault/obj", v2)
	c.getEventually(cl.alive(), byID(vid1), v1)
	c.getEventually(cl.alive(), byID(vid2), v2)

	// ListObjectVersions over the cluster shows both versions.
	resp, body := c.do(cl.leaderS3(), "GET", "/vault?"+canonicalQuery(map[string]string{"versions": ""}), nil)
	if resp == nil || resp.StatusCode != http.StatusOK || strings.Count(string(body), "<Version>") != 2 {
		t.Fatalf("list versions: %v\n%s", resp, body)
	}

	// A delete marker hides the current version; old versions still read.
	c.mutate(cl.alive(), "DELETE", "/vault/obj", nil, http.StatusNoContent)
	c.expectStatus(cl, "GET", "/vault/obj", http.StatusNotFound)
	c.getEventually(cl.alive(), byID(vid2), v2)

	// Permanent delete frees v1's shards; it is then gone.
	c.mutate(cl.alive(), "DELETE", byID(vid1), nil, http.StatusNoContent)
	c.expectStatus(cl, "GET", byID(vid1), http.StatusNotFound)
}

// writeOK sends a write to whichever node commits it (the leader; non-leaders
// answer 503) and returns that successful response, so the caller can read its
// headers — a version id, say.
func (c *s3Client) writeOK(addrs []string, method, path string, body []byte, want int) *http.Response {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		for _, addr := range addrs {
			resp, respBody := c.do(addr, method, path, body)
			if resp == nil {
				continue
			}
			if resp.StatusCode == want {
				return resp
			}
			if resp.StatusCode == http.StatusServiceUnavailable {
				continue
			}
			c.t.Fatalf("%s %s on %s: status %d, want %d\n%s", method, path, addr, resp.StatusCode, want, respBody)
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatalf("%s %s: no node answered %d before the deadline", method, path, want)
	return nil
}

// expectStatus polls the current leader until one request returns want — the
// strongly consistent view, re-resolving the leader each round in case it moved.
func (c *s3Client) expectStatus(cl *cluster, method, path string, want int) {
	c.t.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		resp, _ := c.do(cl.leaderS3(), method, path, nil)
		if resp != nil && resp.StatusCode == want {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatalf("%s %s: leader never returned %d before the deadline", method, path, want)
}
