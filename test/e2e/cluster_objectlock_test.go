//go:build e2e

package e2e

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestClusterObjectLock proves object lock works over the erasure-coded cluster
// path: a lock-enabled bucket, an object stored with COMPLIANCE retention via the
// x-amz-object-lock-* PUT headers (threaded through coord.Put), the lock state on
// the cluster GET, and — invariant 4 — the COMPLIANCE-locked version refusing
// deletion across the cluster.
func TestClusterObjectLock(t *testing.T) {
	env := []string{"HAMSTER_ACCESS_KEY_ID=e2e-lock", "HAMSTER_SECRET_ACCESS_KEY=e2e-lock-secret"}
	cl := startCluster(t, "e2e-lock", 3, env)
	c := &s3Client{t: t, akid: "e2e-lock", secret: "e2e-lock-secret", region: "us-east-1"}

	// A bucket with object lock (enables versioning).
	c.writeH(cl, "PUT", "/vault", nil, map[string]string{"x-amz-bucket-object-lock-enabled": "true"}, http.StatusOK)

	// An erasure-coded object under COMPLIANCE retention to a far-future date.
	body := make([]byte, 200<<10)
	for i := range body {
		body[i] = byte(i * 5)
	}
	put := c.writeH(cl, "PUT", "/vault/locked", body, map[string]string{
		"x-amz-object-lock-mode":              "COMPLIANCE",
		"x-amz-object-lock-retain-until-date": "2099-01-01T00:00:00Z",
	}, http.StatusOK)
	vid := put.Header.Get("x-amz-version-id")
	if vid == "" {
		t.Fatal("locked PUT returned no version id")
	}

	// It reads back through the data path, carrying its lock headers.
	c.getEventually(cl.alive(), "/vault/locked", body)
	g, _ := c.do(cl.leaderS3(), "GET", "/vault/locked", nil)
	if g == nil || g.Header.Get("x-amz-object-lock-mode") != "COMPLIANCE" || g.Header.Get("x-amz-object-lock-retain-until-date") == "" {
		t.Fatalf("lock headers on cluster GET: %v", g)
	}

	// Invariant 4 over the cluster: the COMPLIANCE-locked version cannot be deleted.
	c.expectStatus(cl, "DELETE", "/vault/locked?"+canonicalQuery(map[string]string{"versionId": vid}), http.StatusForbidden)
}

// doH is do with caller-supplied headers, set after signing — SigV4 signs only
// host/date/content-sha256 here, so unsigned x-amz-* headers (the object-lock
// ones, the bucket-lock-enabled flag) pass through to the server unverified.
func (c *s3Client) doH(addr, method, path string, body []byte, hdrs map[string]string) (*http.Response, []byte) {
	c.t.Helper()
	req, err := http.NewRequest(method, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	c.sign(req, body)
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		return nil, nil
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, respBody
}

// writeH is a header-bearing write to whichever node commits it (the leader;
// non-leaders answer 503), returning that response.
func (c *s3Client) writeH(cl *cluster, method, path string, body []byte, hdrs map[string]string, want int) *http.Response {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		for _, addr := range cl.alive() {
			resp, rb := c.doH(addr, method, path, body, hdrs)
			if resp == nil {
				continue
			}
			if resp.StatusCode == want {
				return resp
			}
			if resp.StatusCode == http.StatusServiceUnavailable {
				continue
			}
			c.t.Fatalf("%s %s on %s: status %d, want %d\n%s", method, path, addr, resp.StatusCode, want, rb)
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatalf("%s %s: no node answered %d before the deadline", method, path, want)
	return nil
}
