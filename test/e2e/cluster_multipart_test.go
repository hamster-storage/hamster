//go:build e2e

package e2e

import (
	"encoding/xml"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestClusterMultipart proves erasure-coded multipart on the cluster S3 path
// (ADR-0038): a real three-node cluster runs the full multipart lifecycle —
// initiate, upload independently erasure-coded parts, complete into one object —
// plus a Range read crossing a part boundary, UploadPartCopy from an existing
// object, and Abort. The compat suite is single-node, so this is the only place
// aws-style multipart runs against a real cluster.
func TestClusterMultipart(t *testing.T) {
	const (
		akid   = "e2e-mp"
		secret = "e2e-mp-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	cl := startCluster(t, "e2e-mp", 3, env)
	c := &s3Client{t: t, akid: akid, secret: secret, region: region}

	// mpDo sends a request to whichever node commits (the leader; non-leaders
	// answer 503), retrying through leadership changes. Returns the response and
	// its body so the caller can read both headers (the part ETag) and XML.
	mpDo := func(method, path string, body []byte, hdrs map[string]string) (*http.Response, []byte) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			for _, addr := range cl.alive() {
				resp, rb := c.doH(addr, method, path, body, hdrs)
				if resp == nil || resp.StatusCode == http.StatusServiceUnavailable {
					continue
				}
				return resp, rb
			}
			time.Sleep(500 * time.Millisecond)
		}
		t.Fatalf("%s %s: no node committed before the deadline", method, path)
		return nil, nil
	}

	initiate := func(key string) string {
		resp, rb := mpDo("POST", "/vault/"+key+"?uploads", nil, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("initiate %s: status %d\n%s", key, resp.StatusCode, rb)
		}
		var res struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.Unmarshal(rb, &res); err != nil || res.UploadID == "" {
			t.Fatalf("initiate %s: parse upload id: %v\n%s", key, err, rb)
		}
		return res.UploadID
	}

	c.mutate([]string{cl.leaderS3()}, "PUT", "/vault", nil, http.StatusOK)

	// --- Basic multipart: two parts, complete, whole + range read. ---
	rng := rand.New(rand.NewPCG(7, 9))
	const minPart = 5 << 20
	p1 := randBytes(rng, minPart)
	p2 := randBytes(rng, 1000) // the tail can be any size
	whole := append(append([]byte{}, p1...), p2...)

	uid := initiate("big.bin")
	part := func(key, uploadID string, n int, body []byte) string {
		resp, rb := mpDo("PUT", fmt.Sprintf("/vault/%s?partNumber=%d&uploadId=%s", key, n, uploadID), body, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("upload part %d: status %d\n%s", n, resp.StatusCode, rb)
		}
		etag := resp.Header.Get("ETag")
		if etag == "" {
			t.Fatalf("upload part %d: no ETag header", n)
		}
		return etag
	}
	e1 := part("big.bin", uid, 1, p1)
	e2 := part("big.bin", uid, 2, p2)

	complete := func(key, uploadID string, etags map[int]string) {
		var b strings.Builder
		b.WriteString("<CompleteMultipartUpload>")
		for n := 1; n <= len(etags); n++ {
			fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", n, etags[n])
		}
		b.WriteString("</CompleteMultipartUpload>")
		resp, rb := mpDo("POST", "/vault/"+key+"?uploadId="+uploadID, []byte(b.String()), nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("complete %s: status %d\n%s", key, resp.StatusCode, rb)
		}
	}
	complete("big.bin", uid, map[int]string{1: e1, 2: e2})

	c.getEventually(cl.alive(), "/vault/big.bin", whole)

	// A Range crossing the part-1/part-2 boundary touches both parts.
	off := minPart - 400
	resp, rb := mpDo("GET", "/vault/big.bin", nil, map[string]string{
		"Range": fmt.Sprintf("bytes=%d-%d", off, off+799),
	})
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		t.Fatalf("range get: status %d", resp.StatusCode)
	}
	if want := whole[off : off+800]; string(rb) != string(want) {
		t.Fatalf("cross-boundary range: %d bytes, want %d", len(rb), len(want))
	}

	// --- UploadPartCopy: a single copied part from an existing object. ---
	src := randBytes(rng, 1<<20)
	c.mutate([]string{cl.leaderS3()}, "PUT", "/vault/srcobj", src, http.StatusOK)
	cuid := initiate("copydest")
	cpResp, cpBody := mpDo("PUT", "/vault/copydest?partNumber=1&uploadId="+cuid, nil,
		map[string]string{"x-amz-copy-source": "/vault/srcobj"})
	if cpResp.StatusCode != http.StatusOK {
		t.Fatalf("upload-part-copy: status %d\n%s", cpResp.StatusCode, cpBody)
	}
	var cpRes struct {
		ETag string `xml:"ETag"`
	}
	if err := xml.Unmarshal(cpBody, &cpRes); err != nil || cpRes.ETag == "" {
		t.Fatalf("upload-part-copy: parse ETag: %v\n%s", err, cpBody)
	}
	complete("copydest", cuid, map[int]string{1: cpRes.ETag})
	c.getEventually(cl.alive(), "/vault/copydest", src)

	// --- Abort: an uploaded part, then abort, leaves no object and no upload. ---
	auid := initiate("aborted")
	part("aborted", auid, 1, randBytes(rng, minPart))
	if resp, rb := mpDo("DELETE", "/vault/aborted?uploadId="+auid, nil, nil); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("abort: status %d\n%s", resp.StatusCode, rb)
	}
	// The object never materialized.
	if resp, _ := c.do(cl.leaderS3(), "GET", "/vault/aborted", nil); resp != nil && resp.StatusCode == http.StatusOK {
		t.Fatal("aborted upload produced a readable object")
	}
	// The upload is gone: listing its parts is NoSuchUpload (404).
	if resp, _ := mpDo("GET", "/vault/aborted?uploadId="+auid, nil, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("list parts of aborted upload: status %d, want 404", resp.StatusCode)
	}
}
