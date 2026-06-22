//go:build e2e

package e2e

import (
	"encoding/xml"
	"fmt"
	"math/rand/v2"
	"net/http"
	"testing"
	"time"
)

// TestClusterProposalForwarding proves a write that lands on a non-leader is
// committed, not bounced (ADR-0037). The follower runs the data plane locally —
// erasure coding, shard transfer — and forwards only the small metadata commit
// to the leader; the object then reads back from any node. Before forwarding a
// non-leader answered 503 and the client had to find the leader itself.
func TestClusterProposalForwarding(t *testing.T) {
	const (
		akid   = "e2e-fwd"
		secret = "e2e-fwd-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	cl := startCluster(t, "e2e-fwd", 3, env)
	c := &s3Client{t: t, akid: akid, secret: secret, region: region}

	follower := cl.followerS3()

	// directed sends one signed request to a specific node and demands an exact
	// status — no leader-hunting, so a refusal would fail the test.
	directed := func(method, path string, body []byte, hdrs map[string]string, want int) (*http.Response, []byte) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for {
			resp, rb := c.doH(follower, method, path, body, hdrs)
			if resp != nil && resp.StatusCode == want {
				return resp, rb
			}
			// 503 only while the follower is still finding the leader at startup;
			// it must stop bouncing well before the deadline.
			if resp != nil && resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("%s %s on follower: status %d, want %d\n%s", method, path, resp.StatusCode, want, rb)
			}
			if time.Now().After(deadline) {
				code := 0
				if resp != nil {
					code = resp.StatusCode
				}
				t.Fatalf("%s %s on follower: never reached %d (last %d)", method, path, want, code)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	// A bucket and an object, both written through the follower.
	directed("PUT", "/vault", nil, nil, http.StatusOK)
	body := randBytes(rand.New(rand.NewPCG(3, 4)), 2<<20+123)
	directed("PUT", "/vault/forwarded", body, nil, http.StatusOK)

	// The object reads back from every node — the commit went through Raft on the
	// leader and replicated, even though the write landed on a follower.
	c.getEventually(cl.alive(), "/vault/forwarded", body)

	// A real apply error keeps its identity across the forward hop: a delete with
	// object lock would be 403, a copy of a missing source is 404. Here, a
	// multipart upload driven entirely through the follower exercises a typed
	// result (the upload ID, the part ETags, the composite) across the hop.
	resp, rb := directed("POST", "/vault/mp?uploads", nil, nil, http.StatusOK)
	var initiated struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(rb, &initiated); err != nil || initiated.UploadID == "" {
		t.Fatalf("initiate via follower: %v\n%s", err, rb)
	}
	part := randBytes(rand.New(rand.NewPCG(5, 6)), 5<<20)
	resp, _ = directed("PUT", fmt.Sprintf("/vault/mp?partNumber=1&uploadId=%s", initiated.UploadID), part, nil, http.StatusOK)
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("forwarded UploadPart returned no ETag")
	}
	completeXML := fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>", etag)
	directed("POST", "/vault/mp?uploadId="+initiated.UploadID, []byte(completeXML), nil, http.StatusOK)
	c.getEventually(cl.alive(), "/vault/mp", part)

	// An apply error keeps its identity across the hop (a meta sentinel, not a
	// generic 500): a server-side copy of a missing source is NoSuchKey → 404,
	// driven through the follower.
	resp, _ = c.doH(follower, "PUT", "/vault/copydest", nil, map[string]string{"x-amz-copy-source": "/vault/does-not-exist"})
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("copy of missing source via follower: want 404, got %v", resp)
	}
}
