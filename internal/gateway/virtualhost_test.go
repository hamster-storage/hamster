package gateway_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
)

// doHost is do with an explicit Host header, signed as sent — how a client
// behind real DNS would address a virtual-hosted bucket.
func (e *env) doHost(method, host, path string, body []byte) *http.Response {
	e.t.Helper()
	r, err := http.NewRequest(method, e.srv.URL+path, bytes.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	r.Host = host
	sum := sha256.Sum256(body)
	signRequest(r, hex.EncodeToString(sum[:]))
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		e.t.Fatal(err)
	}
	return resp
}

func TestVirtualHostedAddressing(t *testing.T) {
	e := newEnv(t) // Domain: s3.test

	// Create and use a bucket entirely through the Host header.
	e.expect(e.doHost("PUT", "vhb.s3.test", "/", nil), 200)
	e.expect(e.doHost("PUT", "vhb.s3.test", "/dir/key.txt", []byte("hello")), 200)
	if got := e.expect(e.doHost("GET", "vhb.s3.test", "/dir/key.txt", nil), 200); string(got) != "hello" {
		t.Fatalf("virtual-hosted GET: %q", got)
	}

	// Both styles address the same bucket.
	if got := e.expect(e.do("GET", "/vhb/dir/key.txt", nil, nil), 200); string(got) != "hello" {
		t.Fatalf("path-style GET of virtual-hosted PUT: %q", got)
	}

	// Bucket-level operations ride the root path.
	body := e.expect(e.doHost("GET", "vhb.s3.test", "/?list-type=2", nil), 200)
	if !strings.Contains(string(body), "<Key>dir/key.txt</Key>") || !strings.Contains(string(body), "<Name>vhb</Name>") {
		t.Fatalf("virtual-hosted listing:\n%s", body)
	}
	body = e.expect(e.doHost("GET", "vhb.s3.test", "/?uploads", nil), 200)
	if !strings.Contains(string(body), "ListMultipartUploadsResult") {
		t.Fatalf("virtual-hosted ?uploads:\n%s", body)
	}

	// A port or different letter case on the Host changes nothing.
	e.expect(e.doHost("HEAD", "vhb.s3.test:9000", "/dir/key.txt", nil), 200)
	e.expect(e.doHost("HEAD", "VHB.S3.Test", "/dir/key.txt", nil), 200)

	// Bucket names may contain dots: every label before the domain is the
	// bucket, so my.logs.s3.test is the bucket "my.logs".
	e.expect(e.doHost("PUT", "my.logs.s3.test", "/", nil), 200)
	e.expect(e.doHost("PUT", "my.logs.s3.test", "/x", []byte("dot")), 200)
	if got := e.expect(e.do("GET", "/my.logs/x", nil, nil), 200); string(got) != "dot" {
		t.Fatalf("dotted bucket via path style: %q", got)
	}

	// The bare domain is plain path-style: service level at "/", bucket in
	// the path.
	body = e.expect(e.doHost("GET", "s3.test", "/", nil), 200)
	if !strings.Contains(string(body), "<Name>vhb</Name>") || !strings.Contains(string(body), "<Name>my.logs</Name>") {
		t.Fatalf("ListBuckets on the bare domain:\n%s", body)
	}
	if got := e.expect(e.doHost("GET", "s3.test", "/vhb/dir/key.txt", nil), 200); string(got) != "hello" {
		t.Fatalf("path-style on the bare domain: %q", got)
	}

	// A virtual-hosted miss is NoSuchBucket, the same answer path-style gives.
	if code := e.errorCode(e.doHost("GET", "ghost.s3.test", "/?list-type=2", nil), 404); code != "NoSuchBucket" {
		t.Fatalf("missing virtual-hosted bucket: %s", code)
	}
}
