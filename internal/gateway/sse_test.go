package gateway_test

import "testing"

// TestSSESingleNodeRefuses: on the single-node path the gateway never
// encrypts, so it refuses every server-side-encryption request honestly
// rather than storing plaintext under the impression it is protected
// (ADR-0021). A write with no SSE header is unaffected, and its read carries
// no SSE header.
func TestSSESingleNodeRefuses(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/vault", nil, nil), 200) // bucket

	// An explicit AES256 request the single node cannot honor.
	if code := e.errorCode(e.do("PUT", "/vault/k", []byte("x"),
		map[string]string{"x-amz-server-side-encryption": "AES256"}), 501); code != "NotImplemented" {
		t.Fatalf("AES256: %s", code)
	}
	// SSE-KMS and SSE-C are refused regardless.
	if code := e.errorCode(e.do("PUT", "/vault/k", []byte("x"),
		map[string]string{"x-amz-server-side-encryption": "aws:kms"}), 501); code != "NotImplemented" {
		t.Fatalf("kms: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/vault/k", []byte("x"),
		map[string]string{"x-amz-server-side-encryption-customer-algorithm": "AES256"}), 501); code != "NotImplemented" {
		t.Fatalf("sse-c: %s", code)
	}
	// A bad value is a 400.
	if code := e.errorCode(e.do("PUT", "/vault/k", []byte("x"),
		map[string]string{"x-amz-server-side-encryption": "rot13"}), 400); code != "InvalidArgument" {
		t.Fatalf("bad value: %s", code)
	}

	// A plain write succeeds and its read carries no SSE header.
	e.expect(e.do("PUT", "/vault/plain", []byte("hello"), nil), 200)
	resp := e.do("GET", "/vault/plain", nil, nil)
	e.expect(resp, 200)
	if v := resp.Header.Get("x-amz-server-side-encryption"); v != "" {
		t.Fatalf("unencrypted object carried SSE header %q", v)
	}
}
