package sigv4

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// signRequestHeader signs a request the way a client would, using the
// package's own canonicalization. Round-trip tests built on it cannot
// catch a canonicalization bug on their own — the AWS vectors in
// vectors_test.go do that — but they pin sign/verify consistency across
// arbitrary inputs and prove that tampering with any signed element is
// detected.
func signRequestHeader(t *testing.T, r *http.Request, akid, secret, region, when string) {
	t.Helper()
	r.Header.Set("x-amz-date", when)
	if r.Header.Get("x-amz-content-sha256") == "" {
		r.Header.Set("x-amz-content-sha256", UnsignedPayload)
	}
	var names []string
	names = append(names, "host", "x-amz-content-sha256", "x-amz-date")
	signedHeaders := strings.Join(names, ";")

	block, err := canonicalHeaderBlock(r, signedHeaders)
	if err != nil {
		t.Fatal(err)
	}
	cq, err := canonicalQueryString(r.URL.RawQuery, "")
	if err != nil {
		t.Fatal(err)
	}
	canonical := strings.Join([]string{
		r.Method, canonicalURI(r), cq, block, signedHeaders, r.Header.Get("x-amz-content-sha256"),
	}, "\n")
	date := when[:8]
	scope := date + "/" + region + "/s3/aws4_request"
	sig := signRequest(signingKey(secret, date, region), when, scope, canonical)
	r.Header.Set("Authorization", algorithm+" Credential="+akid+"/"+scope+
		",SignedHeaders="+signedHeaders+",Signature="+sig)
}

func TestRoundTripRandomRequests(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 0))
	v := &Verifier{
		Region: "eu-central-1",
		Lookup: func(akid string) (string, bool) { return "secret-" + akid, true },
	}
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	when := now.Format(timeFormat)

	paths := []string{"/", "/bucket/key", "/bucket/a%20b/c+d", "/bucket/日本語", "/b/we%20ird", "/b/dollar$sign"}
	queries := []string{"", "list-type=2&prefix=a%2Fb", "versions", "delimiter=%2F&max-keys=100"}

	for i := 0; i < 50; i++ {
		target := paths[rng.IntN(len(paths))]
		if q := queries[rng.IntN(len(queries))]; q != "" {
			target += "?" + q
		}
		r := httptest.NewRequest("GET", "http://s3.example.test"+target, nil)
		akid := fmt.Sprintf("AKID%d", rng.IntN(5))
		signRequestHeader(t, r, akid, "secret-"+akid, v.Region, when)

		id, err := v.Verify(r, now)
		if err != nil {
			t.Fatalf("round trip %q failed: %v", target, err)
		}
		if id.AccessKeyID != akid {
			t.Fatalf("identity %q, want %q", id.AccessKeyID, akid)
		}

		// Any mutation of a signed element must break the signature.
		tampered := httptest.NewRequest("GET", "http://s3.example.test"+target+"#", nil)
		tampered.URL = r.URL
		tampered.Header = r.Header.Clone()
		tampered.Host = r.Host
		switch rng.IntN(3) {
		case 0:
			tampered.Method = "DELETE"
		case 1:
			tampered.Host = "evil.example.test"
		case 2:
			tampered.Header.Set("x-amz-content-sha256", emptySHA256)
		}
		if _, err := v.Verify(tampered, now); !errors.Is(err, ErrSignatureMismatch) {
			t.Fatalf("tampered request accepted: %v", err)
		}
	}
}

func TestVerifyFailureModes(t *testing.T) {
	v := &Verifier{
		Region: "us-east-1",
		Lookup: func(akid string) (string, bool) {
			if akid == "GOODKEY" {
				return "shh", true
			}
			return "", false
		},
	}
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	when := now.Format(timeFormat)
	signed := func() *http.Request {
		r := httptest.NewRequest("GET", "http://s3.example.test/bucket/key", nil)
		signRequestHeader(t, r, "GOODKEY", "shh", v.Region, when)
		return r
	}

	if _, err := v.Verify(httptest.NewRequest("GET", "http://x.test/", nil), now); !errors.Is(err, ErrMissingAuthentication) {
		t.Fatalf("anonymous request: %v", err)
	}

	if _, err := v.Verify(signed(), now); err != nil {
		t.Fatalf("control request failed: %v", err)
	}

	// Clock skew beyond the window, both directions.
	if _, err := v.Verify(signed(), now.Add(maxSkew+time.Second)); !errors.Is(err, ErrTimeSkewed) {
		t.Fatalf("stale request: %v", err)
	}
	if _, err := v.Verify(signed(), now.Add(-maxSkew-time.Second)); !errors.Is(err, ErrTimeSkewed) {
		t.Fatalf("future request: %v", err)
	}

	// Unknown access key.
	r := signed()
	r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "GOODKEY", "BADKEY", 1))
	if _, err := v.Verify(r, now); !errors.Is(err, ErrUnknownAccessKey) {
		t.Fatalf("unknown key: %v", err)
	}

	// Wrong region in the credential scope.
	other := &Verifier{Region: "eu-west-1", Lookup: v.Lookup}
	if _, err := other.Verify(signed(), now); !errors.Is(err, ErrCredentialScope) {
		t.Fatalf("cross-region request: %v", err)
	}

	// A signed header that is missing from the request.
	r = signed()
	r.Header.Del("x-amz-content-sha256")
	if _, err := v.Verify(r, now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("missing signed header: %v", err)
	}

	// Garbage authorization headers must not panic, only reject.
	for _, garbage := range []string{
		"AWS4-HMAC-SHA256",
		"AWS4-HMAC-SHA256 Credential=x",
		"AWS4-HMAC-SHA256 Credential=a/b/c,SignedHeaders=host,Signature=zz",
		"Basic dXNlcjpwYXNz",
		"AWS4-HMAC-SHA256 Credential=k/20260611/us-east-1/s3/aws4_request,SignedHeaders=range,Signature=" + strings.Repeat("0", 64),
	} {
		r := httptest.NewRequest("GET", "http://s3.example.test/", nil)
		r.Header.Set("x-amz-date", when)
		r.Header.Set("x-amz-content-sha256", UnsignedPayload)
		r.Header.Set("Authorization", garbage)
		if _, err := v.Verify(r, now); err == nil {
			t.Fatalf("garbage authorization %q accepted", garbage)
		}
	}
}

func TestPresignedStartSkew(t *testing.T) {
	// A presigned URL used slightly before its X-Amz-Date is within
	// allowed skew; far before is not.
	v := exampleVerifier()
	url := "http://examplebucket.s3.amazonaws.com/test.txt" +
		"?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
		"&X-Amz-Date=20130524T000000Z" +
		"&X-Amz-Expires=86400" +
		"&X-Amz-SignedHeaders=host" +
		"&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	issued := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

	r := httptest.NewRequest("GET", url, nil)
	if _, err := v.Verify(r, issued.Add(-time.Minute)); err != nil {
		t.Fatalf("presigned URL within start skew rejected: %v", err)
	}
	if _, err := v.Verify(r, issued.Add(-maxSkew-time.Minute)); !errors.Is(err, ErrTimeSkewed) {
		t.Fatalf("presigned URL far before issue: %v", err)
	}
}
