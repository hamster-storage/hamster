package sigv4

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// AWS's published S3 SigV4 examples ("Authenticating Requests: Using the
// Authorization Header"), reproduced byte for byte. The credentials are
// AWS's documented example pair; the expected signatures are the ones the
// documentation states.
const (
	exampleAKID   = "AKIAIOSFODNN7EXAMPLE"
	exampleSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	exampleDate   = "20130524T000000Z"
	exampleScope  = "20130524/us-east-1/s3/aws4_request"
)

func exampleVerifier() *Verifier {
	return &Verifier{
		Region: "us-east-1",
		Lookup: func(akid string) (string, bool) {
			if akid == exampleAKID {
				return exampleSecret, true
			}
			return "", false
		},
	}
}

// exampleNow is within the skew window of the example timestamps.
var exampleNow = time.Date(2013, 5, 24, 0, 0, 10, 0, time.UTC)

func authHeader(signedHeaders, signature string) string {
	return algorithm + " Credential=" + exampleAKID + "/" + exampleScope +
		",SignedHeaders=" + signedHeaders + ",Signature=" + signature
}

func TestAWSExampleGetObject(t *testing.T) {
	r := httptest.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/test.txt", nil)
	r.Header.Set("Range", "bytes=0-9")
	r.Header.Set("x-amz-content-sha256", emptySHA256)
	r.Header.Set("x-amz-date", exampleDate)
	r.Header.Set("Authorization", authHeader(
		"host;range;x-amz-content-sha256;x-amz-date",
		"f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"))

	id, err := exampleVerifier().Verify(r, exampleNow)
	if err != nil {
		t.Fatalf("AWS GET example rejected: %v", err)
	}
	if id.AccessKeyID != exampleAKID || id.PayloadHash != emptySHA256 || id.Streaming {
		t.Fatalf("identity: %+v", id)
	}
}

func TestAWSExamplePutObject(t *testing.T) {
	r := httptest.NewRequest("PUT", "http://examplebucket.s3.amazonaws.com/test$file.text", strings.NewReader("Welcome to Amazon S3."))
	r.Header.Set("Date", "Fri, 24 May 2013 00:00:00 GMT")
	r.Header.Set("x-amz-date", exampleDate)
	r.Header.Set("x-amz-storage-class", "REDUCED_REDUNDANCY")
	r.Header.Set("x-amz-content-sha256", "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072")
	r.Header.Set("Authorization", authHeader(
		"date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class",
		"98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"))

	if _, err := exampleVerifier().Verify(r, exampleNow); err != nil {
		t.Fatalf("AWS PUT example rejected: %v", err)
	}
}

func TestAWSExampleGetLifecycle(t *testing.T) {
	r := httptest.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/?lifecycle", nil)
	r.Header.Set("x-amz-content-sha256", emptySHA256)
	r.Header.Set("x-amz-date", exampleDate)
	r.Header.Set("Authorization", authHeader(
		"host;x-amz-content-sha256;x-amz-date",
		"fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"))

	if _, err := exampleVerifier().Verify(r, exampleNow); err != nil {
		t.Fatalf("AWS lifecycle example rejected: %v", err)
	}
}

func TestAWSExampleListObjects(t *testing.T) {
	r := httptest.NewRequest("GET", "http://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J", nil)
	r.Header.Set("x-amz-content-sha256", emptySHA256)
	r.Header.Set("x-amz-date", exampleDate)
	r.Header.Set("Authorization", authHeader(
		"host;x-amz-content-sha256;x-amz-date",
		"34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"))

	if _, err := exampleVerifier().Verify(r, exampleNow); err != nil {
		t.Fatalf("AWS list example rejected: %v", err)
	}
}

func TestAWSExamplePresignedGet(t *testing.T) {
	url := "http://examplebucket.s3.amazonaws.com/test.txt" +
		"?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request" +
		"&X-Amz-Date=20130524T000000Z" +
		"&X-Amz-Expires=86400" +
		"&X-Amz-SignedHeaders=host" +
		"&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	r := httptest.NewRequest("GET", url, nil)

	id, err := exampleVerifier().Verify(r, exampleNow)
	if err != nil {
		t.Fatalf("AWS presigned example rejected: %v", err)
	}
	if id.PayloadHash != UnsignedPayload {
		t.Fatalf("presigned payload mode: %q", id.PayloadHash)
	}

	// The same URL one second past expiry.
	expired := exampleNow.Add(86400 * time.Second)
	if _, err := exampleVerifier().Verify(r, expired); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired presigned URL: %v, want ErrExpired", err)
	}
}

// TestAWSExampleChunkedUpload is AWS's full aws-chunked example: a 64 KiB
// chunk of 'a', a 1 KiB chunk of 'a', and the zero-length terminator, with
// the documented seed and per-chunk signatures.
func TestAWSExampleChunkedUpload(t *testing.T) {
	const (
		seedSig  = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
		chunk1   = "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
		chunk2   = "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"
		finalSig = "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"
	)
	body := "10000;chunk-signature=" + chunk1 + "\r\n" + strings.Repeat("a", 65536) + "\r\n" +
		"400;chunk-signature=" + chunk2 + "\r\n" + strings.Repeat("a", 1024) + "\r\n" +
		"0;chunk-signature=" + finalSig + "\r\n\r\n"

	r := httptest.NewRequest("PUT", "http://s3.amazonaws.com/examplebucket/chunkObject.txt", strings.NewReader(body))
	r.Header.Set("x-amz-date", exampleDate)
	r.Header.Set("x-amz-storage-class", "REDUCED_REDUNDANCY")
	r.Header.Set("Content-Encoding", "aws-chunked")
	r.Header.Set("x-amz-decoded-content-length", "66560")
	r.Header.Set("Content-Length", "66824")
	r.Header.Set("x-amz-content-sha256", StreamingPayload)
	r.Header.Set("Authorization", authHeader(
		"content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class",
		seedSig))

	id, err := exampleVerifier().Verify(r, exampleNow)
	if err != nil {
		t.Fatalf("AWS chunked example rejected at the header: %v", err)
	}
	if !id.Streaming {
		t.Fatal("identity is not streaming")
	}
	payload, err := io.ReadAll(id.ChunkedBody(r.Body))
	if err != nil {
		t.Fatalf("chunked body rejected: %v", err)
	}
	if len(payload) != 66560 || strings.Trim(string(payload), "a") != "" {
		t.Fatalf("decoded %d bytes, want 66560 of 'a'", len(payload))
	}

	// Flip one payload byte: the chunk signature must catch it.
	tampered := strings.Replace(body, "aaaa", "aaba", 1)
	r2 := httptest.NewRequest("PUT", "http://s3.amazonaws.com/examplebucket/chunkObject.txt", strings.NewReader(tampered))
	if _, err := io.ReadAll(id.ChunkedBody(strings.NewReader(tampered))); !errors.Is(err, ErrSignatureMismatch) {
		t.Fatalf("tampered chunk accepted: %v", err)
	}
	_ = r2
}
