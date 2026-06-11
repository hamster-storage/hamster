package gateway_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// This file is an independent client-side SigV4 signer, written from the
// specification rather than from internal/sigv4, so the end-to-end tests
// exercise real client/server agreement instead of one implementation
// agreeing with itself.

const (
	testAKID   = "HAMSTERTESTKEY"
	testSecret = "hamster-test-secret"
	testRegion = "us-east-1"
)

// clientNow is the fixed time the test clock reports and requests are
// signed at.
var clientNow = time.Date(2026, 6, 11, 15, 0, 0, 0, time.UTC)

func hmac256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

// clientEncode is SigV4 uri-encoding: unreserved bytes literal, everything
// else percent-encoded uppercase.
func clientEncode(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0xf])
		}
	}
	return b.String()
}

// signRequest signs r with the test credentials, covering host,
// x-amz-content-sha256, and x-amz-date. payloadHash is what the client
// declares for the body.
func signRequest(r *http.Request, payloadHash string) {
	amzDate := clientNow.Format("20060102T150405Z")
	date := amzDate[:8]
	r.Header.Set("x-amz-date", amzDate)
	r.Header.Set("x-amz-content-sha256", payloadHash)

	// Canonical URI: each path segment uri-encoded once over the decoded
	// segment bytes.
	segs := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	for i, s := range segs {
		segs[i] = clientEncode(s)
	}
	canonicalURI := "/" + strings.Join(segs, "/")

	// Canonical query: decoded pairs re-encoded and sorted.
	q, _ := url.ParseQuery(r.URL.RawQuery)
	var pairs []string
	for k, vs := range q {
		for _, v := range vs {
			pairs = append(pairs, clientEncode(k)+"="+clientEncode(v))
		}
	}
	slices.Sort(pairs)
	canonicalQuery := strings.Join(pairs, "&")

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + r.Host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"

	canonical := strings.Join([]string{
		r.Method, canonicalURI, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash,
	}, "\n")

	scope := date + "/" + testRegion + "/s3/aws4_request"
	canonicalHash := sha256.Sum256([]byte(canonical))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(canonicalHash[:]),
	}, "\n")

	key := hmac256([]byte("AWS4"+testSecret), date)
	key = hmac256(key, testRegion)
	key = hmac256(key, "s3")
	key = hmac256(key, "aws4_request")
	signature := hex.EncodeToString(hmac256(key, stringToSign))

	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+testAKID+"/"+scope+
		",SignedHeaders="+signedHeaders+",Signature="+signature)
}
