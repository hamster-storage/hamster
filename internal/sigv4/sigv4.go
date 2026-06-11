// Package sigv4 verifies AWS Signature Version 4 on incoming S3 requests,
// implemented in-house on the standard library per ADR-0018. It supports
// the four v0.1 modes: Authorization-header signing, presigned query URLs,
// UNSIGNED-PAYLOAD, and STREAMING-AWS4-HMAC-SHA256-PAYLOAD (aws-chunked)
// bodies.
//
// Verification is pure computation: the current time is a parameter, never
// an ambient read, so the package runs under the simulation harness
// unchanged. Conformance is tested against AWS's published example
// signatures.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	algorithm     = "AWS4-HMAC-SHA256"
	service       = "s3"
	requestSuffix = "aws4_request"
	timeFormat    = "20060102T150405Z"
	dateFormat    = "20060102"

	// maxSkew is how far a signed request's clock may differ from ours,
	// matching AWS's RequestTimeTooSkewed window.
	maxSkew = 15 * time.Minute

	// maxPresignExpiry is the largest X-Amz-Expires AWS accepts: 7 days.
	maxPresignExpiry = 7 * 24 * time.Hour

	// UnsignedPayload is the x-amz-content-sha256 value declaring that the
	// body is not covered by the signature.
	UnsignedPayload = "UNSIGNED-PAYLOAD"

	// StreamingPayload is the x-amz-content-sha256 value for aws-chunked
	// bodies with per-chunk signatures.
	StreamingPayload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

	// emptySHA256 is hex(sha256("")), the payload hash of bodyless requests.
	emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// Verification errors. The API layer maps these onto S3 error codes
// (SignatureDoesNotMatch, InvalidAccessKeyId, RequestTimeTooSkewed, ...).
var (
	ErrMissingAuthentication = errors.New("request carries no authentication")
	ErrMalformed             = errors.New("malformed authentication parameters")
	ErrUnknownAccessKey      = errors.New("unknown access key")
	ErrCredentialScope       = errors.New("credential scope does not match")
	ErrSignatureMismatch     = errors.New("signature does not match")
	ErrTimeSkewed            = errors.New("request time too skewed")
	ErrExpired               = errors.New("presigned URL has expired")
)

// CredentialLookup resolves an access key ID to its secret. Credentials
// live in replicated metadata (ADR-0018); the lookup is injected so this
// package stays pure.
type CredentialLookup func(accessKeyID string) (secretKey string, ok bool)

// Verifier checks request signatures for one configured region.
type Verifier struct {
	Region string
	Lookup CredentialLookup
}

// Identity is a successfully authenticated request: who signed it and how
// the body is covered.
type Identity struct {
	AccessKeyID string

	// PayloadHash is the declared x-amz-content-sha256. For a literal hash
	// the handler must verify the body against it while reading; for
	// UnsignedPayload there is nothing to check; for StreamingPayload the
	// body must be unwrapped with ChunkedBody, which verifies as it reads.
	PayloadHash string

	// Streaming reports an aws-chunked body (PayloadHash ==
	// StreamingPayload).
	Streaming bool

	signingKey []byte
	amzDate    string
	scope      string
	seedSig    string
}

// Verify authenticates r at time now. Requests bearing query-string
// authentication are verified as presigned URLs; requests with an
// Authorization header are verified as header-signed; anything else
// returns ErrMissingAuthentication for the caller to treat as anonymous.
func (v *Verifier) Verify(r *http.Request, now time.Time) (*Identity, error) {
	if r.URL.Query().Get("X-Amz-Signature") != "" {
		return v.verifyPresigned(r, now)
	}
	if r.Header.Get("Authorization") != "" {
		return v.verifyHeader(r, now)
	}
	return nil, ErrMissingAuthentication
}

func (v *Verifier) verifyHeader(r *http.Request, now time.Time) (*Identity, error) {
	rest, ok := strings.CutPrefix(r.Header.Get("Authorization"), algorithm+" ")
	if !ok {
		return nil, ErrMalformed
	}
	params := map[string]string{}
	for _, field := range strings.Split(rest, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(field), "=")
		if !ok {
			return nil, ErrMalformed
		}
		params[k] = val
	}
	credential, signedHeaders, signature := params["Credential"], params["SignedHeaders"], params["Signature"]
	if credential == "" || signedHeaders == "" || signature == "" {
		return nil, ErrMalformed
	}

	akid, credDate, err := v.parseCredential(credential)
	if err != nil {
		return nil, err
	}
	amzDate := r.Header.Get("x-amz-date")
	reqTime, err := time.Parse(timeFormat, amzDate)
	if err != nil {
		return nil, ErrMalformed
	}
	if reqTime.Format(dateFormat) != credDate {
		return nil, ErrCredentialScope
	}
	if d := now.Sub(reqTime); d > maxSkew || d < -maxSkew {
		return nil, ErrTimeSkewed
	}

	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		return nil, ErrMalformed // S3 requires it on every signed request
	}

	secret, ok := v.Lookup(akid)
	if !ok {
		return nil, ErrUnknownAccessKey
	}

	canonicalQuery, err := canonicalQueryString(r.URL.RawQuery, "")
	if err != nil {
		return nil, ErrMalformed
	}
	canonicalHeaders, err := canonicalHeaderBlock(r, signedHeaders)
	if err != nil {
		return nil, err
	}
	canonical := strings.Join([]string{
		r.Method, canonicalURI(r), canonicalQuery, canonicalHeaders, signedHeaders, payloadHash,
	}, "\n")

	scope := credDate + "/" + v.Region + "/" + service + "/" + requestSuffix
	key := signingKey(secret, credDate, v.Region)
	want := signRequest(key, amzDate, scope, canonical)
	if !signaturesEqual(want, signature) {
		return nil, ErrSignatureMismatch
	}
	return &Identity{
		AccessKeyID: akid,
		PayloadHash: payloadHash,
		Streaming:   payloadHash == StreamingPayload,
		signingKey:  key,
		amzDate:     amzDate,
		scope:       scope,
		seedSig:     signature,
	}, nil
}

func (v *Verifier) verifyPresigned(r *http.Request, now time.Time) (*Identity, error) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != algorithm {
		return nil, ErrMalformed
	}
	signature := q.Get("X-Amz-Signature")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	if signature == "" || signedHeaders == "" {
		return nil, ErrMalformed
	}

	akid, credDate, err := v.parseCredential(q.Get("X-Amz-Credential"))
	if err != nil {
		return nil, err
	}
	reqTime, err := time.Parse(timeFormat, q.Get("X-Amz-Date"))
	if err != nil {
		return nil, ErrMalformed
	}
	if reqTime.Format(dateFormat) != credDate {
		return nil, ErrCredentialScope
	}
	expires, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || expires < 1 || time.Duration(expires)*time.Second > maxPresignExpiry {
		return nil, ErrMalformed
	}
	if now.Before(reqTime.Add(-maxSkew)) {
		return nil, ErrTimeSkewed
	}
	if now.After(reqTime.Add(time.Duration(expires) * time.Second)) {
		return nil, ErrExpired
	}

	secret, ok := v.Lookup(akid)
	if !ok {
		return nil, ErrUnknownAccessKey
	}

	// The signature itself is excluded from the canonical query; presigned
	// S3 requests always sign UNSIGNED-PAYLOAD.
	canonicalQuery, err := canonicalQueryString(r.URL.RawQuery, "X-Amz-Signature")
	if err != nil {
		return nil, ErrMalformed
	}
	canonicalHeaders, err := canonicalHeaderBlock(r, signedHeaders)
	if err != nil {
		return nil, err
	}
	canonical := strings.Join([]string{
		r.Method, canonicalURI(r), canonicalQuery, canonicalHeaders, signedHeaders, UnsignedPayload,
	}, "\n")

	scope := credDate + "/" + v.Region + "/" + service + "/" + requestSuffix
	key := signingKey(secret, credDate, v.Region)
	want := signRequest(key, q.Get("X-Amz-Date"), scope, canonical)
	if !signaturesEqual(want, signature) {
		return nil, ErrSignatureMismatch
	}
	return &Identity{
		AccessKeyID: akid,
		PayloadHash: UnsignedPayload,
		signingKey:  key,
		amzDate:     q.Get("X-Amz-Date"),
		scope:       scope,
		seedSig:     signature,
	}, nil
}

// parseCredential splits "AKID/date/region/service/aws4_request" and
// checks the scope against this verifier.
func (v *Verifier) parseCredential(credential string) (akid, date string, err error) {
	parts := strings.Split(credential, "/")
	if len(parts) != 5 {
		return "", "", ErrMalformed
	}
	if _, err := time.Parse(dateFormat, parts[1]); err != nil {
		return "", "", ErrMalformed
	}
	if parts[2] != v.Region || parts[3] != service || parts[4] != requestSuffix {
		return "", "", ErrCredentialScope
	}
	return parts[0], parts[1], nil
}

// canonicalURI is the request path in SigV4's canonical form: each
// segment URI-encoded exactly once, slashes preserved, and — S3
// specifically — no dot-segment normalization. The wire path is
// percent-decoded and re-encoded so it lands at "encoded once" whether
// the client sent `$` or `%24`.
func canonicalURI(r *http.Request) string {
	uri := r.RequestURI
	if uri == "" || strings.Contains(uri, "://") {
		// No raw request line (tests), or absolute-form; fall back to the
		// parsed path.
		uri = r.URL.EscapedPath()
	}
	if i := strings.IndexByte(uri, '?'); i >= 0 {
		uri = uri[:i]
	}
	if uri == "" {
		return "/"
	}
	if decoded, err := url.PathUnescape(uri); err == nil {
		uri = decoded
	}
	return uriEncodePath(uri)
}

// uriEncodePath applies the SigV4 encoding to a path, keeping '/' literal.
func uriEncodePath(path string) string {
	segments := strings.Split(path, "/")
	for i, s := range segments {
		segments[i] = uriEncode(s)
	}
	return strings.Join(segments, "/")
}

// canonicalQueryString re-encodes the query per SigV4: parameters sorted
// by name then value, both URI-encoded, omitting one parameter (the
// signature itself, for presigned requests).
func canonicalQueryString(rawQuery, omit string) (string, error) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", err
	}
	var pairs []string
	for name, vals := range values {
		if name == omit {
			continue
		}
		for _, val := range vals {
			pairs = append(pairs, uriEncode(name)+"="+uriEncode(val))
		}
	}
	slices.Sort(pairs)
	return strings.Join(pairs, "&"), nil
}

// canonicalHeaderBlock builds the canonical headers section for the
// client's SignedHeaders list. Host must be signed; values are trimmed
// with internal runs of spaces collapsed, per the SigV4 spec.
func canonicalHeaderBlock(r *http.Request, signedHeaders string) (string, error) {
	names := strings.Split(signedHeaders, ";")
	if !slices.Contains(names, "host") {
		return "", ErrMalformed
	}
	var b strings.Builder
	for _, name := range names {
		var vals []string
		if name == "host" {
			vals = []string{r.Host}
		} else {
			vals = r.Header.Values(name)
			if len(vals) == 0 {
				return "", ErrMalformed // signed a header that is not present
			}
		}
		for i, v := range vals {
			vals[i] = collapseSpaces(strings.TrimSpace(v))
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strings.Join(vals, ","))
		b.WriteByte('\n')
	}
	return b.String(), nil
}

func collapseSpaces(s string) string {
	if !strings.Contains(s, "  ") {
		return s
	}
	var b strings.Builder
	space := false
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' {
			space = true
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteByte(s[i])
	}
	return b.String()
}

// uriEncode implements the SigV4 encoding rules: unreserved characters
// stay literal, everything else becomes uppercase percent escapes, space
// included.
func uriEncode(s string) string {
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

// signingKey derives the per-day key: the HMAC chain over date, region,
// service, and the terminator.
func signingKey(secret, date, region string) []byte {
	k := hmacSHA256([]byte("AWS4"+secret), date)
	k = hmacSHA256(k, region)
	k = hmacSHA256(k, service)
	return hmacSHA256(k, requestSuffix)
}

// signRequest computes the final signature over the string to sign.
func signRequest(key []byte, amzDate, scope, canonicalRequest string) string {
	sum := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := algorithm + "\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	return hex.EncodeToString(hmacSHA256(key, stringToSign))
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

// signaturesEqual compares hex signatures in constant time.
func signaturesEqual(want, got string) bool {
	return subtle.ConstantTimeCompare([]byte(want), []byte(strings.ToLower(got))) == 1
}
