//go:build e2e

package e2e

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestClusterObjects is suite points 1-3 against a real cluster: bulk writes of
// a spread of sizes, listing (V2 pagination, prefix, delimiter, and a V1 pass),
// and GET variants (whole, ranges, and a missing-key 404). A 3-node cluster
// (2+1) is the smallest cluster; the harness parametrizes node count so the same
// shape can later run across the profile ladder.
func TestClusterObjects(t *testing.T) {
	const (
		akid   = "e2e-obj"
		secret = "e2e-obj-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	cl := startCluster(t, "e2e-obj", 3, env)
	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	lead := cl.leaderS3() // read listings/ranges from the leader — strongly consistent

	// --- Point 1: bulk writes of a spread of sizes ---
	c.mutate(cl.alive(), "PUT", "/vault", nil, http.StatusOK)
	rng := rand.New(rand.NewPCG(2024, 7))
	// Sizes span the empty object, the small-object k=1 copies, a single stripe,
	// and several stripes — so listing and GET cover every shape.
	sizes := []int{0, 1, 100, 1 << 10, 7 << 10, 64 << 10, 200 << 10, 512 << 10, 1<<20 + 7, 2<<20 + 11}
	bodies := map[string][]byte{}
	var keys []string
	put := func(key string, size int) {
		body := randBytes(rng, size)
		bodies[key] = body
		keys = append(keys, key)
		c.mutate(cl.alive(), "PUT", "/vault/"+key, body, http.StatusOK)
	}
	for i := 0; i < 100; i++ {
		put(fmt.Sprintf("bulk/obj-%03d", i), sizes[i%len(sizes)])
	}
	// Sibling prefixes for the prefix/delimiter tests, and one large object for
	// the range tests (multi-stripe, so ranges exercise the chunk-covering read).
	for _, k := range []string{"docs/guide", "docs/readme", "docs/spec"} {
		put(k, 3<<10)
	}
	for _, k := range []string{"images/a.png", "images/b.png"} {
		put(k, 9<<10)
	}
	const bigKey = "range/big"
	put(bigKey, 3<<20+5)
	sort.Strings(keys)
	t.Logf("stored %d objects", len(keys))

	// --- Point 2: listing ---

	// V2, paged small so pagination is exercised: every key, in order, once.
	got := c.listAllV2(lead, "vault", nil, 7)
	if !equalStrings(got, keys) {
		t.Fatalf("V2 full listing: got %d keys, want %d (first diff at %s)", len(got), len(keys), firstDiff(got, keys))
	}
	// A single large page reports the same KeyCount and is not truncated.
	if lr := c.list(lead, "vault", map[string]string{"list-type": "2", "max-keys": "1000"}); lr.KeyCount != len(keys) || lr.IsTruncated {
		t.Fatalf("V2 one-page: KeyCount=%d truncated=%v, want %d false", lr.KeyCount, lr.IsTruncated, len(keys))
	}

	// Prefix narrows to the 100 bulk objects.
	wantBulk := keysWithPrefix(keys, "bulk/")
	if gotBulk := c.listAllV2(lead, "vault", map[string]string{"prefix": "bulk/"}, 13); !equalStrings(gotBulk, wantBulk) {
		t.Fatalf("V2 prefix bulk/: got %d keys, want %d", len(gotBulk), len(wantBulk))
	}

	// Delimiter "/" at the root groups every key under its top folder: no bare
	// contents, one common prefix per folder.
	lr := c.list(lead, "vault", map[string]string{"list-type": "2", "delimiter": "/", "max-keys": "1000"})
	if len(lr.Contents) != 0 {
		t.Fatalf("delimiter root: %d contents, want 0 (every key is foldered)", len(lr.Contents))
	}
	gotPrefixes := commonPrefixes(lr)
	wantPrefixes := []string{"bulk/", "docs/", "images/", "range/"}
	if !equalStrings(gotPrefixes, wantPrefixes) {
		t.Fatalf("delimiter root common prefixes: got %v, want %v", gotPrefixes, wantPrefixes)
	}

	// Delimiter under a prefix lists that folder's leaves and no sub-prefixes.
	lr = c.list(lead, "vault", map[string]string{"list-type": "2", "prefix": "docs/", "delimiter": "/", "max-keys": "1000"})
	gotDocs := contentsKeys(lr)
	wantDocs := []string{"docs/guide", "docs/readme", "docs/spec"}
	if !equalStrings(gotDocs, wantDocs) || len(lr.CommonPrefixes) != 0 {
		t.Fatalf("delimiter under docs/: contents %v, prefixes %v, want %v and none", gotDocs, commonPrefixes(lr), wantDocs)
	}

	// The V1 (marker) listing returns the same keyspace.
	if gotV1 := c.listAllV1(lead, "vault", nil, 9); !equalStrings(gotV1, keys) {
		t.Fatalf("V1 full listing: got %d keys, want %d (first diff at %s)", len(gotV1), len(keys), firstDiff(gotV1, keys))
	}

	// --- Point 3: GET variants ---

	// A sample of whole objects across the size spread reads bit-identical from
	// any node (the rest are proven present by the listing).
	for i := 0; i < 100; i += 9 {
		key := fmt.Sprintf("bulk/obj-%03d", i)
		c.getEventually(cl.alive(), "/vault/"+key, bodies[key])
	}
	for _, k := range []string{"docs/spec", "images/b.png", bigKey} {
		c.getEventually(cl.alive(), "/vault/"+k, bodies[k])
	}

	// Ranges over the large object: a leading slice, a mid slice, a slice that
	// crosses a megabyte boundary, a suffix, an open-ended tail, and a range
	// whose end runs past the object (clamped). Each must be 206 and exact.
	big := bodies[bigKey]
	n := int64(len(big))
	ranges := []struct {
		hdr        string
		first, end int64 // want == big[first:end]
	}{
		{"bytes=0-99", 0, 100},
		{"bytes=1000-2000", 1000, 2001},
		{"bytes=1048570-1049000", 1048570, 1049001},
		{"bytes=-100", n - 100, n},
		{"bytes=2000000-", 2000000, n},
		{"bytes=3000000-9999999", 3000000, n}, // end past EOF, clamped
	}
	for _, rg := range ranges {
		status, body := c.getRange(lead, "/vault/"+bigKey, rg.hdr)
		if status != http.StatusPartialContent {
			t.Fatalf("GET %s %s: status %d, want 206", bigKey, rg.hdr, status)
		}
		if want := big[rg.first:rg.end]; !bytes.Equal(body, want) {
			t.Fatalf("GET %s %s: %d bytes, want %d", bigKey, rg.hdr, len(body), len(want))
		}
	}

	// A missing key is a clean 404.
	if resp, _ := c.do(lead, "GET", "/vault/does-not-exist", nil); resp == nil || resp.StatusCode != http.StatusNotFound {
		got := 0
		if resp != nil {
			got = resp.StatusCode
		}
		t.Fatalf("GET missing key: status %d, want 404", got)
	}
}

// listResult is the subset of ListBucketResult (V1 and V2 share the element)
// the tests assert on.
type listResult struct {
	XMLName               xml.Name      `xml:"ListBucketResult"`
	IsTruncated           bool          `xml:"IsTruncated"`
	KeyCount              int           `xml:"KeyCount"`
	NextContinuationToken string        `xml:"NextContinuationToken"`
	NextMarker            string        `xml:"NextMarker"`
	Contents              []listContent `xml:"Contents"`
	CommonPrefixes        []listPrefix  `xml:"CommonPrefixes"`
}

type listContent struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
}

type listPrefix struct {
	Prefix string `xml:"Prefix"`
}

// list issues one ListObjects request and parses the result. params are sent as
// a SigV4-canonical query string (sorted, URI-encoded) so the existing signer —
// which signs req.URL.RawQuery verbatim — matches the server's re-encoding.
func (c *s3Client) list(addr, bucket string, params map[string]string) listResult {
	c.t.Helper()
	path := "/" + bucket
	if q := canonicalQuery(params); q != "" {
		path += "?" + q
	}
	resp, body := c.do(addr, "GET", path, nil)
	if resp == nil {
		c.t.Fatalf("list %s: no response", path)
	}
	if resp.StatusCode != http.StatusOK {
		c.t.Fatalf("list %s: status %d\n%s", path, resp.StatusCode, body)
	}
	var lr listResult
	if err := xml.Unmarshal(body, &lr); err != nil {
		c.t.Fatalf("list %s: parsing %v\n%s", path, err, body)
	}
	return lr
}

// listAllV2 pages through every object via ListObjectsV2 at pageSize per page,
// following the continuation token, and returns the keys in returned order.
func (c *s3Client) listAllV2(addr, bucket string, params map[string]string, pageSize int) []string {
	c.t.Helper()
	var out []string
	token := ""
	for {
		p := map[string]string{"list-type": "2", "max-keys": strconv.Itoa(pageSize)}
		for k, v := range params {
			p[k] = v
		}
		if token != "" {
			p["continuation-token"] = token
		}
		lr := c.list(addr, bucket, p)
		out = append(out, contentsKeys(lr)...)
		if !lr.IsTruncated {
			return out
		}
		if lr.NextContinuationToken == "" {
			c.t.Fatalf("truncated V2 listing with no continuation token")
		}
		token = lr.NextContinuationToken
	}
}

// listAllV1 pages through every object via the V1 (marker) listing. Without a
// delimiter S3 sets no NextMarker, so the last returned key is the next marker.
func (c *s3Client) listAllV1(addr, bucket string, params map[string]string, pageSize int) []string {
	c.t.Helper()
	var out []string
	marker := ""
	for {
		p := map[string]string{"max-keys": strconv.Itoa(pageSize)}
		for k, v := range params {
			p[k] = v
		}
		if marker != "" {
			p["marker"] = marker
		}
		lr := c.list(addr, bucket, p)
		out = append(out, contentsKeys(lr)...)
		if !lr.IsTruncated {
			return out
		}
		if lr.NextMarker != "" {
			marker = lr.NextMarker
		} else {
			marker = out[len(out)-1]
		}
	}
}

// getRange issues a ranged GET (the Range header is not among the signed
// headers, so it is set after signing) and returns the status and body.
func (c *s3Client) getRange(addr, path, rangeHdr string) (int, []byte) {
	c.t.Helper()
	req, err := http.NewRequest("GET", "http://"+addr+path, nil)
	if err != nil {
		c.t.Fatal(err)
	}
	c.sign(req, nil)
	req.Header.Set("Range", rangeHdr)
	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		return 0, nil
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, body
}

// canonicalQuery renders params as a SigV4-canonical query string: each pair
// URI-encoded and the set sorted, matching the server's canonicalQueryString.
func canonicalQuery(params map[string]string) string {
	pairs := make([]string, 0, len(params))
	for k, v := range params {
		pairs = append(pairs, uriEncode(k)+"="+uriEncode(v))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

// uriEncode mirrors the server's SigV4 encoding (internal/sigv4): the RFC 3986
// unreserved bytes pass through, every other byte becomes %XX.
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

func contentsKeys(lr listResult) []string {
	out := make([]string, 0, len(lr.Contents))
	for _, o := range lr.Contents {
		out = append(out, o.Key)
	}
	return out
}

func commonPrefixes(lr listResult) []string {
	out := make([]string, 0, len(lr.CommonPrefixes))
	for _, p := range lr.CommonPrefixes {
		out = append(out, p.Prefix)
	}
	sort.Strings(out)
	return out
}

func keysWithPrefix(keys []string, prefix string) []string {
	var out []string
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// firstDiff reports the first index where got and want differ, for a failure
// message that points at the divergence rather than dumping both lists.
func firstDiff(got, want []string) string {
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			return fmt.Sprintf("index %d: got %q, want %q", i, got[i], want[i])
		}
	}
	return fmt.Sprintf("length %d vs %d", len(got), len(want))
}
