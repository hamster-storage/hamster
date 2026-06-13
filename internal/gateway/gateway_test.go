package gateway_test

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/blob"
	"github.com/hamster-storage/hamster/internal/gateway"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sys"
)

// fixedClock pins request time so signing and verification share one
// instant. The gateway never schedules timers.
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }
func (c fixedClock) AfterFunc(time.Duration, func()) seam.Timer {
	panic("gateway test clock: AfterFunc is not used")
}

type env struct {
	t    *testing.T
	srv  *httptest.Server
	disk *sys.Disk
}

func newEnv(t *testing.T) *env {
	t.Helper()
	loop := sys.NewLoop()
	t.Cleanup(loop.Stop) // registered first: runs after the server closes

	disk, err := sys.NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	g := gateway.New(gateway.Config{
		Region: testRegion,
		Domain: "s3.test",
		Lookup: func(akid string) (string, bool) {
			if akid == testAKID {
				return testSecret, true
			}
			return "", false
		},
		Clock: fixedClock{now: clientNow},
		Meta: gateway.NewLoopMetadata(meta.NewStore(), loop,
			fixedClock{now: clientNow}, rand.New(rand.NewPCG(42, 0))),
		Blobs: blob.NewStore(disk),
	})
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return &env{t: t, srv: srv, disk: disk}
}

// do sends one signed request and returns the response.
func (e *env) do(method, path string, body []byte, hdrs map[string]string) *http.Response {
	e.t.Helper()
	r, err := http.NewRequest(method, e.srv.URL+path, bytes.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	r.Host = r.URL.Host
	for k, v := range hdrs {
		r.Header.Set(k, v)
	}
	sum := sha256.Sum256(body)
	signRequest(r, hex.EncodeToString(sum[:]))
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		e.t.Fatal(err)
	}
	return resp
}

func (e *env) expect(resp *http.Response, status int) []byte {
	e.t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	if resp.StatusCode != status {
		e.t.Fatalf("status %d, want %d; body:\n%s", resp.StatusCode, status, body)
	}
	return body
}

func (e *env) errorCode(resp *http.Response, status int) string {
	e.t.Helper()
	body := e.expect(resp, status)
	var er struct {
		Code string `xml:"Code"`
	}
	if err := xml.Unmarshal(body, &er); err != nil {
		e.t.Fatalf("error body is not the XML envelope: %v\n%s", err, body)
	}
	return er.Code
}

func TestBucketLifecycle(t *testing.T) {
	e := newEnv(t)

	e.expect(e.do("PUT", "/docs", nil, nil), 200)
	e.expect(e.do("HEAD", "/docs", nil, nil), 200)

	if code := e.errorCode(e.do("PUT", "/docs", nil, nil), 409); code != "BucketAlreadyOwnedByYou" {
		t.Fatalf("duplicate create: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/UPPER", nil, nil), 400); code != "InvalidBucketName" {
		t.Fatalf("bad name: %s", code)
	}

	body := e.expect(e.do("GET", "/", nil, nil), 200)
	if !strings.Contains(string(body), "<Name>docs</Name>") {
		t.Fatalf("ListBuckets missing bucket:\n%s", body)
	}

	body = e.expect(e.do("GET", "/docs?location", nil, nil), 200)
	if !strings.Contains(string(body), "LocationConstraint") {
		t.Fatalf("GetBucketLocation:\n%s", body)
	}

	e.expect(e.do("PUT", "/docs/blocker", []byte("x"), nil), 200)
	if code := e.errorCode(e.do("DELETE", "/docs", nil, nil), 409); code != "BucketNotEmpty" {
		t.Fatalf("delete nonempty: %s", code)
	}
	e.expect(e.do("DELETE", "/docs/blocker", nil, nil), 204)
	e.expect(e.do("DELETE", "/docs", nil, nil), 204)
	e.expect(e.do("HEAD", "/docs", nil, nil), 404)
}

func TestObjectRoundTrip(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	content := []byte("the quick brown hamster")
	wantETag := md5.Sum(content)
	resp := e.do("PUT", "/bkt/dir/file.txt", content, map[string]string{
		"Content-Type":     "text/plain",
		"x-amz-meta-color": "golden",
	})
	if etag := resp.Header.Get("ETag"); etag != `"`+hex.EncodeToString(wantETag[:])+`"` {
		t.Fatalf("PUT ETag %q", etag)
	}
	e.expect(resp, 200)

	resp = e.do("GET", "/bkt/dir/file.txt", nil, nil)
	got := e.expect(resp, 200)
	if !bytes.Equal(got, content) {
		t.Fatalf("GET body %q", got)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/plain" {
		t.Fatalf("Content-Type %q", ct)
	}
	if c := resp.Header.Get("x-amz-meta-color"); c != "golden" {
		t.Fatalf("user metadata %q", c)
	}
	if resp.Header.Get("Last-Modified") == "" {
		t.Fatal("no Last-Modified")
	}

	resp = e.do("HEAD", "/bkt/dir/file.txt", nil, nil)
	if body := e.expect(resp, 200); len(body) != 0 {
		t.Fatalf("HEAD returned a body: %q", body)
	}
	if resp.ContentLength != int64(len(content)) {
		t.Fatalf("HEAD Content-Length %d", resp.ContentLength)
	}

	e.expect(e.do("DELETE", "/bkt/dir/file.txt", nil, nil), 204)
	if code := e.errorCode(e.do("GET", "/bkt/dir/file.txt", nil, nil), 404); code != "NoSuchKey" {
		t.Fatalf("GET after delete: %s", code)
	}
	// Idempotent: deleting a missing key is still 204.
	e.expect(e.do("DELETE", "/bkt/dir/file.txt", nil, nil), 204)

	if code := e.errorCode(e.do("GET", "/nope/x", nil, nil), 404); code != "NoSuchBucket" {
		t.Fatalf("GET in missing bucket: %s", code)
	}
}

func TestRangeGet(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)
	content := []byte("0123456789abcdefghij")
	e.expect(e.do("PUT", "/bkt/r", content, nil), 200)

	resp := e.do("GET", "/bkt/r", nil, map[string]string{"Range": "bytes=5-9"})
	got := e.expect(resp, 206)
	if string(got) != "56789" {
		t.Fatalf("range body %q", got)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes 5-9/20" {
		t.Fatalf("Content-Range %q", cr)
	}
}

// TestOverwriteReclaimsBlob pins the v0.1 blob accounting: an unversioned
// overwrite and a delete both remove the replaced blob from the disk.
func TestOverwriteReclaimsBlob(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	e.expect(e.do("PUT", "/bkt/k", []byte("first"), nil), 200)
	e.expect(e.do("PUT", "/bkt/k", []byte("second"), nil), 200)
	if got := e.expect(e.do("GET", "/bkt/k", nil, nil), 200); string(got) != "second" {
		t.Fatalf("after overwrite: %q", got)
	}
	if n := e.blobCount(); n != 1 {
		t.Fatalf("%d blobs on disk after overwrite, want 1", n)
	}
	e.expect(e.do("DELETE", "/bkt/k", nil, nil), 204)
	if n := e.blobCount(); n != 0 {
		t.Fatalf("%d blobs on disk after delete, want 0", n)
	}
}

func (e *env) blobCount() int {
	e.t.Helper()
	names, err := e.disk.List()
	if err != nil {
		e.t.Fatal(err)
	}
	n := 0
	for _, name := range names {
		if strings.HasPrefix(name, "o/") {
			n++
		}
	}
	return n
}

func TestListObjectsV2(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)
	for _, k := range []string{"a.txt", "b/1", "b/2", "b/3", "c.txt"} {
		e.expect(e.do("PUT", "/bkt/"+k, []byte(k), nil), 200)
	}

	type result struct {
		IsTruncated           bool     `xml:"IsTruncated"`
		KeyCount              int      `xml:"KeyCount"`
		NextContinuationToken string   `xml:"NextContinuationToken"`
		Keys                  []string `xml:"Contents>Key"`
		Prefixes              []string `xml:"CommonPrefixes>Prefix"`
	}
	list := func(query string) result {
		t.Helper()
		var res result
		body := e.expect(e.do("GET", "/bkt?"+query, nil, nil), 200)
		if err := xml.Unmarshal(body, &res); err != nil {
			t.Fatalf("%v:\n%s", err, body)
		}
		return res
	}

	// Delimiter grouping: b/* collapses to one common prefix.
	res := list("list-type=2&delimiter=%2F")
	if fmt.Sprint(res.Keys) != "[a.txt c.txt]" || fmt.Sprint(res.Prefixes) != "[b/]" {
		t.Fatalf("delimiter listing: keys %v prefixes %v", res.Keys, res.Prefixes)
	}
	if res.IsTruncated || res.KeyCount != 3 {
		t.Fatalf("delimiter listing: %+v", res)
	}

	// Prefix narrows to the group.
	res = list("list-type=2&prefix=b%2F")
	if fmt.Sprint(res.Keys) != "[b/1 b/2 b/3]" {
		t.Fatalf("prefix listing: %v", res.Keys)
	}

	// Page through the delimited listing one entry at a time; entries
	// must arrive in order with no duplicates, including the grouped one.
	var pages []string
	query := "list-type=2&delimiter=%2F&max-keys=1"
	for {
		res = list(query)
		pages = append(pages, append(res.Keys, res.Prefixes...)...)
		if !res.IsTruncated {
			break
		}
		query = "list-type=2&delimiter=%2F&max-keys=1&continuation-token=" +
			url.QueryEscape(res.NextContinuationToken)
	}
	if fmt.Sprint(pages) != "[a.txt b/ c.txt]" {
		t.Fatalf("paged listing: %v", pages)
	}

	// start-after skips; encoding-type=url encodes keys.
	res = list("list-type=2&start-after=b%2F3")
	if fmt.Sprint(res.Keys) != "[c.txt]" {
		t.Fatalf("start-after: %v", res.Keys)
	}

	e.expect(e.do("PUT", "/bkt/sp ace.txt", []byte("x"), nil), 200)
	res = list("list-type=2&encoding-type=url&prefix=sp")
	if fmt.Sprint(res.Keys) != "[sp%20ace.txt]" {
		t.Fatalf("encoding-type=url: %v", res.Keys)
	}
}

func TestListObjectsV1(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)
	for _, k := range []string{"x/1", "x/2", "y", "z"} {
		e.expect(e.do("PUT", "/bkt/"+k, []byte(k), nil), 200)
	}

	type result struct {
		IsTruncated bool     `xml:"IsTruncated"`
		NextMarker  string   `xml:"NextMarker"`
		Keys        []string `xml:"Contents>Key"`
		Prefixes    []string `xml:"CommonPrefixes>Prefix"`
	}
	var res result
	body := e.expect(e.do("GET", "/bkt?delimiter=%2F&max-keys=2", nil, nil), 200)
	if err := xml.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(res.Prefixes) != "[x/]" || fmt.Sprint(res.Keys) != "[y]" || !res.IsTruncated || res.NextMarker != "y" {
		t.Fatalf("V1 page 1: %+v", res)
	}

	res = result{}
	body = e.expect(e.do("GET", "/bkt?delimiter=%2F&max-keys=2&marker=y", nil, nil), 200)
	if err := xml.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(res.Keys) != "[z]" || res.IsTruncated {
		t.Fatalf("V1 page 2: %+v", res)
	}
}

func TestAuthFailures(t *testing.T) {
	e := newEnv(t)

	// Anonymous request.
	r, _ := http.NewRequest("GET", e.srv.URL+"/", nil)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	if code := e.errorCode(resp, 403); code != "AccessDenied" {
		t.Fatalf("anonymous: %s", code)
	}

	// Wrong signature: signed body differs from sent body.
	r, _ = http.NewRequest("PUT", e.srv.URL+"/bkt/k", strings.NewReader("real"))
	r.Host = r.URL.Host
	sum := sha256.Sum256([]byte("real"))
	signRequest(r, hex.EncodeToString(sum[:]))
	// Mutate a signed header, staying inside the skew window so the
	// failure is the signature, not the timestamp.
	r.Header.Set("x-amz-date", "20260611T145900Z")
	resp, err = http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	if code := e.errorCode(resp, 403); code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered: %s", code)
	}
}

// TestDeclaredHashMismatch covers the gap SigV4 alone leaves: the signature
// only covers the *declared* payload hash, so the gateway must compare it
// against the bytes that actually arrived.
func TestDeclaredHashMismatch(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	r, _ := http.NewRequest("PUT", e.srv.URL+"/bkt/k", strings.NewReader("tampered"))
	r.Host = r.URL.Host
	sum := sha256.Sum256([]byte("original"))
	signRequest(r, hex.EncodeToString(sum[:])) // declared hash of different bytes
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	if code := e.errorCode(resp, 400); code != "XAmzContentSHA256Mismatch" {
		t.Fatalf("hash mismatch: %s", code)
	}
}

func TestUnsignedPayload(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	r, _ := http.NewRequest("PUT", e.srv.URL+"/bkt/u", strings.NewReader("payload"))
	r.Host = r.URL.Host
	signRequest(r, "UNSIGNED-PAYLOAD")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	e.expect(resp, 200)
	if got := e.expect(e.do("GET", "/bkt/u", nil, nil), 200); string(got) != "payload" {
		t.Fatalf("unsigned payload round trip: %q", got)
	}
}

func TestRequestValidation(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	if code := e.errorCode(e.do("PUT", "/bkt/bad%00key", []byte("x"), nil), 400); code != "InvalidObjectName" {
		t.Fatalf("NUL key: %s", code)
	}
	long := strings.Repeat("k", 1025)
	if code := e.errorCode(e.do("PUT", "/bkt/"+long, []byte("x"), nil), 400); code != "KeyTooLongError" {
		t.Fatalf("long key: %s", code)
	}
	if code := e.errorCode(e.do("GET", "/bkt/k?tagging", nil, nil), 501); code != "NotImplemented" {
		t.Fatalf("subresource: %s", code)
	}
	// aws-sdk-go-v2 tags every request with ?x-id=<operation>; it must not
	// read as a subresource (rclone's PUTs broke on this in the compat suite).
	e.expect(e.do("PUT", "/bkt/xid?x-id=PutObject", []byte("x"), nil), 200)
	if got := e.expect(e.do("GET", "/bkt/xid?x-id=GetObject", nil, nil), 200); string(got) != "x" {
		t.Fatalf("x-id round trip: %q", got)
	}
	if code := e.errorCode(e.do("POST", "/bkt?notreal", nil, nil), 501); code != "NotImplemented" {
		t.Fatalf("unknown bucket POST: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/locked", nil, map[string]string{
		"x-amz-bucket-object-lock-enabled": "true",
	}), 501); code != "NotImplemented" {
		t.Fatalf("lock-enabled create: %s", code)
	}
}
