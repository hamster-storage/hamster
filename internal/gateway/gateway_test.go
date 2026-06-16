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

func TestBucketVersioningConfig(t *testing.T) {
	e := newEnv(t)

	// Missing bucket: GetBucketVersioning is a 404, like any bucket subresource.
	if code := e.errorCode(e.do("GET", "/vault?versioning", nil, nil), 404); code != "NoSuchBucket" {
		t.Fatalf("versioning on missing bucket: %s", code)
	}

	e.expect(e.do("PUT", "/vault", nil, nil), 200)

	// A fresh bucket is Unversioned: an empty <VersioningConfiguration/>, no Status.
	body := e.expect(e.do("GET", "/vault?versioning", nil, nil), 200)
	if strings.Contains(string(body), "<Status>") {
		t.Fatalf("unversioned bucket should report no Status:\n%s", body)
	}

	enable := []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`)
	e.expect(e.do("PUT", "/vault?versioning", enable, nil), 200)
	body = e.expect(e.do("GET", "/vault?versioning", nil, nil), 200)
	if !strings.Contains(string(body), "<Status>Enabled</Status>") {
		t.Fatalf("expected Enabled:\n%s", body)
	}

	suspend := []byte(`<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`)
	e.expect(e.do("PUT", "/vault?versioning", suspend, nil), 200)
	body = e.expect(e.do("GET", "/vault?versioning", nil, nil), 200)
	if !strings.Contains(string(body), "<Status>Suspended</Status>") {
		t.Fatalf("expected Suspended:\n%s", body)
	}

	// A Status S3 does not define is MalformedXML, not a silent accept.
	bad := []byte(`<VersioningConfiguration><Status>Paused</Status></VersioningConfiguration>`)
	if code := e.errorCode(e.do("PUT", "/vault?versioning", bad, nil), 400); code != "MalformedXML" {
		t.Fatalf("bad status: %s", code)
	}

	// MFA Delete is a non-goal: refused honestly, never silently dropped.
	mfa := []byte(`<VersioningConfiguration><Status>Enabled</Status><MfaDelete>Enabled</MfaDelete></VersioningConfiguration>`)
	if code := e.errorCode(e.do("PUT", "/vault?versioning", mfa, nil), 501); code != "NotImplemented" {
		t.Fatalf("mfa delete: %s", code)
	}
}

func TestObjectLockConfig(t *testing.T) {
	e := newEnv(t)

	// Creating a bucket with object lock enables versioning on it.
	e.expect(e.do("PUT", "/vault", nil, map[string]string{"x-amz-bucket-object-lock-enabled": "true"}), 200)
	if body := e.expect(e.do("GET", "/vault?versioning", nil, nil), 200); !strings.Contains(string(body), "<Status>Enabled</Status>") {
		t.Fatalf("object-lock bucket should report versioning Enabled:\n%s", body)
	}

	// A fresh lock bucket reports Enabled with no default rule.
	body := e.expect(e.do("GET", "/vault?object-lock", nil, nil), 200)
	if !strings.Contains(string(body), "<ObjectLockEnabled>Enabled</ObjectLockEnabled>") || strings.Contains(string(body), "<Rule>") {
		t.Fatalf("fresh lock config:\n%s", body)
	}

	// Set a default retention rule; it round-trips in days shape.
	rule := []byte(`<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>30</Days></DefaultRetention></Rule></ObjectLockConfiguration>`)
	e.expect(e.do("PUT", "/vault?object-lock", rule, nil), 200)
	body = e.expect(e.do("GET", "/vault?object-lock", nil, nil), 200)
	if !strings.Contains(string(body), "<Mode>GOVERNANCE</Mode>") || !strings.Contains(string(body), "<Days>30</Days>") {
		t.Fatalf("default retention:\n%s", body)
	}

	// Both Days and Years is malformed.
	bad := []byte(`<ObjectLockConfiguration><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>1</Days><Years>1</Years></DefaultRetention></Rule></ObjectLockConfiguration>`)
	if code := e.errorCode(e.do("PUT", "/vault?object-lock", bad, nil), 400); code != "MalformedXML" {
		t.Fatalf("both days and years: %s", code)
	}

	// A bucket without object lock has no configuration and rejects one.
	e.expect(e.do("PUT", "/plain", nil, nil), 200)
	if code := e.errorCode(e.do("GET", "/plain?object-lock", nil, nil), 404); code != "ObjectLockConfigurationNotFoundError" {
		t.Fatalf("get on non-lock bucket: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/plain?object-lock", rule, nil), 400); code != "InvalidRequest" {
		t.Fatalf("put on non-lock bucket: %s", code)
	}
}

func TestObjectLegalHold(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/vault", nil, map[string]string{"x-amz-bucket-object-lock-enabled": "true"}), 200)
	put := e.do("PUT", "/vault/doc", []byte("data"), nil)
	e.expect(put, 200)
	vid := put.Header.Get("x-amz-version-id")

	// No hold to start.
	if body := e.expect(e.do("GET", "/vault/doc?legal-hold", nil, nil), 200); !strings.Contains(string(body), "<Status>OFF</Status>") {
		t.Fatalf("initial legal hold:\n%s", body)
	}

	// Place a hold; it shows in the subresource and the object response header.
	on := []byte(`<LegalHold><Status>ON</Status></LegalHold>`)
	e.expect(e.do("PUT", "/vault/doc?legal-hold", on, nil), 200)
	if body := e.expect(e.do("GET", "/vault/doc?legal-hold", nil, nil), 200); !strings.Contains(string(body), "<Status>ON</Status>") {
		t.Fatalf("hold not set:\n%s", body)
	}
	get := e.do("GET", "/vault/doc", nil, nil)
	e.expect(get, 200)
	if get.Header.Get("x-amz-object-lock-legal-hold") != "ON" {
		t.Fatalf("legal-hold header = %q", get.Header.Get("x-amz-object-lock-legal-hold"))
	}

	// A legal hold blocks deletion of the version.
	if code := e.errorCode(e.do("DELETE", "/vault/doc?versionId="+vid, nil, nil), 403); code != "AccessDenied" {
		t.Fatalf("delete under legal hold: %s", code)
	}

	// Release the hold; the version can then be deleted.
	off := []byte(`<LegalHold><Status>OFF</Status></LegalHold>`)
	e.expect(e.do("PUT", "/vault/doc?legal-hold", off, nil), 200)
	e.expect(e.do("DELETE", "/vault/doc?versionId="+vid, nil, nil), 204)
}

func TestObjectRetention(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/vault", nil, map[string]string{"x-amz-bucket-object-lock-enabled": "true"}), 200)

	// PUT with explicit COMPLIANCE retention.
	put := e.do("PUT", "/vault/locked", []byte("data"), map[string]string{
		"x-amz-object-lock-mode":              "COMPLIANCE",
		"x-amz-object-lock-retain-until-date": "2099-01-01T00:00:00Z",
	})
	e.expect(put, 200)
	vid := put.Header.Get("x-amz-version-id")

	// GET surfaces the lock headers.
	get := e.do("GET", "/vault/locked", nil, nil)
	e.expect(get, 200)
	if get.Header.Get("x-amz-object-lock-mode") != "COMPLIANCE" || get.Header.Get("x-amz-object-lock-retain-until-date") == "" {
		t.Fatalf("lock headers: mode=%q until=%q", get.Header.Get("x-amz-object-lock-mode"), get.Header.Get("x-amz-object-lock-retain-until-date"))
	}

	// GetObjectRetention returns the rule.
	if body := e.expect(e.do("GET", "/vault/locked?retention", nil, nil), 200); !strings.Contains(string(body), "<Mode>COMPLIANCE</Mode>") {
		t.Fatalf("get retention:\n%s", body)
	}

	// Invariant 4: a COMPLIANCE-locked version cannot be deleted.
	if code := e.errorCode(e.do("DELETE", "/vault/locked?versionId="+vid, nil, nil), 403); code != "AccessDenied" {
		t.Fatalf("delete locked version: %s", code)
	}

	// Retention may be extended (later date), never shortened under COMPLIANCE.
	ext := []byte(`<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2099-06-01T00:00:00Z</RetainUntilDate></Retention>`)
	e.expect(e.do("PUT", "/vault/locked?retention", ext, nil), 200)
	earlier := []byte(`<Retention><Mode>COMPLIANCE</Mode><RetainUntilDate>2099-02-01T00:00:00Z</RetainUntilDate></Retention>`)
	if code := e.errorCode(e.do("PUT", "/vault/locked?retention", earlier, nil), 403); code != "AccessDenied" {
		t.Fatalf("shorten compliance: %s", code)
	}

	// A bucket default retention applies to a plain PUT.
	rule := []byte(`<ObjectLockConfiguration><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>7</Days></DefaultRetention></Rule></ObjectLockConfiguration>`)
	e.expect(e.do("PUT", "/vault?object-lock", rule, nil), 200)
	e.expect(e.do("PUT", "/vault/auto", []byte("x"), nil), 200)
	auto := e.do("GET", "/vault/auto", nil, nil)
	e.expect(auto, 200)
	if auto.Header.Get("x-amz-object-lock-mode") != "GOVERNANCE" {
		t.Fatalf("default retention not applied: %q", auto.Header.Get("x-amz-object-lock-mode"))
	}
}

func TestObjectVersioning(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/vault", nil, nil), 200)

	// An unversioned bucket surfaces no version id on PUT.
	r := e.do("PUT", "/vault/k", []byte("plain"), nil)
	e.expect(r, 200)
	if v := r.Header.Get("x-amz-version-id"); v != "" {
		t.Fatalf("unversioned PUT returned x-amz-version-id %q", v)
	}

	enable := []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)
	e.expect(e.do("PUT", "/vault?versioning", enable, nil), 200)

	// Two versions of one key; each PUT echoes a distinct version id.
	r1 := e.do("PUT", "/vault/k", []byte("v1"), nil)
	e.expect(r1, 200)
	vid1 := r1.Header.Get("x-amz-version-id")
	r2 := e.do("PUT", "/vault/k", []byte("v2"), nil)
	e.expect(r2, 200)
	vid2 := r2.Header.Get("x-amz-version-id")
	if vid1 == "" || vid2 == "" || vid1 == vid2 {
		t.Fatalf("version ids: %q, %q", vid1, vid2)
	}

	// Current read is the latest; each version is reachable by id.
	if got := e.expect(e.do("GET", "/vault/k", nil, nil), 200); string(got) != "v2" {
		t.Fatalf("current GET = %q", got)
	}
	if got := e.expect(e.do("GET", "/vault/k?versionId="+vid1, nil, nil), 200); string(got) != "v1" {
		t.Fatalf("GET v1 = %q", got)
	}

	// A bad version id is NoSuchVersion, not a server error.
	if code := e.errorCode(e.do("GET", "/vault/k?versionId=deadbeef", nil, nil), 404); code != "NoSuchVersion" {
		t.Fatalf("bad version: %s", code)
	}

	// DELETE with no id inserts a delete marker; the current read becomes 404,
	// but old versions remain reachable.
	rd := e.do("DELETE", "/vault/k", nil, nil)
	e.expect(rd, 204)
	if rd.Header.Get("x-amz-delete-marker") != "true" {
		t.Fatal("delete did not report a marker")
	}
	markerVID := rd.Header.Get("x-amz-version-id")
	if markerVID == "" {
		t.Fatal("marker carried no version id")
	}
	if code := e.errorCode(e.do("GET", "/vault/k", nil, nil), 404); code != "NoSuchKey" {
		t.Fatalf("GET after marker: %s", code)
	}
	if got := e.expect(e.do("GET", "/vault/k?versionId="+vid2, nil, nil), 200); string(got) != "v2" {
		t.Fatalf("GET v2 after marker = %q", got)
	}

	// GET of the delete marker by id is 405, flagged as a marker.
	rm := e.do("GET", "/vault/k?versionId="+markerVID, nil, nil)
	e.expect(rm, 405)
	if rm.Header.Get("x-amz-delete-marker") != "true" {
		t.Fatal("GET of marker not flagged")
	}

	// Permanent delete of one version frees it; it is then NoSuchVersion.
	rdv := e.do("DELETE", "/vault/k?versionId="+vid1, nil, nil)
	e.expect(rdv, 204)
	if rdv.Header.Get("x-amz-version-id") != vid1 {
		t.Fatalf("permanent delete echoed %q, want %q", rdv.Header.Get("x-amz-version-id"), vid1)
	}
	if code := e.errorCode(e.do("GET", "/vault/k?versionId="+vid1, nil, nil), 404); code != "NoSuchVersion" {
		t.Fatalf("GET after permanent delete: %s", code)
	}
	// v2 survives the targeted delete of v1.
	if got := e.expect(e.do("GET", "/vault/k?versionId="+vid2, nil, nil), 200); string(got) != "v2" {
		t.Fatalf("v2 gone after deleting v1: %q", got)
	}
}

func TestListObjectVersionsAPI(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/vault", nil, nil), 200)
	enable := []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)
	e.expect(e.do("PUT", "/vault?versioning", enable, nil), 200)

	e.expect(e.do("PUT", "/vault/dir/a", []byte("a1"), nil), 200)
	e.expect(e.do("PUT", "/vault/dir/a", []byte("a2"), nil), 200)
	e.expect(e.do("PUT", "/vault/dir/b", []byte("b1"), nil), 200)
	e.expect(e.do("PUT", "/vault/top", []byte("t1"), nil), 200)
	e.expect(e.do("DELETE", "/vault/top", nil, nil), 204) // a delete marker on top

	type vEntry struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
		IsLatest  bool   `xml:"IsLatest"`
	}
	type vResult struct {
		XMLName             xml.Name `xml:"ListVersionsResult"`
		IsTruncated         bool     `xml:"IsTruncated"`
		NextKeyMarker       string   `xml:"NextKeyMarker"`
		NextVersionIdMarker string   `xml:"NextVersionIdMarker"`
		Versions            []vEntry `xml:"Version"`
		DeleteMarkers       []vEntry `xml:"DeleteMarker"`
		CommonPrefixes      []struct {
			Prefix string `xml:"Prefix"`
		} `xml:"CommonPrefixes"`
	}
	parse := func(path string) vResult {
		var res vResult
		body := e.expect(e.do("GET", path, nil, nil), 200)
		if err := xml.Unmarshal(body, &res); err != nil {
			t.Fatalf("unmarshal %s: %v\n%s", path, err, body)
		}
		return res
	}

	// Full listing: four object versions and one delete marker.
	all := parse("/vault?versions")
	if len(all.Versions) != 4 || len(all.DeleteMarkers) != 1 {
		t.Fatalf("listing: %d versions, %d markers", len(all.Versions), len(all.DeleteMarkers))
	}
	// dir/a's two versions: the first is latest, the second is not.
	var dirA []vEntry
	for _, v := range all.Versions {
		if v.Key == "dir/a" {
			dirA = append(dirA, v)
		}
	}
	if len(dirA) != 2 || !dirA[0].IsLatest || dirA[1].IsLatest || dirA[0].VersionID == dirA[1].VersionID {
		t.Fatalf("dir/a versions: %+v", dirA)
	}
	if !all.DeleteMarkers[0].IsLatest || all.DeleteMarkers[0].Key != "top" {
		t.Fatalf("top marker: %+v", all.DeleteMarkers[0])
	}

	// Delimiter rolls dir/* into one common prefix; top stays itemized.
	grouped := parse("/vault?versions&delimiter=/")
	if len(grouped.CommonPrefixes) != 1 || grouped.CommonPrefixes[0].Prefix != "dir/" {
		t.Fatalf("common prefixes: %+v", grouped.CommonPrefixes)
	}
	if len(grouped.Versions) != 1 || grouped.Versions[0].Key != "top" {
		t.Fatalf("grouped versions: %+v", grouped.Versions)
	}

	// Pagination: walk the whole listing two entries at a time.
	seen := 0
	path := "/vault?versions&max-keys=2"
	for {
		page := parse(path)
		seen += len(page.Versions) + len(page.DeleteMarkers)
		if !page.IsTruncated {
			break
		}
		path = "/vault?versions&max-keys=2&key-marker=" + url.QueryEscape(page.NextKeyMarker)
		if page.NextVersionIdMarker != "" {
			path += "&version-id-marker=" + page.NextVersionIdMarker
		}
	}
	if seen != 5 {
		t.Fatalf("paginated total = %d, want 5", seen)
	}
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
	// Creating a bucket with object lock is supported as of v0.6 (it enables
	// versioning); the full surface is covered in TestObjectLockConfig.
	e.expect(e.do("PUT", "/locked", nil, map[string]string{
		"x-amz-bucket-object-lock-enabled": "true",
	}), 200)
}
