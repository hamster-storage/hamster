package gateway_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

const partSize = 5 << 20 // the S3 minimum for any part but the last

// initiate starts a multipart upload and returns its ID.
func (e *env) initiate(t *testing.T, bucket, key string, hdrs map[string]string) string {
	t.Helper()
	body := e.expect(e.do("POST", "/"+bucket+"/"+key+"?uploads", nil, hdrs), 200)
	var res struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(body, &res); err != nil || res.UploadID == "" {
		t.Fatalf("initiate response: %v\n%s", err, body)
	}
	return res.UploadID
}

// putPart uploads one part and returns its unquoted ETag.
func (e *env) putPart(t *testing.T, bucket, key, uploadID string, n int, body []byte) string {
	t.Helper()
	path := fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucket, key, n, uploadID)
	resp := e.do("PUT", path, body, nil)
	e.expect(resp, 200)
	want := md5.Sum(body)
	etag := strings.Trim(resp.Header.Get("ETag"), `"`)
	if etag != hex.EncodeToString(want[:]) {
		t.Fatalf("part %d ETag %q, want its MD5", n, etag)
	}
	return etag
}

func completeXML(parts ...[2]string) []byte {
	var b strings.Builder
	b.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		fmt.Fprintf(&b, `<Part><PartNumber>%s</PartNumber><ETag>"%s"</ETag></Part>`, p[0], p[1])
	}
	b.WriteString("</CompleteMultipartUpload>")
	return []byte(b.String())
}

func TestMultipartRoundTrip(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	uid := e.initiate(t, "bkt", "video.bin", map[string]string{
		"Content-Type": "video/mp4", "x-amz-meta-camera": "rear",
	})

	p1 := bytes.Repeat([]byte("a"), partSize)
	p2 := bytes.Repeat([]byte("b"), partSize)
	p3 := []byte("the short last part")
	t1 := e.putPart(t, "bkt", "video.bin", uid, 1, p1)
	t2 := e.putPart(t, "bkt", "video.bin", uid, 2, p2)
	t3 := e.putPart(t, "bkt", "video.bin", uid, 3, p3)

	// ListParts sees all three, and paginates by part-number-marker.
	body := e.expect(e.do("GET", "/bkt/video.bin?uploadId="+uid, nil, nil), 200)
	for _, n := range []string{"<PartNumber>1<", "<PartNumber>2<", "<PartNumber>3<"} {
		if !strings.Contains(string(body), n) {
			t.Fatalf("ListParts missing %s:\n%s", n, body)
		}
	}
	body = e.expect(e.do("GET", "/bkt/video.bin?uploadId="+uid+"&max-parts=1&part-number-marker=1", nil, nil), 200)
	if !strings.Contains(string(body), "<PartNumber>2<") || strings.Contains(string(body), "<PartNumber>3<") ||
		!strings.Contains(string(body), "<NextPartNumberMarker>2</NextPartNumberMarker>") {
		t.Fatalf("paged ListParts:\n%s", body)
	}

	// Complete: the ETag is the composite MD5-of-MD5s with the part count.
	body = e.expect(e.do("POST", "/bkt/video.bin?uploadId="+uid,
		completeXML([2]string{"1", t1}, [2]string{"2", t2}, [2]string{"3", t3}), nil), 200)
	var sum []byte
	for _, s := range []string{t1, t2, t3} {
		raw, _ := hex.DecodeString(s)
		sum = append(sum, raw...)
	}
	composite := md5.Sum(sum)
	wantETag := `"` + hex.EncodeToString(composite[:]) + `-3"`
	if !strings.Contains(string(body), "<ETag>&#34;"+hex.EncodeToString(composite[:])+"-3&#34;</ETag>") {
		t.Fatalf("complete response missing composite ETag %s:\n%s", wantETag, body)
	}

	// The assembled object reads back whole, with multipart identity.
	resp := e.do("GET", "/bkt/video.bin", nil, nil)
	got := e.expect(resp, 200)
	if !bytes.Equal(got, append(append(append([]byte{}, p1...), p2...), p3...)) {
		t.Fatalf("assembled body wrong: %d bytes", len(got))
	}
	if etag := resp.Header.Get("ETag"); etag != wantETag {
		t.Fatalf("GET ETag %q, want %q", etag, wantETag)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "video/mp4" {
		t.Fatalf("Content-Type %q: initiate metadata was lost", ct)
	}
	if meta := resp.Header.Get("x-amz-meta-camera"); meta != "rear" {
		t.Fatalf("x-amz-meta-camera %q: initiate metadata was lost", meta)
	}

	// Range reads work across the part boundary.
	rangeHdr := map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", partSize-2, partSize+1)}
	if got := e.expect(e.do("GET", "/bkt/video.bin", nil, rangeHdr), 206); string(got) != "aabb" {
		t.Fatalf("cross-part range read %q, want \"aabb\"", got)
	}

	// Listings render the composite ETag too.
	body = e.expect(e.do("GET", "/bkt?list-type=2", nil, nil), 200)
	if !strings.Contains(string(body), "-3&#34;</ETag>") {
		t.Fatalf("listing lost the part-count suffix:\n%s", body)
	}

	// Blob accounting: three part blobs back the object; an overwrite
	// reclaims all of them; the delete reclaims the replacement.
	if n := e.blobCount(); n != 3 {
		t.Fatalf("%d blobs after complete, want 3", n)
	}
	e.expect(e.do("PUT", "/bkt/video.bin", []byte("tiny"), nil), 200)
	if n := e.blobCount(); n != 1 {
		t.Fatalf("%d blobs after overwrite, want 1", n)
	}
	e.expect(e.do("DELETE", "/bkt/video.bin", nil, nil), 204)
	if n := e.blobCount(); n != 0 {
		t.Fatalf("%d blobs after delete, want 0", n)
	}
}

func TestMultipartAbortAndListUploads(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	uidA := e.initiate(t, "bkt", "a.bin", nil)
	uidB := e.initiate(t, "bkt", "b.bin", nil)
	e.putPart(t, "bkt", "a.bin", uidA, 1, []byte("part data"))

	body := e.expect(e.do("GET", "/bkt?uploads", nil, nil), 200)
	for _, want := range []string{"<Key>a.bin</Key>", uidA, "<Key>b.bin</Key>", uidB} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("ListMultipartUploads missing %q:\n%s", want, body)
		}
	}

	// In-progress uploads keep the bucket alive.
	if code := e.errorCode(e.do("DELETE", "/bkt", nil, nil), 409); code != "BucketNotEmpty" {
		t.Fatalf("bucket delete with uploads: %s", code)
	}

	// Abort reclaims the part blob and forgets the upload.
	e.expect(e.do("DELETE", "/bkt/a.bin?uploadId="+uidA, nil, nil), 204)
	if n := e.blobCount(); n != 0 {
		t.Fatalf("%d blobs after abort, want 0", n)
	}
	if code := e.errorCode(e.do("DELETE", "/bkt/a.bin?uploadId="+uidA, nil, nil), 404); code != "NoSuchUpload" {
		t.Fatalf("second abort: %s", code)
	}
	body = e.expect(e.do("GET", "/bkt?uploads", nil, nil), 200)
	if strings.Contains(string(body), "a.bin") || !strings.Contains(string(body), "b.bin") {
		t.Fatalf("listing after abort:\n%s", body)
	}

	e.expect(e.do("DELETE", "/bkt/b.bin?uploadId="+uidB, nil, nil), 204)
	e.expect(e.do("DELETE", "/bkt", nil, nil), 204)
}

func TestMultipartCompleteDiscardsUnusedParts(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	uid := e.initiate(t, "bkt", "k", nil)
	t1 := e.putPart(t, "bkt", "k", uid, 1, bytes.Repeat([]byte("x"), partSize))
	e.putPart(t, "bkt", "k", uid, 2, []byte("orphaned"))
	t3 := e.putPart(t, "bkt", "k", uid, 3, []byte("kept last"))
	// Re-uploading part 1 reclaims the displaced blob immediately.
	t1 = e.putPart(t, "bkt", "k", uid, 1, bytes.Repeat([]byte("y"), partSize))
	if n := e.blobCount(); n != 3 {
		t.Fatalf("%d blobs after re-upload, want 3", n)
	}

	e.expect(e.do("POST", "/bkt/k?uploadId="+uid,
		completeXML([2]string{"1", t1}, [2]string{"3", t3}), nil), 200)
	// Part 2 was not referenced: its blob is reclaimed with the complete.
	if n := e.blobCount(); n != 2 {
		t.Fatalf("%d blobs after partial complete, want 2", n)
	}
	if got := e.expect(e.do("GET", "/bkt/k", nil, nil), 200); !bytes.HasSuffix(got, []byte("kept last")) {
		t.Fatal("assembled object missing the kept part")
	}
}

func TestMultipartErrors(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	if code := e.errorCode(e.do("POST", "/nope/k?uploads", nil, nil), 404); code != "NoSuchBucket" {
		t.Fatalf("initiate on missing bucket: %s", code)
	}

	uid := e.initiate(t, "bkt", "k", nil)
	t1 := e.putPart(t, "bkt", "k", uid, 1, []byte("small first"))
	t2 := e.putPart(t, "bkt", "k", uid, 2, []byte("last"))

	// Part uploads aimed wrong.
	bogus := strings.Repeat("00", 16)
	if code := e.errorCode(e.do("PUT", "/bkt/k?partNumber=1&uploadId="+bogus, []byte("x"), nil), 404); code != "NoSuchUpload" {
		t.Fatalf("part to unknown upload: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/bkt/k?partNumber=1&uploadId=not-an-id", []byte("x"), nil), 404); code != "NoSuchUpload" {
		t.Fatalf("part to malformed upload ID: %s", code)
	}
	for _, n := range []string{"0", "10001", "x"} {
		if code := e.errorCode(e.do("PUT", "/bkt/k?partNumber="+n+"&uploadId="+uid, []byte("x"), nil), 400); code != "InvalidArgument" {
			t.Fatalf("part number %s: %s", n, code)
		}
	}
	// Completion failures, each with its S3 code.
	complete := func(body []byte) *http.Response {
		return e.do("POST", "/bkt/k?uploadId="+uid, body, nil)
	}
	if code := e.errorCode(complete([]byte("<not-xml")), 400); code != "MalformedXML" {
		t.Fatalf("malformed body: %s", code)
	}
	if code := e.errorCode(complete(completeXML()), 400); code != "MalformedXML" {
		t.Fatalf("empty part list: %s", code)
	}
	wrongETag := strings.Repeat("ab", 16)
	if code := e.errorCode(complete(completeXML([2]string{"1", wrongETag})), 400); code != "InvalidPart" {
		t.Fatalf("wrong ETag: %s", code)
	}
	if code := e.errorCode(complete(completeXML([2]string{"2", t2}, [2]string{"1", t1})), 400); code != "InvalidPartOrder" {
		t.Fatalf("descending parts: %s", code)
	}
	if code := e.errorCode(complete(completeXML([2]string{"1", t1}, [2]string{"2", t2})), 400); code != "EntityTooSmall" {
		t.Fatalf("undersized non-last part: %s", code)
	}
	// The failed completes left the upload usable: a single-part complete
	// (any size for the last part) still succeeds.
	e.expect(complete(completeXML([2]string{"1", t1})), 200)

	if code := e.errorCode(e.do("GET", "/bkt/k?uploadId="+uid, nil, nil), 404); code != "NoSuchUpload" {
		t.Fatalf("ListParts after complete: %s", code)
	}
}
