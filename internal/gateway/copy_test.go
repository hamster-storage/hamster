package gateway_test

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"strings"
	"testing"
)

func TestCopyObject(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/src", nil, nil), 200)
	e.expect(e.do("PUT", "/dst", nil, nil), 200)

	content := []byte("the source object body")
	e.expect(e.do("PUT", "/src/orig.txt", content, map[string]string{
		"Content-Type": "text/plain", "x-amz-meta-origin": "earth",
	}), 200)
	srcMD5 := md5.Sum(content)
	wantETag := `"` + hex.EncodeToString(srcMD5[:]) + `"`

	// Default directive (COPY) carries the source's metadata along.
	body := e.expect(e.do("PUT", "/dst/copy.txt", nil,
		map[string]string{"x-amz-copy-source": "/src/orig.txt"}), 200)
	if !strings.Contains(string(body), "<ETag>&#34;"+hex.EncodeToString(srcMD5[:])+"&#34;</ETag>") {
		t.Fatalf("CopyObjectResult missing source ETag:\n%s", body)
	}
	resp := e.do("GET", "/dst/copy.txt", nil, nil)
	if got := e.expect(resp, 200); !bytes.Equal(got, content) {
		t.Fatalf("copied body %q", got)
	}
	if resp.Header.Get("ETag") != wantETag {
		t.Fatalf("copy ETag %q, want %q", resp.Header.Get("ETag"), wantETag)
	}
	if resp.Header.Get("Content-Type") != "text/plain" || resp.Header.Get("x-amz-meta-origin") != "earth" {
		t.Fatal("COPY directive lost the source metadata")
	}

	// REPLACE rewrites the metadata from the copy request.
	e.expect(e.do("PUT", "/dst/replaced.txt", nil, map[string]string{
		"x-amz-copy-source":        "/src/orig.txt",
		"x-amz-metadata-directive": "REPLACE",
		"Content-Type":             "application/json",
		"x-amz-meta-origin":        "mars",
	}), 200)
	resp = e.do("GET", "/dst/replaced.txt", nil, nil)
	e.expect(resp, 200)
	if resp.Header.Get("Content-Type") != "application/json" || resp.Header.Get("x-amz-meta-origin") != "mars" {
		t.Fatal("REPLACE directive did not rewrite the metadata")
	}

	// Self-copy needs REPLACE; with it, it is the S3 way to edit metadata.
	if code := e.errorCode(e.do("PUT", "/src/orig.txt", nil,
		map[string]string{"x-amz-copy-source": "/src/orig.txt"}), 400); code != "InvalidRequest" {
		t.Fatalf("self-copy without REPLACE: %s", code)
	}
	e.expect(e.do("PUT", "/src/orig.txt", nil, map[string]string{
		"x-amz-copy-source":        "/src/orig.txt",
		"x-amz-metadata-directive": "REPLACE",
		"x-amz-meta-origin":        "venus",
	}), 200)
	resp = e.do("GET", "/src/orig.txt", nil, nil)
	e.expect(resp, 200)
	if resp.Header.Get("x-amz-meta-origin") != "venus" {
		t.Fatal("self-copy with REPLACE did not update metadata")
	}

	// URL-encoded source keys decode before lookup.
	e.expect(e.do("PUT", "/src/sp ace.txt", []byte("spaced"), nil), 200)
	e.expect(e.do("PUT", "/dst/sp.txt", nil,
		map[string]string{"x-amz-copy-source": "/src/sp%20ace.txt"}), 200)
	if got := e.expect(e.do("GET", "/dst/sp.txt", nil, nil), 200); string(got) != "spaced" {
		t.Fatalf("encoded-source copy body %q", got)
	}
}

func TestCopyObjectFromMultipartSource(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	uid := e.initiate(t, "bkt", "mp.bin", nil)
	p1 := bytes.Repeat([]byte("m"), partSize)
	p2 := []byte("tail")
	t1 := e.putPart(t, "bkt", "mp.bin", uid, 1, p1)
	t2 := e.putPart(t, "bkt", "mp.bin", uid, 2, p2)
	e.expect(e.do("POST", "/bkt/mp.bin?uploadId="+uid,
		completeXML([2]string{"1", t1}, [2]string{"2", t2}), nil), 200)

	// The copy is a rewrite: one whole blob, plain MD5 ETag, no "-N".
	e.expect(e.do("PUT", "/bkt/flat.bin", nil,
		map[string]string{"x-amz-copy-source": "/bkt/mp.bin"}), 200)
	whole := append(append([]byte{}, p1...), p2...)
	wantMD5 := md5.Sum(whole)
	resp := e.do("GET", "/bkt/flat.bin", nil, nil)
	if got := e.expect(resp, 200); !bytes.Equal(got, whole) {
		t.Fatalf("copy of multipart source: %d bytes", len(got))
	}
	if etag := resp.Header.Get("ETag"); etag != `"`+hex.EncodeToString(wantMD5[:])+`"` {
		t.Fatalf("copy ETag %q, want plain MD5 without a part suffix", etag)
	}
	// 2 part blobs + 1 copied whole blob.
	if n := e.blobCount(); n != 3 {
		t.Fatalf("%d blobs after copy, want 3", n)
	}
}

func TestUploadPartCopy(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	src := bytes.Repeat([]byte("s"), partSize+100)
	e.expect(e.do("PUT", "/bkt/src.bin", src, nil), 200)

	uid := e.initiate(t, "bkt", "dst.bin", nil)
	// Part 1: the whole source. Part 2: a range slice (the last part, so
	// any size is fine).
	body := e.expect(e.do("PUT", "/bkt/dst.bin?partNumber=1&uploadId="+uid, nil,
		map[string]string{"x-amz-copy-source": "/bkt/src.bin"}), 200)
	full := md5.Sum(src)
	if !strings.Contains(string(body), hex.EncodeToString(full[:])) {
		t.Fatalf("CopyPartResult missing ETag:\n%s", body)
	}
	e.expect(e.do("PUT", "/bkt/dst.bin?partNumber=2&uploadId="+uid, nil, map[string]string{
		"x-amz-copy-source":       "/bkt/src.bin",
		"x-amz-copy-source-range": "bytes=0-9",
	}), 200)

	t1 := hex.EncodeToString(full[:])
	sliceMD5 := md5.Sum(src[:10])
	t2 := hex.EncodeToString(sliceMD5[:])
	e.expect(e.do("POST", "/bkt/dst.bin?uploadId="+uid,
		completeXML([2]string{"1", t1}, [2]string{"2", t2}), nil), 200)

	want := append(append([]byte{}, src...), src[:10]...)
	if got := e.expect(e.do("GET", "/bkt/dst.bin", nil, nil), 200); !bytes.Equal(got, want) {
		t.Fatalf("assembled copy-part object: %d bytes, want %d", len(got), len(want))
	}

	// Bad ranges are refused before anything is written.
	uid2 := e.initiate(t, "bkt", "more.bin", nil)
	for _, bad := range []string{"bytes=10-5", "bytes=0-999999999", "0-9", "bytes=x-y"} {
		if code := e.errorCode(e.do("PUT", "/bkt/more.bin?partNumber=1&uploadId="+uid2, nil, map[string]string{
			"x-amz-copy-source":       "/bkt/src.bin",
			"x-amz-copy-source-range": bad,
		}), 400); code != "InvalidArgument" {
			t.Fatalf("range %q: %s", bad, code)
		}
	}
}

func TestCopyRefusals(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)
	e.expect(e.do("PUT", "/bkt/k", []byte("x"), nil), 200)

	if code := e.errorCode(e.do("PUT", "/bkt/c", nil,
		map[string]string{"x-amz-copy-source": "/bkt/missing"}), 404); code != "NoSuchKey" {
		t.Fatalf("missing source key: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/bkt/c", nil,
		map[string]string{"x-amz-copy-source": "/nope/k"}), 404); code != "NoSuchBucket" {
		t.Fatalf("missing source bucket: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/nope/c", nil,
		map[string]string{"x-amz-copy-source": "/bkt/k"}), 404); code != "NoSuchBucket" {
		t.Fatalf("missing destination bucket: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/bkt/c", nil,
		map[string]string{"x-amz-copy-source": "just-a-bucket"}), 400); code != "InvalidArgument" {
		t.Fatalf("source without a key: %s", code)
	}
	// versionId sources wait for the versioning API (v0.5); honoring the
	// header by ignoring it would copy the wrong version.
	if code := e.errorCode(e.do("PUT", "/bkt/c", nil,
		map[string]string{"x-amz-copy-source": "/bkt/k?versionId=abc"}), 501); code != "NotImplemented" {
		t.Fatalf("versioned source: %s", code)
	}
	// Conditional copies are refused, not ignored.
	if code := e.errorCode(e.do("PUT", "/bkt/c", nil, map[string]string{
		"x-amz-copy-source":          "/bkt/k",
		"x-amz-copy-source-if-match": `"abc"`,
	}), 501); code != "NotImplemented" {
		t.Fatalf("conditional copy: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/bkt/c", nil, map[string]string{
		"x-amz-copy-source":        "/bkt/k",
		"x-amz-metadata-directive": "MERGE",
	}), 400); code != "InvalidArgument" {
		t.Fatalf("bad directive: %s", code)
	}
}
