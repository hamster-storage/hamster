package gateway_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"strings"
	"testing"
)

func deleteXML(quiet bool, keys ...string) []byte {
	var b strings.Builder
	b.WriteString("<Delete>")
	if quiet {
		b.WriteString("<Quiet>true</Quiet>")
	}
	for _, k := range keys {
		b.WriteString("<Object><Key>" + k + "</Key></Object>")
	}
	b.WriteString("</Delete>")
	return []byte(b.String())
}

type deleteResultXML struct {
	Deleted []struct {
		Key string `xml:"Key"`
	} `xml:"Deleted"`
	Errors []struct {
		Key  string `xml:"Key"`
		Code string `xml:"Code"`
	} `xml:"Error"`
}

func (e *env) batchDelete(t *testing.T, bucket string, body []byte) deleteResultXML {
	t.Helper()
	raw := e.expect(e.do("POST", "/"+bucket+"?delete", body, nil), 200)
	var res deleteResultXML
	if err := xml.Unmarshal(raw, &res); err != nil {
		t.Fatalf("DeleteResult: %v\n%s", err, raw)
	}
	return res
}

func TestDeleteObjects(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)
	for _, k := range []string{"a", "b", "keep"} {
		e.expect(e.do("PUT", "/bkt/"+k, []byte("data-"+k), nil), 200)
	}

	// Two real keys and a miss: all three report Deleted — S3 batch
	// delete is idempotent per key, exactly like single DELETE.
	res := e.batchDelete(t, "bkt", deleteXML(false, "a", "b", "never-existed"))
	if len(res.Deleted) != 3 || len(res.Errors) != 0 {
		t.Fatalf("deleted %d, errors %d, want 3/0", len(res.Deleted), len(res.Errors))
	}
	if n := e.blobCount(); n != 1 {
		t.Fatalf("%d blobs after batch delete, want 1", n)
	}
	body := e.expect(e.do("GET", "/bkt?list-type=2", nil, nil), 200)
	if strings.Contains(string(body), "<Key>a</Key>") || !strings.Contains(string(body), "<Key>keep</Key>") {
		t.Fatalf("listing after batch delete:\n%s", body)
	}

	// Quiet mode reports only errors.
	res = e.batchDelete(t, "bkt", deleteXML(true, "keep"))
	if len(res.Deleted) != 0 || len(res.Errors) != 0 {
		t.Fatalf("quiet delete reported %d/%d entries", len(res.Deleted), len(res.Errors))
	}
	if n := e.blobCount(); n != 0 {
		t.Fatalf("%d blobs after quiet delete, want 0", n)
	}
}

func TestDeleteObjectsMultipartReclaim(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	uid := e.initiate(t, "bkt", "mp.bin", nil)
	t1 := e.putPart(t, "bkt", "mp.bin", uid, 1, bytes.Repeat([]byte("z"), partSize))
	t2 := e.putPart(t, "bkt", "mp.bin", uid, 2, []byte("tail"))
	e.expect(e.do("POST", "/bkt/mp.bin?uploadId="+uid,
		completeXML([2]string{"1", t1}, [2]string{"2", t2}), nil), 200)

	res := e.batchDelete(t, "bkt", deleteXML(false, "mp.bin"))
	if len(res.Deleted) != 1 {
		t.Fatalf("multipart batch delete: %+v", res)
	}
	if n := e.blobCount(); n != 0 {
		t.Fatalf("%d blobs after deleting multipart object, want 0 — part blobs leaked", n)
	}
}

func TestDeleteObjectsErrors(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)
	e.expect(e.do("PUT", "/bkt/k", []byte("x"), nil), 200)

	if code := e.errorCode(e.do("POST", "/nope?delete", deleteXML(false, "k"), nil), 404); code != "NoSuchBucket" {
		t.Fatalf("missing bucket: %s", code)
	}
	if code := e.errorCode(e.do("POST", "/bkt?delete", []byte("<garbage"), nil), 400); code != "MalformedXML" {
		t.Fatalf("garbage body: %s", code)
	}
	if code := e.errorCode(e.do("POST", "/bkt?delete", deleteXML(false), nil), 400); code != "MalformedXML" {
		t.Fatalf("empty list: %s", code)
	}
	tooMany := make([]string, 1001)
	for i := range tooMany {
		tooMany[i] = "k"
	}
	if code := e.errorCode(e.do("POST", "/bkt?delete", deleteXML(false, tooMany...), nil), 400); code != "MalformedXML" {
		t.Fatalf("over the 1000-key limit: %s", code)
	}

	// Per-key outcomes: a versioned delete is refused per key (v0.5), an
	// invalid key errors per key, and neither poisons the valid one.
	body := []byte(`<Delete>` +
		`<Object><Key>k</Key><VersionId>some-version</VersionId></Object>` +
		`<Object><Key>` + strings.Repeat("x", 1025) + `</Key></Object>` +
		`<Object><Key>k</Key></Object>` +
		`</Delete>`)
	res := e.batchDelete(t, "bkt", body)
	if len(res.Deleted) != 1 || res.Deleted[0].Key != "k" {
		t.Fatalf("deleted: %+v", res.Deleted)
	}
	if len(res.Errors) != 2 || res.Errors[0].Code != "NotImplemented" || res.Errors[1].Code != "InvalidObjectName" {
		t.Fatalf("per-key errors: %+v", res.Errors)
	}
}

func TestContentMD5(t *testing.T) {
	e := newEnv(t)
	e.expect(e.do("PUT", "/bkt", nil, nil), 200)

	content := []byte("checked body")
	sum := md5.Sum(content)
	good := base64.StdEncoding.EncodeToString(sum[:])

	// Correct digest accepted, on PUT and on DeleteObjects alike.
	e.expect(e.do("PUT", "/bkt/k", content, map[string]string{"Content-MD5": good}), 200)
	del := deleteXML(false, "k")
	delSum := md5.Sum(del)
	e.batchDelete(t, "bkt", del) // no header
	e.expect(e.do("PUT", "/bkt/k", content, nil), 200)
	raw := e.expect(e.do("POST", "/bkt?delete", del,
		map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(delSum[:])}), 200)
	if !strings.Contains(string(raw), "<Deleted>") {
		t.Fatalf("delete with Content-MD5:\n%s", raw)
	}

	// A wrong digest is BadDigest; a malformed one is InvalidDigest.
	wrong := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16))
	if code := e.errorCode(e.do("PUT", "/bkt/k", content, map[string]string{"Content-MD5": wrong}), 400); code != "BadDigest" {
		t.Fatalf("wrong digest: %s", code)
	}
	if code := e.errorCode(e.do("PUT", "/bkt/k", content, map[string]string{"Content-MD5": "!!!"}), 400); code != "InvalidDigest" {
		t.Fatalf("malformed digest: %s", code)
	}
}
