package gateway

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/hamster-storage/hamster/internal/meta"
)

// CopyObject and UploadPartCopy (docs/S3-API.md). v0.1 copy is a full
// server-side read-and-rewrite: the source is read and verified exactly
// like a GET, and the destination is committed exactly like a PUT (or an
// UploadPart) of those bytes. Shard sharing between versions is a later
// optimization, not a semantic change.

// parseCopySource decodes an x-amz-copy-source header: "/bucket/key" or
// "bucket/key", URL-encoded, optionally with "?versionId=...". A version
// ID is refused until the versioning API exists (v0.5) — pretending to
// honor it would silently copy the wrong version.
func parseCopySource(s string) (bucket, key string, err error) {
	path, query, hasQuery := strings.Cut(s, "?")
	if hasQuery {
		if v, _ := url.ParseQuery(query); v.Get("versionId") != "" {
			return "", "", errNotImplemented
		}
	}
	path, unescapeErr := url.PathUnescape(path)
	if unescapeErr != nil {
		return "", "", errInvalidArgument
	}
	bucket, key = splitPath(path)
	if bucket == "" || key == "" {
		return "", "", errInvalidArgument
	}
	if err := checkObjectKey(key); err != nil {
		return "", "", err
	}
	return bucket, key, nil
}

// hasConditionalCopyHeaders reports whether the request carries any
// x-amz-copy-source-if-* header — refused honestly rather than ignored,
// because ignoring a condition means copying when the caller said not to.
func hasConditionalCopyHeaders(h http.Header) bool {
	for _, name := range []string{
		"x-amz-copy-source-if-match", "x-amz-copy-source-if-none-match",
		"x-amz-copy-source-if-modified-since", "x-amz-copy-source-if-unmodified-since",
	} {
		if h.Get(name) != "" {
			return true
		}
	}
	return false
}

// readCopySource resolves and reads the source object named by the
// request's x-amz-copy-source header, returning its location too.
func (g *Gateway) readCopySource(r *http.Request) (srcBucket, srcKey string, entry meta.VersionEntry, data []byte, err error) {
	if hasConditionalCopyHeaders(r.Header) {
		return "", "", meta.VersionEntry{}, nil, errNotImplemented
	}
	srcBucket, srcKey, err = parseCopySource(r.Header.Get("x-amz-copy-source"))
	if err != nil {
		return "", "", meta.VersionEntry{}, nil, err
	}
	entry, err = g.lookupCurrent(srcBucket, srcKey)
	if err != nil {
		return "", "", meta.VersionEntry{}, nil, err
	}
	data, err = g.readObjectData(entry)
	if err != nil {
		return "", "", meta.VersionEntry{}, nil, err
	}
	return srcBucket, srcKey, entry, data, nil
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// copyObject is S3 CopyObject: PUT with x-amz-copy-source. The rewrite
// makes the destination an ordinary whole object — its ETag is the plain
// MD5 of the bytes, even when the source was multipart (AWS does the
// same: a copy is a new single-part object).
func (g *Gateway) copyObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	directive := r.Header.Get("x-amz-metadata-directive")
	if directive == "" {
		directive = "COPY"
	}
	if directive != "COPY" && directive != "REPLACE" {
		writeError(w, r, errInvalidArgument)
		return
	}
	if g.cfg.Objects != nil {
		g.copyObjectCluster(w, r, bucket, key, directive)
		return
	}

	srcBucket, srcKey, src, data, err := g.readCopySource(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	// S3 rule: copying an object onto itself is only meaningful when it
	// rewrites the metadata.
	if srcBucket == bucket && srcKey == key && directive != "REPLACE" {
		writeError(w, r, errInvalidRequest)
		return
	}

	contentType := src.ContentType
	userMeta := src.UserMetadata
	if directive == "REPLACE" {
		contentType = r.Header.Get("Content-Type")
		userMeta = userMetadata(r.Header)
	}

	etag := md5.Sum(data)
	checksum := sha256.Sum256(data)

	vid, now := g.cfg.Meta.MintVersionID()
	atMS := now.UnixMilli()
	if _, err := g.cfg.Blobs.Put(vid, bytes.NewReader(data)); err != nil {
		writeError(w, r, errInternal)
		return
	}

	res, applyErr := g.cfg.Meta.ApplyPutObject(meta.PutObject{
		ProposedAtUnixMS: atMS,
		Bucket:           bucket,
		Key:              key,
		VersionID:        vid,
		Size:             int64(len(data)),
		ETag:             etag[:],
		ContentType:      contentType,
		UserMetadata:     userMeta,
		ObjectChecksum:   checksum[:],
	})
	if applyErr != nil {
		_ = g.cfg.Blobs.Remove(vid) // best effort; otherwise an orphan for GC
		writeError(w, r, applyErr)
		return
	}
	for _, dataID := range res.ReplacedDataIDs {
		_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
	}
	writeXML(w, http.StatusOK, copyObjectResult{
		Xmlns:        s3Xmlns,
		LastModified: iso8601(atMS),
		ETag:         quoteETag(etag[:]),
	})
}

// copyObjectCluster is CopyObject on the erasure-coded path: a server-side
// streaming read of the source straight into a destination PUT. The same
// windowed reader the GET path uses (over GetRange) feeds the streaming Put, so
// a copy of any size stays bounded in memory and never round-trips the bytes
// through the client. The destination is an ordinary single-part object — its
// ETag is the plain MD5 of the bytes, even when the source was multipart.
func (g *Gateway) copyObjectCluster(w http.ResponseWriter, r *http.Request, bucket, key, directive string) {
	if hasConditionalCopyHeaders(r.Header) {
		writeError(w, r, errNotImplemented)
		return
	}
	srcBucket, srcKey, err := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	src, err := g.lookupCurrent(srcBucket, srcKey)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if src.Kind == meta.KindDeleteMarker {
		writeError(w, r, errNoSuchKey) // the source's current version is a delete marker
		return
	}
	// S3 rule: copying an object onto itself is only meaningful when it rewrites
	// the metadata.
	if srcBucket == bucket && srcKey == key && directive != "REPLACE" {
		writeError(w, r, errInvalidRequest)
		return
	}

	contentType := src.ContentType
	userMeta := src.UserMetadata
	if directive == "REPLACE" {
		contentType = r.Header.Get("Content-Type")
		userMeta = userMetadata(r.Header)
	}

	// Stream the source object's bytes straight into the destination PUT — the
	// windowed reader pulls only the covering shards, the Put paces them through
	// erasure coding, so neither side buffers the object whole.
	srcReader := &rangeReader{
		size: src.Size,
		fetch: func(off, length int64) ([]byte, error) {
			return g.cfg.Objects.GetRange(src, off, length)
		},
	}
	etag, vid, err := g.cfg.Objects.Put(bucket, key, srcReader, src.Size, PutObjectOptions{
		ContentType:  contentType,
		UserMetadata: userMeta,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	var atMS int64
	if committed, ok := g.cfg.Meta.GetVersion(bucket, key, vid); ok {
		atMS = committed.CreatedUnixMS
	}
	writeXML(w, http.StatusOK, copyObjectResult{
		Xmlns:        s3Xmlns,
		LastModified: iso8601(atMS),
		ETag:         quoteETag(etag),
	})
}

type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// uploadPartCopy is S3 UploadPartCopy: UploadPart whose bytes come from an
// existing object, optionally narrowed by x-amz-copy-source-range.
func (g *Gateway) uploadPartCopy(w http.ResponseWriter, r *http.Request, bucket, key string, uid meta.VersionID) {
	n, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || n < 1 || n > meta.MaxPartNumber {
		writeError(w, r, meta.ErrInvalidPartNumber)
		return
	}
	if g.cfg.Objects != nil {
		g.uploadPartCopyCluster(w, r, bucket, key, uid, uint32(n))
		return
	}

	_, _, src, data, err := g.readCopySource(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if rangeHdr := r.Header.Get("x-amz-copy-source-range"); rangeHdr != "" {
		first, last, ok := parseCopyRange(rangeHdr, src.Size)
		if !ok {
			writeError(w, r, errInvalidArgument)
			return
		}
		data = data[first : last+1]
	}

	etag := md5.Sum(data)
	checksum := sha256.Sum256(data)

	if _, exists := g.cfg.Meta.GetUpload(bucket, key, uid); !exists {
		writeError(w, r, meta.ErrNoSuchUpload)
		return
	}
	dataID, now := g.cfg.Meta.MintVersionID()
	atMS := now.UnixMilli()
	if _, err := g.cfg.Blobs.Put(dataID, bytes.NewReader(data)); err != nil {
		writeError(w, r, errInternal)
		return
	}

	res, applyErr := g.cfg.Meta.ApplyUploadPart(meta.UploadPart{
		ProposedAtUnixMS: atMS,
		Bucket:           bucket,
		Key:              key,
		UploadID:         uid,
		PartNumber:       uint32(n),
		DataID:           dataID,
		Size:             int64(len(data)),
		ETag:             etag[:],
		Checksum:         checksum[:],
	})
	if applyErr != nil {
		_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
		writeError(w, r, applyErr)
		return
	}
	if !res.ReplacedDataID.IsZero() {
		_ = g.cfg.Blobs.Remove(res.ReplacedDataID) // best effort; otherwise an orphan for GC
	}
	writeXML(w, http.StatusOK, copyPartResult{
		Xmlns:        s3Xmlns,
		LastModified: iso8601(atMS),
		ETag:         quoteETag(etag[:]),
	})
}

// uploadPartCopyCluster is UploadPartCopy on the erasure-coded path: the part
// bytes come from an existing object's range, read through the data path and
// re-encoded straight into the part — streamed, never buffered whole, the same
// windowed reader the GET and CopyObject paths use. The coordinator commits the
// part row itself.
func (g *Gateway) uploadPartCopyCluster(w http.ResponseWriter, r *http.Request, bucket, key string, uid meta.VersionID, partNumber uint32) {
	if hasConditionalCopyHeaders(r.Header) {
		writeError(w, r, errNotImplemented)
		return
	}
	srcBucket, srcKey, err := parseCopySource(r.Header.Get("x-amz-copy-source"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	src, err := g.lookupCurrent(srcBucket, srcKey)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if src.Kind == meta.KindDeleteMarker {
		writeError(w, r, errNoSuchKey)
		return
	}
	if _, exists := g.cfg.Meta.GetUpload(bucket, key, uid); !exists {
		writeError(w, r, meta.ErrNoSuchUpload)
		return
	}

	// The copied span: the whole source, or the inclusive byte range the
	// x-amz-copy-source-range header narrows it to.
	first, length := int64(0), src.Size
	if rangeHdr := r.Header.Get("x-amz-copy-source-range"); rangeHdr != "" {
		lo, hi, ok := parseCopyRange(rangeHdr, src.Size)
		if !ok {
			writeError(w, r, errInvalidArgument)
			return
		}
		first, length = lo, hi-lo+1
	}

	// Stream the source span straight into the part PUT: the windowed reader
	// pulls only the covering shards, the part is erasure-coded as it arrives.
	srcReader := &rangeReader{
		size: length,
		fetch: func(off, n int64) ([]byte, error) {
			return g.cfg.Objects.GetRange(src, first+off, n)
		},
	}
	etag, err := g.cfg.Objects.PutPart(bucket, key, uid, partNumber, srcReader, length)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var atMS int64
	if up, ok := g.cfg.Meta.GetUpload(bucket, key, uid); ok {
		atMS = up.CreatedUnixMS
	}
	writeXML(w, http.StatusOK, copyPartResult{
		Xmlns:        s3Xmlns,
		LastModified: iso8601(atMS),
		ETag:         quoteETag(etag),
	})
}

// parseCopyRange parses x-amz-copy-source-range ("bytes=first-last", both
// inclusive) and bounds it against the source size.
func parseCopyRange(s string, size int64) (first, last int64, ok bool) {
	spec, found := strings.CutPrefix(s, "bytes=")
	if !found {
		return 0, 0, false
	}
	a, b, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}
	first, err1 := strconv.ParseInt(a, 10, 64)
	last, err2 := strconv.ParseInt(b, 10, 64)
	if err1 != nil || err2 != nil || first < 0 || last < first || last >= size {
		return 0, 0, false
	}
	return first, last, true
}
