// Package gateway is the S3 HTTP front end: it authenticates requests
// (internal/sigv4), routes them to handlers, and turns metadata operations
// (internal/meta) plus blob data into S3 wire responses.
//
// Concurrency: HTTP handlers run on net/http's goroutines, but the metadata
// store and the version-ID rng are owned by the node's event loop (the seam
// contract — core state has a single logical thread). Every store access is
// therefore posted to the loop and awaited. Blob I/O stays off the loop:
// data bytes never block metadata.
//
// Shutdown order matters: stop the HTTP server before stopping the loop,
// because work posted to a stopped loop is discarded and its awaiting
// handler would never wake.
//
// Upload bodies stream to disk through the write buffer — bounded memory
// regardless of object size, with the ETag and object checksum computed on
// the same pass (ADR-0019).
//
// v0.1 shape, stated honestly: GET and server-side copy still buffer the
// object whole in memory — streaming reads arrive with the per-chunk
// integrity of the framed data stream (docs/DATA-STREAM.md), which is what
// makes verify-while-serving possible. And a failed request can orphan a
// blob — garbage collection arrives with the reserved g/ metadata prefix.
package gateway

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sigv4"
)

// BlobStore is the gateway's view of the data path: durable object storage
// addressed by data ID. internal/blob implements it for a single node; the
// erasure-coded path will implement it for a cluster.
type BlobStore interface {
	// Put streams data from r under id, durable when it returns, and
	// reports the byte count stored. On error — r's or the disk's — the
	// implementation cleans up its own staging (best effort) and returns
	// r's error wrapped, so the caller can classify it.
	Put(id meta.VersionID, r io.Reader) (int64, error)
	// Get returns the data stored under id.
	Get(id meta.VersionID) ([]byte, error)
	// Remove deletes the blob under id.
	Remove(id meta.VersionID) error
}

// Metadata is the gateway's view of the metadata plane. Implementations
// own their synchronization — every method is safe to call from request
// goroutines and returns a consistent read or a committed mutation. The
// single-node implementation (NewLoopMetadata) posts to the store's event
// loop; the cluster implementation proposes mutations through Raft and
// reads from the local replica.
//
// Listings page batch by batch, each batch its own consistent read —
// cross-batch mutations may show, exactly as S3's own paginated listings
// allow.
type Metadata interface {
	// MintVersionID mints a fresh version ID and returns the time it was
	// minted from — the proposal timestamp for the operation carrying it.
	MintVersionID() (meta.VersionID, time.Time)

	GetBucket(name string) (meta.BucketConfig, bool)
	ListBuckets() []meta.BucketConfig
	Current(bucket, key string) (meta.CurrentRecord, bool)
	GetVersion(bucket, key string, vid meta.VersionID) (meta.VersionEntry, bool)
	ListVersions(bucket, key string) []meta.VersionEntry
	ListObjects(bucket, prefix, startAfter string, max int) []meta.ObjectListing
	ListObjectVersions(bucket, prefix, keyMarker string, versionIDMarker meta.VersionID, max int) ([]meta.VersionListing, bool)
	GetUpload(bucket, key string, uid meta.VersionID) (meta.UploadRecord, bool)
	ListUploads(bucket, prefix, keyMarker string, uploadMarker meta.VersionID, max int) []meta.UploadListing
	ListUploadParts(bucket, key string, uid meta.VersionID, afterPart uint32, max int) ([]meta.PartRecord, bool)

	ApplyCreateBucket(meta.CreateBucket) error
	ApplyDeleteBucket(meta.DeleteBucket) error
	ApplySetBucketVersioning(meta.SetBucketVersioning) error
	ApplySetObjectLockConfiguration(meta.SetObjectLockConfiguration) error
	ApplyPutObject(meta.PutObject) (meta.PutResult, error)
	ApplyDeleteObject(meta.DeleteObject) (meta.DeleteObjectResult, error)
	ApplyDeleteVersion(meta.DeleteVersion) (meta.DeleteVersionResult, error)
	ApplyUpdateRetention(meta.UpdateRetention) error
	ApplyUpdateLegalHold(meta.UpdateLegalHold) error
	ApplyCreateMultipartUpload(meta.CreateMultipartUpload) error
	ApplyUploadPart(meta.UploadPart) (meta.UploadPartResult, error)
	ApplyCompleteMultipartUpload(meta.CompleteMultipartUpload) (meta.CompleteResult, error)
	ApplyAbortMultipartUpload(meta.AbortMultipartUpload) (meta.AbortResult, error)
}

// Config carries the gateway's dependencies. All fields are required except
// Domain.
type Config struct {
	// Region is the SigV4 scope region and the GetBucketLocation answer.
	Region string
	// Domain is the base domain for virtual-hosted addressing: a request
	// whose Host is <bucket>.<Domain> addresses that bucket, and its URL
	// path is the object key. Empty disables virtual-hosted addressing;
	// path-style always works either way.
	Domain string
	// Lookup resolves access key IDs to secrets.
	Lookup sigv4.CredentialLookup

	// Clock provides request time for SigV4 validity and proposal
	// timestamps.
	Clock seam.Clock
	// Meta is the metadata plane.
	Meta Metadata
	// Blobs is the data path.
	Blobs BlobStore
	// Objects, when non-nil, is the erasure-coded cluster data path for
	// whole objects: the handlers delegate puts and gets to it wholesale
	// (it places, encodes, transfers, and commits metadata itself),
	// deletes reclaim shards through it, and multipart plus server-side
	// copy are refused — their cluster data path is later work. Nil on a
	// single node, where Blobs serves object data.
	Objects ObjectBackend

	// EncryptionEnabled reports whether the cluster encrypts new writes at
	// rest (ADR-0021, the SSE-S3 surface). Nil — the single-node preview and
	// any unencrypted cluster — means it does not, so an explicit
	// x-amz-server-side-encryption: AES256 request is refused honestly rather
	// than silently storing plaintext.
	EncryptionEnabled func() bool
}

// PutObjectOptions carries the request facts a cluster PUT records beyond the
// body: content type, user metadata, and the object-lock fields from the
// x-amz-object-lock-* headers (ADR-0006).
type PutObjectOptions struct {
	ContentType       string
	UserMetadata      map[string]string
	RetentionMode     meta.RetentionMode
	RetainUntilUnixMS int64
	LegalHold         bool
}

// ObjectBackend is the cluster data path's face (internal/coord behind a
// cluster composition). Implementations own their synchronization, like
// Metadata. The gateway resolves a version entry through Metadata first, then
// serves its bytes through GetRange pinned to that entry — so the response
// headers and the body always describe the same version.
type ObjectBackend interface {
	Put(bucket, key string, body []byte, opts PutObjectOptions) (etag []byte, versionID meta.VersionID, err error)
	// GetRange serves the plaintext range [off, off+length) of a stored
	// version entry; a negative length means to the end of the object. It
	// fetches only the covering shards, so a Range read and a windowed
	// whole-object read both stay bounded in memory. The entry must be a
	// stored object — a delete marker holds no shards.
	GetRange(entry meta.VersionEntry, off, length int64) (data []byte, err error)
	// DeleteShards best-effort reclaims a displaced version's shards.
	DeleteShards(e meta.VersionEntry)
}

// Gateway is an http.Handler serving the S3 API.
type Gateway struct {
	cfg      Config
	verifier *sigv4.Verifier
	reqID    atomic.Uint64
}

// New returns a Gateway over cfg.
func New(cfg Config) *Gateway {
	cfg.Domain = strings.ToLower(cfg.Domain)
	return &Gateway{
		cfg:      cfg,
		verifier: &sigv4.Verifier{Region: cfg.Region, Lookup: cfg.Lookup},
	}
}

// refuseOnCluster answers 501 for operations whose cluster data path is
// not built yet (multipart, server-side copy — they ride the v0.1 blob
// path until their erasure-coded design lands). Refusing honestly beats
// committing metadata that names blobs no node stores.
func (g *Gateway) refuseOnCluster(w http.ResponseWriter, r *http.Request) bool {
	if g.cfg.Objects == nil {
		return false
	}
	writeError(w, r, errNotImplemented)
	return true
}

// loopMetadata is the single-node Metadata: a meta.Store and the version
// rng owned by an event loop, with every call posted and awaited. The
// loop is the synchronization; the methods themselves are one consistent
// trip each.
type loopMetadata struct {
	store *meta.Store
	loop  seam.Loop
	clock seam.Clock
	rng   *rand.Rand
}

// NewLoopMetadata wraps a loop-owned store and rng as the gateway's
// Metadata. The clock is read inside the loop, so minted IDs and their
// timestamps stay loop-ordered.
func NewLoopMetadata(store *meta.Store, loop seam.Loop, clock seam.Clock, rng *rand.Rand) Metadata {
	return &loopMetadata{store: store, loop: loop, clock: clock, rng: rng}
}

func (l *loopMetadata) on(fn func()) {
	done := make(chan struct{})
	l.loop.Post(func() {
		defer close(done)
		fn()
	})
	<-done
}

func (l *loopMetadata) MintVersionID() (vid meta.VersionID, now time.Time) {
	l.on(func() {
		now = l.clock.Now()
		vid = meta.NewVersionID(now, l.rng)
	})
	return
}

func (l *loopMetadata) GetBucket(name string) (cfg meta.BucketConfig, ok bool) {
	l.on(func() { cfg, ok = l.store.GetBucket(name) })
	return
}

func (l *loopMetadata) ListBuckets() (out []meta.BucketConfig) {
	l.on(func() { out = l.store.ListBuckets() })
	return
}

func (l *loopMetadata) Current(bucket, key string) (cur meta.CurrentRecord, ok bool) {
	l.on(func() { cur, ok = l.store.Current(bucket, key) })
	return
}

func (l *loopMetadata) GetVersion(bucket, key string, vid meta.VersionID) (e meta.VersionEntry, ok bool) {
	l.on(func() { e, ok = l.store.GetVersion(bucket, key, vid) })
	return
}

func (l *loopMetadata) ListVersions(bucket, key string) (out []meta.VersionEntry) {
	l.on(func() { out = l.store.ListVersions(bucket, key) })
	return
}

func (l *loopMetadata) ListObjects(bucket, prefix, startAfter string, max int) (out []meta.ObjectListing) {
	l.on(func() { out = l.store.ListObjects(bucket, prefix, startAfter, max) })
	return
}

func (l *loopMetadata) ListObjectVersions(bucket, prefix, keyMarker string, versionIDMarker meta.VersionID, max int) (out []meta.VersionListing, truncated bool) {
	l.on(func() { out, truncated = l.store.ListObjectVersions(bucket, prefix, keyMarker, versionIDMarker, max) })
	return
}

func (l *loopMetadata) GetUpload(bucket, key string, uid meta.VersionID) (up meta.UploadRecord, ok bool) {
	l.on(func() { up, ok = l.store.GetUpload(bucket, key, uid) })
	return
}

func (l *loopMetadata) ListUploads(bucket, prefix, keyMarker string, uploadMarker meta.VersionID, max int) (out []meta.UploadListing) {
	l.on(func() { out = l.store.ListUploads(bucket, prefix, keyMarker, uploadMarker, max) })
	return
}

func (l *loopMetadata) ListUploadParts(bucket, key string, uid meta.VersionID, afterPart uint32, max int) (parts []meta.PartRecord, ok bool) {
	l.on(func() { parts, ok = l.store.ListUploadParts(bucket, key, uid, afterPart, max) })
	return
}

func (l *loopMetadata) ApplyCreateBucket(p meta.CreateBucket) (err error) {
	l.on(func() { err = l.store.ApplyCreateBucket(p) })
	return
}

func (l *loopMetadata) ApplyDeleteBucket(p meta.DeleteBucket) (err error) {
	l.on(func() { err = l.store.ApplyDeleteBucket(p) })
	return
}

func (l *loopMetadata) ApplySetBucketVersioning(p meta.SetBucketVersioning) (err error) {
	l.on(func() { err = l.store.ApplySetBucketVersioning(p) })
	return
}

func (l *loopMetadata) ApplySetObjectLockConfiguration(p meta.SetObjectLockConfiguration) (err error) {
	l.on(func() { err = l.store.ApplySetObjectLockConfiguration(p) })
	return
}

func (l *loopMetadata) ApplyPutObject(p meta.PutObject) (res meta.PutResult, err error) {
	l.on(func() { res, err = l.store.ApplyPutObject(p) })
	return
}

func (l *loopMetadata) ApplyDeleteObject(p meta.DeleteObject) (res meta.DeleteObjectResult, err error) {
	l.on(func() { res, err = l.store.ApplyDeleteObject(p) })
	return
}

func (l *loopMetadata) ApplyDeleteVersion(p meta.DeleteVersion) (res meta.DeleteVersionResult, err error) {
	l.on(func() { res, err = l.store.ApplyDeleteVersion(p) })
	return
}

func (l *loopMetadata) ApplyUpdateRetention(p meta.UpdateRetention) (err error) {
	l.on(func() { err = l.store.ApplyUpdateRetention(p) })
	return
}

func (l *loopMetadata) ApplyUpdateLegalHold(p meta.UpdateLegalHold) (err error) {
	l.on(func() { err = l.store.ApplyUpdateLegalHold(p) })
	return
}

func (l *loopMetadata) ApplyCreateMultipartUpload(p meta.CreateMultipartUpload) (err error) {
	l.on(func() { err = l.store.ApplyCreateMultipartUpload(p) })
	return
}

func (l *loopMetadata) ApplyUploadPart(p meta.UploadPart) (res meta.UploadPartResult, err error) {
	l.on(func() { res, err = l.store.ApplyUploadPart(p) })
	return
}

func (l *loopMetadata) ApplyCompleteMultipartUpload(p meta.CompleteMultipartUpload) (res meta.CompleteResult, err error) {
	l.on(func() { res, err = l.store.ApplyCompleteMultipartUpload(p) })
	return
}

func (l *loopMetadata) ApplyAbortMultipartUpload(p meta.AbortMultipartUpload) (res meta.AbortResult, err error) {
	l.on(func() { res, err = l.store.ApplyAbortMultipartUpload(p) })
	return
}

// virtualHostBucket extracts the bucket from a virtual-hosted Host header.
// The match is the configured domain as a strict suffix; anything else — the
// bare domain, an IP, an unrelated name — falls through to path-style.
// Bucket names may contain dots, so everything before the domain belongs to
// the bucket (my.logs.s3.example.com → my.logs). SigV4 is unaffected: the
// Host header is signed as sent and the canonical path is the path as sent,
// whichever style the client chose.
func (g *Gateway) virtualHostBucket(host string) (string, bool) {
	if g.cfg.Domain == "" {
		return "", false
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	bucket, found := strings.CutSuffix(strings.ToLower(host), "."+g.cfg.Domain)
	if !found || bucket == "" {
		return "", false
	}
	return bucket, true
}

// splitPath splits a path-style request into bucket and key. Either or both
// may be empty: "/" is the service level, "/b" and "/b/" are bucket level.
func splitPath(p string) (bucket, key string) {
	p = strings.TrimPrefix(p, "/")
	bucket, key, _ = strings.Cut(p, "/")
	return bucket, key
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := fmt.Sprintf("%016X", g.reqID.Add(1))
	w.Header().Set("x-amz-request-id", reqID)

	id, err := g.verifier.Verify(r, g.cfg.Clock.Now())
	if err != nil {
		writeAuthError(w, r, err)
		return
	}

	bucket, key := splitPath(r.URL.Path)
	if vb, ok := g.virtualHostBucket(r.Host); ok {
		bucket, key = vb, strings.TrimPrefix(r.URL.Path, "/")
	}
	switch {
	case bucket == "":
		g.serveService(w, r)
	case key == "":
		g.serveBucket(w, r, id, bucket)
	default:
		g.serveObject(w, r, id, bucket, key)
	}
}

func (g *Gateway) serveService(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, r, errMethodNotAllowed)
		return
	}
	g.listBuckets(w, r)
}

func (g *Gateway) serveBucket(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket string) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodPut:
		if q.Has("versioning") {
			g.putBucketVersioning(w, r, id, bucket)
			return
		}
		if q.Has("object-lock") {
			g.putObjectLockConfiguration(w, r, id, bucket)
			return
		}
		if len(q) != 0 {
			writeError(w, r, errNotImplemented)
			return
		}
		g.createBucket(w, r, bucket)
	case http.MethodDelete:
		if len(q) != 0 {
			writeError(w, r, errNotImplemented)
			return
		}
		g.deleteBucket(w, r, bucket)
	case http.MethodHead:
		g.headBucket(w, r, bucket)
	case http.MethodGet:
		switch {
		case q.Has("location"):
			g.getBucketLocation(w, r, bucket)
		case q.Has("versioning"):
			g.getBucketVersioning(w, r, bucket)
		case q.Has("object-lock"):
			g.getObjectLockConfiguration(w, r, bucket)
		case q.Has("versions"):
			g.listObjectVersions(w, r, bucket)
		case q.Has("uploads"):
			g.listMultipartUploads(w, r, bucket)
		case q.Get("list-type") == "2":
			g.listObjectsV2(w, r, bucket)
		case hasSubresource(q):
			writeError(w, r, errNotImplemented)
		default:
			g.listObjectsV1(w, r, bucket)
		}
	case http.MethodPost:
		if q.Has("delete") {
			g.deleteObjects(w, r, id, bucket)
			return
		}
		writeError(w, r, errNotImplemented)
	default:
		writeError(w, r, errMethodNotAllowed)
	}
}

func (g *Gateway) serveObject(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket, key string) {
	if err := checkObjectKey(key); err != nil {
		writeError(w, r, err)
		return
	}
	q := r.URL.Query()
	if q.Has("uploads") {
		if r.Method != http.MethodPost {
			writeError(w, r, errMethodNotAllowed)
			return
		}
		g.createMultipartUpload(w, r, bucket, key)
		return
	}
	if q.Has("uploadId") {
		uid, ok := parseUploadID(q.Get("uploadId"))
		if !ok {
			// An ID this server never minted: the upload cannot exist.
			writeError(w, r, meta.ErrNoSuchUpload)
			return
		}
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("x-amz-copy-source") != "" {
				g.uploadPartCopy(w, r, bucket, key, uid)
				return
			}
			g.uploadPart(w, r, id, bucket, key, uid)
		case http.MethodPost:
			g.completeMultipartUpload(w, r, id, bucket, key, uid)
		case http.MethodGet:
			g.listParts(w, r, bucket, key, uid)
		case http.MethodDelete:
			g.abortMultipartUpload(w, r, bucket, key, uid)
		default:
			writeError(w, r, errMethodNotAllowed)
		}
		return
	}
	if q.Has("retention") {
		switch r.Method {
		case http.MethodGet:
			g.getObjectRetention(w, r, bucket, key)
		case http.MethodPut:
			g.putObjectRetention(w, r, id, bucket, key)
		default:
			writeError(w, r, errMethodNotAllowed)
		}
		return
	}
	if q.Has("legal-hold") {
		switch r.Method {
		case http.MethodGet:
			g.getObjectLegalHold(w, r, bucket, key)
		case http.MethodPut:
			g.putObjectLegalHold(w, r, id, bucket, key)
		default:
			writeError(w, r, errMethodNotAllowed)
		}
		return
	}
	if hasSubresource(q) {
		writeError(w, r, errNotImplemented)
		return
	}
	switch r.Method {
	case http.MethodPut:
		if r.Header.Get("x-amz-copy-source") != "" {
			g.copyObject(w, r, bucket, key)
			return
		}
		g.putObject(w, r, id, bucket, key)
	case http.MethodGet, http.MethodHead:
		g.getObject(w, r, bucket, key)
	case http.MethodDelete:
		g.deleteObject(w, r, bucket, key)
	default:
		writeError(w, r, errMethodNotAllowed)
	}
}

// hasSubresource reports whether the query addresses an S3 subresource or
// operation the gateway does not implement yet (?acl, ?tagging, ...).
// Subresources that are implemented (?versioning, ?uploads, ?location) are
// routed before this check. SigV4 query parameters and listing parameters are
// not subresources.
func hasSubresource(q map[string][]string) bool {
	for k := range q {
		switch k {
		case "list-type", "prefix", "delimiter", "max-keys", "encoding-type",
			"continuation-token", "start-after", "marker", "fetch-owner",
			"response-content-type", "response-content-disposition":
			continue
		case "versionId":
			// Not a subresource: a parameter selecting a version on the
			// ordinary GET/HEAD/DELETE verbs (v0.5).
			continue
		case "x-id":
			// aws-sdk-go-v2 (rclone among others) tags every request with
			// its operation name; real S3 ignores it.
			continue
		}
		if strings.HasPrefix(k, "X-Amz-") {
			continue // presigned-URL auth parameters
		}
		return true
	}
	return false
}

// checkObjectKey enforces the documented key rules at the request layer,
// before any proposal exists (METADATA.md): 1–1024 bytes, no NUL byte.
func checkObjectKey(key string) error {
	if len(key) > 1024 {
		return errKeyTooLong
	}
	if strings.ContainsRune(key, '\x00') {
		return errInvalidObjectName
	}
	return nil
}

// Payload limits. Object bodies stream to disk through the write buffer
// and are bounded by S3's single-payload limit (S3-API.md: single PUT and
// individual parts up to 5 GiB). Control bodies — the XML of DeleteObjects
// and CompleteMultipartUpload — are read whole and bounded far lower: the
// largest legitimate one (a 10,000-part complete) is under 1 MiB.
const (
	maxObjectSize  = 5 << 30
	maxControlBody = 4 << 20
)

// bodyReader unwraps a request's payload reader according to its SigV4
// identity: chunked streams are decoded with their per-chunk signatures
// verified as they are read (a tampered chunk surfaces as a read error),
// other payloads are read as sent.
func bodyReader(r *http.Request, id *sigv4.Identity) io.Reader {
	if id.Streaming {
		return id.ChunkedBody(r.Body)
	}
	return r.Body
}

// streamBody stores a request's payload under dataID — streamed to disk
// through the write buffer, never held whole in memory — and validates it:
// signed single-shot payloads against the declared SHA-256, a Content-MD5
// header (when supplied) against the payload. The MD5 ETag and SHA-256
// object checksum are computed on the same pass (ADR-0019: no extra read).
//
// Validation runs after the bytes are durable, which is safe because only
// the metadata commit makes data visible: on any failure the blob is
// removed and the request never had an object. The error comes back ready
// to classify — sigv4 errors wrapped, S3 errors as themselves.
func (g *Gateway) streamBody(r *http.Request, id *sigv4.Identity, dataID meta.VersionID) (size int64, etag, checksum []byte, err error) {
	md5h, sha := md5.New(), sha256.New()
	src := io.TeeReader(&capReader{r: bodyReader(r, id), remaining: maxObjectSize}, io.MultiWriter(md5h, sha))
	size, err = g.cfg.Blobs.Put(dataID, src)
	if err != nil {
		return 0, nil, nil, err
	}
	etag, checksum = md5h.Sum(nil), sha.Sum(nil)
	if !id.Streaming && id.PayloadHash != sigv4.UnsignedPayload && id.PayloadHash != hex.EncodeToString(checksum) {
		err = errContentSHA256Mismatch
	} else {
		err = checkContentMD5(r.Header.Get("Content-MD5"), etag)
	}
	if err != nil {
		_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
		return 0, nil, nil, err
	}
	return size, etag, checksum, nil
}

// capReader fails the read once more than its limit has passed through —
// the backstop for the 5 GiB payload limit now that bodies stream to disk
// instead of dying in memory first.
type capReader struct {
	r         io.Reader
	remaining int64
}

func (c *capReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.remaining -= int64(n)
	if c.remaining < 0 {
		return n, errEntityTooLarge
	}
	return n, err
}

// readBody reads and validates a small control payload (request XML)
// according to its SigV4 identity: chunked streams are unwrapped with
// per-chunk signatures verified, signed single-shot payloads are checked
// against the declared SHA-256, unsigned payloads are accepted as sent. A
// Content-MD5 header, when supplied, is verified against the decoded
// payload (enforced, not advisory — it is free integrity; it is not
// *required*, because checksum-era SDKs send x-amz-checksum-* instead).
func readBody(r *http.Request, id *sigv4.Identity) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(bodyReader(r, id), maxControlBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxControlBody {
		return nil, errEntityTooLarge
	}
	sum := sha256.Sum256(body)
	if !id.Streaming && id.PayloadHash != sigv4.UnsignedPayload && id.PayloadHash != hex.EncodeToString(sum[:]) {
		return nil, errContentSHA256Mismatch
	}
	md5sum := md5.Sum(body)
	if err := checkContentMD5(r.Header.Get("Content-MD5"), md5sum[:]); err != nil {
		return nil, err
	}
	return body, nil
}

// checkContentMD5 verifies a supplied Content-MD5 header (base64 of the
// raw 16-byte digest) against the payload's computed MD5.
func checkContentMD5(header string, got []byte) error {
	if header == "" {
		return nil
	}
	want, err := base64.StdEncoding.DecodeString(header)
	if err != nil || len(want) != md5.Size {
		return errInvalidDigest
	}
	if !bytes.Equal(got, want) {
		return errBadDigest
	}
	return nil
}
