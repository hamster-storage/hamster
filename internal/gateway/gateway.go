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
// v0.1 shape, stated honestly: object bodies are buffered whole in memory
// (seam.Disk is whole-file until the write buffer lands), addressing is
// path-style only, and deleting or overwriting an object orphans its blob —
// garbage collection arrives with the reserved g/ metadata prefix.
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
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sigv4"
)

// BlobStore is the gateway's view of the data path: durable whole-object
// storage addressed by data ID. internal/blob implements it for a single
// node; the erasure-coded path will implement it for a cluster.
type BlobStore interface {
	// Put stores data under id, durable when it returns.
	Put(id meta.VersionID, data []byte) error
	// Get returns the data stored under id.
	Get(id meta.VersionID) ([]byte, error)
	// Remove deletes the blob under id.
	Remove(id meta.VersionID) error
}

// Config carries the gateway's dependencies. All fields are required.
type Config struct {
	// Region is the SigV4 scope region and the GetBucketLocation answer.
	Region string
	// Lookup resolves access key IDs to secrets.
	Lookup sigv4.CredentialLookup

	// Store is the metadata store, owned by Loop: the gateway touches it
	// only from posted functions.
	Store *meta.Store
	// Loop is the event loop that owns Store and Rand.
	Loop seam.Loop
	// Clock provides request time for SigV4 validity and version minting.
	Clock seam.Clock
	// Rand mints version IDs. Owned by Loop, like the store.
	Rand *rand.Rand
	// Blobs is the data path.
	Blobs BlobStore
}

// Gateway is an http.Handler serving the S3 API.
type Gateway struct {
	cfg      Config
	verifier *sigv4.Verifier
	reqID    atomic.Uint64
}

// New returns a Gateway over cfg.
func New(cfg Config) *Gateway {
	return &Gateway{
		cfg:      cfg,
		verifier: &sigv4.Verifier{Region: cfg.Region, Lookup: cfg.Lookup},
	}
}

// onLoop runs fn on the metadata loop and waits for it to finish.
func (g *Gateway) onLoop(fn func()) {
	done := make(chan struct{})
	g.cfg.Loop.Post(func() {
		defer close(done)
		fn()
	})
	<-done
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
// operation the gateway does not implement yet (?acl, ?tagging, ?uploads,
// ?versioning, ...). SigV4 query parameters and listing parameters are not
// subresources.
func hasSubresource(q map[string][]string) bool {
	for k := range q {
		switch k {
		case "list-type", "prefix", "delimiter", "max-keys", "encoding-type",
			"continuation-token", "start-after", "marker", "fetch-owner",
			"response-content-type", "response-content-disposition":
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

// readBody reads and validates a request's payload according to its SigV4
// identity: chunked streams are unwrapped and their per-chunk signatures
// verified (a tampered chunk surfaces as the reader's error); signed
// single-shot payloads are checked against the declared SHA-256; unsigned
// payloads are accepted as sent. A Content-MD5 header, when supplied, is
// verified against the decoded payload (enforced, not advisory — it is
// free integrity; it is not *required*, because checksum-era SDKs send
// x-amz-checksum-* instead). Returns the payload and its SHA-256, which
// PutObject records as the object checksum.
func readBody(r *http.Request, id *sigv4.Identity) ([]byte, []byte, error) {
	var body []byte
	var err error
	if id.Streaming {
		body, err = io.ReadAll(id.ChunkedBody(r.Body))
	} else {
		body, err = io.ReadAll(r.Body)
	}
	if err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(body)
	if !id.Streaming && id.PayloadHash != sigv4.UnsignedPayload && id.PayloadHash != hex.EncodeToString(sum[:]) {
		return nil, nil, errContentSHA256Mismatch
	}
	if err := checkContentMD5(r.Header.Get("Content-MD5"), body); err != nil {
		return nil, nil, err
	}
	return body, sum[:], nil
}

// checkContentMD5 verifies a supplied Content-MD5 header (base64 of the
// raw 16-byte digest) against the payload.
func checkContentMD5(header string, body []byte) error {
	if header == "" {
		return nil
	}
	want, err := base64.StdEncoding.DecodeString(header)
	if err != nil || len(want) != md5.Size {
		return errInvalidDigest
	}
	if sum := md5.Sum(body); !bytes.Equal(sum[:], want) {
		return errBadDigest
	}
	return nil
}
