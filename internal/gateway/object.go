package gateway

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sigv4"
)

// putObject is S3 PutObject: read and validate the payload, write the blob
// durably under a freshly minted ID, then commit the metadata — the commit
// is the linearization point, so the data must already be durable
// (docs/ARCHITECTURE.md).
func (g *Gateway) putObject(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket, key string) {
	body, checksum, err := readBody(r, id)
	if err != nil {
		// A tampered or truncated chunk stream is an authentication
		// failure, not a bad argument.
		if errors.Is(err, sigv4.ErrSignatureMismatch) || errors.Is(err, sigv4.ErrMalformed) {
			writeAuthError(w, r, err)
		} else {
			writeError(w, r, err)
		}
		return
	}
	etag := md5.Sum(body)

	// Mint on the loop: the version-ID rng is loop-owned state.
	var vid meta.VersionID
	var atMS int64
	g.onLoop(func() {
		now := g.cfg.Clock.Now()
		atMS = now.UnixMilli()
		vid = meta.NewVersionID(now, g.cfg.Rand)
	})

	if err := g.cfg.Blobs.Put(vid, body); err != nil {
		writeError(w, r, errInternal)
		return
	}

	var applyErr error
	var replaced meta.VersionID
	g.onLoop(func() {
		// A PUT to a bucket without versioning enabled replaces the key's
		// null version; capture its data address so the blob can be
		// reclaimed after the commit.
		if cfg, ok := g.cfg.Store.GetBucket(bucket); ok && cfg.Versioning != meta.VersioningEnabled {
			for _, v := range g.cfg.Store.ListVersions(bucket, key) {
				if v.NullVersion && v.Kind == meta.KindObject {
					replaced = v.DataID
					break
				}
			}
		}
		// The result's possibly-bumped VersionID becomes the
		// x-amz-version-id header when versioning is exposed (v0.5).
		_, applyErr = g.cfg.Store.ApplyPutObject(meta.PutObject{
			ProposedAtUnixMS: atMS,
			Bucket:           bucket,
			Key:              key,
			VersionID:        vid,
			Size:             int64(len(body)),
			ETag:             etag[:],
			ContentType:      r.Header.Get("Content-Type"),
			UserMetadata:     userMetadata(r.Header),
			ObjectChecksum:   checksum,
		})
	})
	if applyErr != nil {
		_ = g.cfg.Blobs.Remove(vid) // best effort; otherwise an orphan for GC
		writeError(w, r, applyErr)
		return
	}
	if !replaced.IsZero() {
		_ = g.cfg.Blobs.Remove(replaced) // best effort; otherwise an orphan for GC
	}

	w.Header().Set("ETag", quoteETag(etag[:]))
	w.WriteHeader(http.StatusOK)
}

// getObject serves GET and HEAD. Range and conditional headers are handled
// by http.ServeContent over the in-memory blob.
func (g *Gateway) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	var entry meta.VersionEntry
	var bucketOK, found bool
	g.onLoop(func() {
		if _, ok := g.cfg.Store.GetBucket(bucket); !ok {
			return
		}
		bucketOK = true
		cur, ok := g.cfg.Store.Current(bucket, key)
		if !ok {
			return
		}
		entry, found = g.cfg.Store.GetVersion(bucket, key, cur.VersionID)
	})
	if !bucketOK {
		writeError(w, r, meta.ErrNoSuchBucket)
		return
	}
	if !found {
		writeError(w, r, errNoSuchKey)
		return
	}

	data, err := g.cfg.Blobs.Get(entry.DataID)
	if err != nil {
		writeError(w, r, errInternal)
		return
	}
	// The real integrity check (ADR-0019: ETags are compatibility, this is
	// integrity): verify the stored checksum before serving a single byte.
	if sum := sha256.Sum256(data); !bytes.Equal(sum[:], entry.ObjectChecksum) {
		writeError(w, r, errInternal)
		return
	}

	w.Header().Set("ETag", quoteETag(entry.ETag))
	if entry.ContentType != "" {
		w.Header().Set("Content-Type", entry.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	for k, v := range entry.UserMetadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	http.ServeContent(w, r, "", time.UnixMilli(entry.CreatedUnixMS).UTC(), bytes.NewReader(data))
}

// deleteObject is S3 DeleteObject without a version ID. Idempotent: deleting
// a missing key is 204, exactly like S3.
func (g *Gateway) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	var res meta.DeleteObjectResult
	var applyErr error
	var removed meta.VersionID
	g.onLoop(func() {
		now := g.cfg.Clock.Now()
		// Capture the current version's data address: if the delete
		// removes a row (unversioned bucket), its blob can be reclaimed.
		if cur, ok := g.cfg.Store.Current(bucket, key); ok {
			if e, ok := g.cfg.Store.GetVersion(bucket, key, cur.VersionID); ok {
				removed = e.DataID
			}
		}
		res, applyErr = g.cfg.Store.ApplyDeleteObject(meta.DeleteObject{
			ProposedAtUnixMS: now.UnixMilli(),
			Bucket:           bucket,
			Key:              key,
			VersionID:        meta.NewVersionID(now, g.cfg.Rand),
		})
	})
	if applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	if res.Removed && !removed.IsZero() {
		_ = g.cfg.Blobs.Remove(removed) // best effort; otherwise an orphan for GC
	}
	if res.MarkerCreated {
		w.Header().Set("x-amz-delete-marker", "true")
	}
	w.WriteHeader(http.StatusNoContent)
}

// userMetadata collects x-amz-meta-* headers. Header names reach us
// canonicalized; S3 metadata names are case-insensitive and returned
// lowercase.
func userMetadata(h http.Header) map[string]string {
	var m map[string]string
	for name, values := range h {
		lower := strings.ToLower(name)
		if rest, ok := strings.CutPrefix(lower, "x-amz-meta-"); ok && len(values) > 0 {
			if m == nil {
				m = make(map[string]string)
			}
			m[rest] = values[0]
		}
	}
	return m
}

func quoteETag(etag []byte) string {
	return `"` + hex.EncodeToString(etag) + `"`
}
