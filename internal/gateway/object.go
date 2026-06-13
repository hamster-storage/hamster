package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sigv4"
)

// putObject is S3 PutObject: stream the payload durably to disk under a
// freshly minted ID (validating it on the same pass), then commit the
// metadata — the commit is the linearization point, so the data must
// already be durable (docs/ARCHITECTURE.md).
func (g *Gateway) putObject(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket, key string) {
	if r.ContentLength > maxObjectSize {
		writeError(w, r, errEntityTooLarge)
		return
	}
	if g.cfg.Objects != nil {
		g.putObjectCluster(w, r, id, bucket, key)
		return
	}

	// Mint first — the blob needs its address before the body can stream
	// to disk.
	vid, now := g.cfg.Meta.MintVersionID()
	atMS := now.UnixMilli()

	size, etag, checksum, err := g.streamBody(r, id, vid)
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

	// The result's possibly-bumped VersionID becomes the
	// x-amz-version-id header when versioning is exposed (v0.5).
	res, applyErr := g.cfg.Meta.ApplyPutObject(meta.PutObject{
		ProposedAtUnixMS: atMS,
		Bucket:           bucket,
		Key:              key,
		VersionID:        vid,
		Size:             size,
		ETag:             etag,
		ContentType:      r.Header.Get("Content-Type"),
		UserMetadata:     userMetadata(r.Header),
		ObjectChecksum:   checksum,
	})
	if applyErr != nil {
		_ = g.cfg.Blobs.Remove(vid) // best effort; otherwise an orphan for GC
		writeError(w, r, applyErr)
		return
	}
	for _, dataID := range res.ReplacedDataIDs {
		_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
	}

	w.Header().Set("ETag", quoteETag(etag))
	w.WriteHeader(http.StatusOK)
}

// putObjectCluster is PutObject on the erasure-coded cluster path: the
// backend places, encodes, transfers, and commits — one call. The body is
// buffered whole (the v0.3 preview shape, like the single-node GET; the
// streaming pump arrives with the operational hardening).
func (g *Gateway) putObjectCluster(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket, key string) {
	body, err := readBody(r, id)
	if err != nil {
		if errors.Is(err, sigv4.ErrSignatureMismatch) || errors.Is(err, sigv4.ErrMalformed) {
			writeAuthError(w, r, err)
		} else {
			writeError(w, r, err)
		}
		return
	}
	etag, err := g.cfg.Objects.Put(bucket, key, body, r.Header.Get("Content-Type"), userMetadata(r.Header))
	if err != nil {
		writeError(w, r, err)
		return
	}
	w.Header().Set("ETag", quoteETag(etag))
	w.WriteHeader(http.StatusOK)
}

// lookupCurrent resolves a key's current version entry. The returned
// error is ready for writeError.
func (g *Gateway) lookupCurrent(bucket, key string) (meta.VersionEntry, error) {
	if _, ok := g.cfg.Meta.GetBucket(bucket); !ok {
		return meta.VersionEntry{}, meta.ErrNoSuchBucket
	}
	cur, ok := g.cfg.Meta.Current(bucket, key)
	if !ok {
		return meta.VersionEntry{}, errNoSuchKey
	}
	entry, found := g.cfg.Meta.GetVersion(bucket, key, cur.VersionID)
	if !found {
		return meta.VersionEntry{}, errNoSuchKey
	}
	return entry, nil
}

// readObjectData fetches and verifies an entry's bytes — the real
// integrity check (ADR-0019: ETags are compatibility, this is integrity),
// applied before a single byte is served or copied. Whole PUTs carry one
// object checksum; multipart objects carry one per part, verified as the
// body is assembled from the part blobs.
func (g *Gateway) readObjectData(entry meta.VersionEntry) ([]byte, error) {
	if len(entry.Parts) > 0 {
		data := make([]byte, 0, entry.Size)
		for _, p := range entry.Parts {
			part, err := g.cfg.Blobs.Get(p.DataID)
			if err != nil {
				return nil, errInternal
			}
			if sum := sha256.Sum256(part); !bytes.Equal(sum[:], p.Checksum) {
				return nil, errInternal
			}
			data = append(data, part...)
		}
		return data, nil
	}
	data, err := g.cfg.Blobs.Get(entry.DataID)
	if err != nil {
		return nil, errInternal
	}
	if sum := sha256.Sum256(data); !bytes.Equal(sum[:], entry.ObjectChecksum) {
		return nil, errInternal
	}
	return data, nil
}

// getObject serves GET and HEAD. Range and conditional headers are handled
// by http.ServeContent over the in-memory blob.
func (g *Gateway) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	var entry meta.VersionEntry
	var data []byte
	var err error
	if g.cfg.Objects != nil {
		// The cluster path: bytes and entry from one consistent read,
		// integrity carried by the frame's chunk CRCs end to end.
		data, entry, err = g.cfg.Objects.Get(bucket, key)
		if err != nil {
			writeError(w, r, err)
			return
		}
	} else {
		entry, err = g.lookupCurrent(bucket, key)
		if err != nil {
			writeError(w, r, err)
			return
		}
		data, err = g.readObjectData(entry)
		if err != nil {
			writeError(w, r, err)
			return
		}
	}

	w.Header().Set("ETag", objectETag(entry.ETag, len(entry.Parts)))
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
	// On the cluster path, shard reclaim needs the displaced version's
	// parameters; capture them before the delete (best effort — a racing
	// overwrite leaves an orphan for a future scan, never a wrong delete:
	// the apply's result says which data IDs were actually freed).
	var displaced meta.VersionEntry
	if g.cfg.Objects != nil {
		if cur, ok := g.cfg.Meta.Current(bucket, key); ok {
			displaced, _ = g.cfg.Meta.GetVersion(bucket, key, cur.VersionID)
		}
	}
	vid, now := g.cfg.Meta.MintVersionID()
	res, applyErr := g.cfg.Meta.ApplyDeleteObject(meta.DeleteObject{
		ProposedAtUnixMS: now.UnixMilli(),
		Bucket:           bucket,
		Key:              key,
		VersionID:        vid,
	})
	if applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	g.reclaim(res.RemovedDataIDs, displaced)
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

// objectETag renders a stored object ETag for the wire: hex, quoted, with
// the "-N" part-count suffix when the object was a multipart upload
// (ADR-0019 — the composite form sync tools verify).
func objectETag(etag []byte, partCount int) string {
	if partCount > 0 {
		return `"` + hex.EncodeToString(etag) + "-" + strconv.Itoa(partCount) + `"`
	}
	return quoteETag(etag)
}

// reclaim best-effort frees displaced data: blobs on the single-node
// path; shards through the cluster backend when the freed IDs belong to
// the captured entry. Anything missed is an orphan for a future scan,
// unreadable as an object because no metadata names it.
func (g *Gateway) reclaim(freed []meta.VersionID, displaced meta.VersionEntry) {
	if g.cfg.Objects == nil {
		for _, dataID := range freed {
			_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
		}
		return
	}
	if len(freed) > 0 && displaced.DataID == freed[0] {
		g.cfg.Objects.DeleteShards(displaced)
	}
}
