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

	mode, retainUntil, legalHold, lerr := parseLockHeaders(r.Header)
	if lerr != nil {
		writeError(w, r, lerr)
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
		ProposedAtUnixMS:  atMS,
		Bucket:            bucket,
		Key:               key,
		VersionID:         vid,
		Size:              size,
		ETag:              etag,
		ContentType:       r.Header.Get("Content-Type"),
		UserMetadata:      userMetadata(r.Header),
		ObjectChecksum:    checksum,
		RetentionMode:     mode,
		RetainUntilUnixMS: retainUntil,
		LegalHold:         legalHold,
	})
	if applyErr != nil {
		_ = g.cfg.Blobs.Remove(vid) // best effort; otherwise an orphan for GC
		writeError(w, r, applyErr)
		return
	}
	for _, dataID := range res.ReplacedDataIDs {
		_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
	}

	state := g.bucketVersioning(bucket)
	w.Header().Set("ETag", quoteETag(etag))
	setVersionID(w, state, meta.VersionEntry{VersionID: res.VersionID, NullVersion: state != meta.VersioningEnabled})
	w.WriteHeader(http.StatusOK)
}

// putObjectCluster is PutObject on the erasure-coded cluster path: the
// backend places, encodes, transfers, and commits — one call. The body is
// buffered whole (the v0.3 preview shape, like the single-node GET; the
// streaming pump arrives with the operational hardening).
func (g *Gateway) putObjectCluster(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket, key string) {
	// Explicit per-object lock headers need the lock fields threaded through
	// coord.Put (v0.6 pass 4); refuse them honestly for now. A bucket default
	// retention still applies — it is set in the apply layer, which both paths
	// share — and retention can be set after the PUT via PutObjectRetention.
	if hasLockHeaders(r.Header) {
		writeError(w, r, errNotImplemented)
		return
	}
	body, err := readBody(r, id)
	if err != nil {
		if errors.Is(err, sigv4.ErrSignatureMismatch) || errors.Is(err, sigv4.ErrMalformed) {
			writeAuthError(w, r, err)
		} else {
			writeError(w, r, err)
		}
		return
	}
	etag, vid, err := g.cfg.Objects.Put(bucket, key, body, r.Header.Get("Content-Type"), userMetadata(r.Header))
	if err != nil {
		writeError(w, r, err)
		return
	}
	state := g.bucketVersioning(bucket)
	w.Header().Set("ETag", quoteETag(etag))
	setVersionID(w, state, meta.VersionEntry{VersionID: vid, NullVersion: state != meta.VersioningEnabled})
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
// by http.ServeContent over the in-memory blob. ?versionId selects a specific
// version; without it, the current version is served.
func (g *Gateway) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	versionID := r.URL.Query().Get("versionId")

	if g.cfg.Objects != nil {
		state := g.bucketVersioning(bucket)
		if versionID != "" {
			entry, err := g.resolveVersion(bucket, key, versionID)
			if err != nil {
				writeError(w, r, err)
				return
			}
			if entry.Kind == meta.KindDeleteMarker {
				w.Header().Set("x-amz-delete-marker", "true")
				setVersionID(w, state, entry)
				w.Header().Set("Last-Modified", time.UnixMilli(entry.CreatedUnixMS).UTC().Format(http.TimeFormat))
				writeError(w, r, errMethodNotAllowed)
				return
			}
			data, err := g.cfg.Objects.GetVersion(bucket, key, entry.VersionID)
			if err != nil {
				writeError(w, r, err)
				return
			}
			setVersionID(w, state, entry)
			g.serveEntry(w, r, entry, data)
			return
		}
		data, entry, err := g.cfg.Objects.Get(bucket, key)
		if err != nil {
			writeError(w, r, err)
			return
		}
		setVersionID(w, state, entry)
		g.serveEntry(w, r, entry, data)
		return
	}

	state := g.bucketVersioning(bucket)
	var entry meta.VersionEntry
	var err error
	if versionID != "" {
		entry, err = g.resolveVersion(bucket, key, versionID)
	} else {
		entry, err = g.lookupCurrent(bucket, key)
	}
	if err != nil {
		writeError(w, r, err)
		return
	}
	// A delete marker has no body: a GET or HEAD addressing one is 405, with the
	// marker's identity in the headers so a client can tell deleted from absent.
	if entry.Kind == meta.KindDeleteMarker {
		w.Header().Set("x-amz-delete-marker", "true")
		setVersionID(w, state, entry)
		w.Header().Set("Last-Modified", time.UnixMilli(entry.CreatedUnixMS).UTC().Format(http.TimeFormat))
		writeError(w, r, errMethodNotAllowed)
		return
	}
	data, err := g.readObjectData(entry)
	if err != nil {
		writeError(w, r, err)
		return
	}
	setVersionID(w, state, entry)
	g.serveEntry(w, r, entry, data)
}

// serveEntry writes an object version's headers and body. The caller sets any
// version-id header first (the cluster path does not yet, single-node does).
func (g *Gateway) serveEntry(w http.ResponseWriter, r *http.Request, entry meta.VersionEntry, data []byte) {
	w.Header().Set("ETag", objectETag(entry.ETag, len(entry.Parts)))
	if entry.ContentType != "" {
		w.Header().Set("Content-Type", entry.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	for k, v := range entry.UserMetadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}
	setLockHeaders(w, entry)
	w.Header().Set("Accept-Ranges", "bytes")
	http.ServeContent(w, r, "", time.UnixMilli(entry.CreatedUnixMS).UTC(), bytes.NewReader(data))
}

// deleteObject is S3 DeleteObject. With ?versionId it permanently destroys that
// one version; without, it removes the object on an unversioned bucket or inserts
// a delete marker on a versioned one. Idempotent: deleting a missing key or
// version is 204, exactly like S3.
func (g *Gateway) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if versionID := r.URL.Query().Get("versionId"); versionID != "" {
		g.deleteObjectVersion(w, r, bucket, key, versionID)
		return
	}
	state := g.bucketVersioning(bucket)
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
		// A suspended bucket's marker is the null version; an enabled bucket's
		// carries its minted ID.
		setVersionID(w, state, meta.VersionEntry{VersionID: res.MarkerID, NullVersion: state == meta.VersioningSuspended})
	}
	w.WriteHeader(http.StatusNoContent)
}

// deleteObjectVersion permanently removes one version (S3 DELETE with a version
// ID) — the operation that frees a version's data. A missing version is an
// idempotent 204.
func (g *Gateway) deleteObjectVersion(w http.ResponseWriter, r *http.Request, bucket, key, versionID string) {
	entry, err := g.resolveVersion(bucket, key, versionID)
	if errors.Is(err, meta.ErrNoSuchVersion) {
		// Deleting a version that is not there echoes the requested id, 204.
		w.Header().Set("x-amz-version-id", versionID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		writeError(w, r, err)
		return
	}
	res, applyErr := g.cfg.Meta.ApplyDeleteVersion(meta.DeleteVersion{
		ProposedAtUnixMS: g.cfg.Clock.Now().UnixMilli(),
		Bucket:           bucket,
		Key:              key,
		VersionID:        entry.VersionID,
		BypassGovernance: strings.EqualFold(r.Header.Get("x-amz-bypass-governance-retention"), "true"),
	})
	if applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	// Only an object version holds data; a delete marker frees nothing. Reclaim
	// shards on the cluster path, blobs on the single node.
	if res.Removed && entry.Kind == meta.KindObject {
		if g.cfg.Objects != nil {
			g.cfg.Objects.DeleteShards(entry)
		} else {
			for _, dataID := range entry.DataIDs() {
				_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
			}
		}
	}
	w.Header().Set("x-amz-version-id", versionLabel(entry))
	if entry.Kind == meta.KindDeleteMarker {
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

// versionIDString is the wire form of a version ID: the 32-hex-char string S3
// carries in x-amz-version-id and ?versionId. The literal "null" addresses the
// null version separately (versionLabel and resolveVersion handle it).
func versionIDString(id meta.VersionID) string { return hex.EncodeToString(id[:]) }

// parseVersionID decodes the hex wire form. "null" is not an ID and is rejected
// here; resolveVersion handles it before reaching this.
func parseVersionID(s string) (meta.VersionID, bool) {
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != len(meta.VersionID{}) {
		return meta.VersionID{}, false
	}
	var id meta.VersionID
	copy(id[:], raw)
	return id, true
}

// versionLabel is how a version names itself on the wire: "null" for the null
// version (the one a bucket holds while unversioned or suspended), the hex ID
// otherwise.
func versionLabel(e meta.VersionEntry) string {
	if e.NullVersion {
		return "null"
	}
	return versionIDString(e.VersionID)
}

// setVersionID writes x-amz-version-id per S3's rules: nothing at all on a bucket
// that was never versioned (Unversioned), else the version's label.
func setVersionID(w http.ResponseWriter, state meta.VersioningState, e meta.VersionEntry) {
	if state == meta.Unversioned {
		return
	}
	w.Header().Set("x-amz-version-id", versionLabel(e))
}

// bucketVersioning reads a bucket's versioning state, defaulting to Unversioned
// for a missing bucket (the handlers surface NoSuchBucket on their own path).
func (g *Gateway) bucketVersioning(bucket string) meta.VersioningState {
	if cfg, ok := g.cfg.Meta.GetBucket(bucket); ok {
		return cfg.Versioning
	}
	return meta.Unversioned
}

// resolveVersion finds the version a ?versionId selects: the null version by
// scan, or the hex ID by direct lookup. The error is writeError-ready
// (NoSuchBucket, or NoSuchVersion when nothing matches).
func (g *Gateway) resolveVersion(bucket, key, versionID string) (meta.VersionEntry, error) {
	if _, ok := g.cfg.Meta.GetBucket(bucket); !ok {
		return meta.VersionEntry{}, meta.ErrNoSuchBucket
	}
	if versionID == "null" {
		for _, e := range g.cfg.Meta.ListVersions(bucket, key) {
			if e.NullVersion {
				return e, nil
			}
		}
		return meta.VersionEntry{}, meta.ErrNoSuchVersion
	}
	vid, ok := parseVersionID(versionID)
	if !ok {
		return meta.VersionEntry{}, meta.ErrNoSuchVersion
	}
	e, found := g.cfg.Meta.GetVersion(bucket, key, vid)
	if !found {
		return meta.VersionEntry{}, meta.ErrNoSuchVersion
	}
	return e, nil
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
