package gateway

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sigv4"
)

// Multipart upload handlers (docs/S3-API.md). Every SDK switches to
// multipart automatically above ~8 MiB, so this is the path most real
// uploads take. Parts follow the PutObject discipline — blob durable
// first, metadata commit second — and CompleteMultipartUpload is a single
// metadata transaction that makes the assembled object visible.

// parseUploadID decodes the wire form of an upload ID: the 32-hex-char
// encoding of the minted 16-byte ID.
func parseUploadID(s string) (meta.VersionID, bool) {
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != 16 {
		return meta.VersionID{}, false
	}
	var id meta.VersionID
	copy(id[:], raw)
	return id, true
}

func uploadIDString(id meta.VersionID) string { return hex.EncodeToString(id[:]) }

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

func (g *Gateway) createMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if g.refuseOnCluster(w, r) {
		return
	}
	uid, now := g.cfg.Meta.MintVersionID()
	applyErr := g.cfg.Meta.ApplyCreateMultipartUpload(meta.CreateMultipartUpload{
		ProposedAtUnixMS: now.UnixMilli(),
		Bucket:           bucket,
		Key:              key,
		UploadID:         uid,
		ContentType:      r.Header.Get("Content-Type"),
		UserMetadata:     userMetadata(r.Header),
	})
	if applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Xmlns: s3Xmlns, Bucket: bucket, Key: key, UploadID: uploadIDString(uid),
	})
}

// uploadPart is S3 UploadPart: the part body follows the PutObject
// discipline — streamed durably to disk under a minted data ID and
// validated on the same pass, then committed as a part row. Re-uploading a
// part number displaces the prior part; its blob is reclaimed after the
// commit.
func (g *Gateway) uploadPart(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket, key string, uid meta.VersionID) {
	n, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || n < 1 || n > meta.MaxPartNumber {
		writeError(w, r, meta.ErrInvalidPartNumber)
		return
	}
	partNumber := uint32(n)
	if r.ContentLength > maxObjectSize {
		writeError(w, r, errEntityTooLarge)
		return
	}

	// Check the upload before paying for the blob write — a part aimed at
	// a nonexistent upload writes nothing. The authoritative check is the
	// apply; this one just saves the work.
	if _, exists := g.cfg.Meta.GetUpload(bucket, key, uid); !exists {
		writeError(w, r, meta.ErrNoSuchUpload)
		return
	}
	dataID, now := g.cfg.Meta.MintVersionID()
	atMS := now.UnixMilli()

	size, etag, checksum, err := g.streamBody(r, id, dataID)
	if err != nil {
		if errors.Is(err, sigv4.ErrSignatureMismatch) || errors.Is(err, sigv4.ErrMalformed) {
			writeAuthError(w, r, err)
		} else {
			writeError(w, r, err)
		}
		return
	}

	res, applyErr := g.cfg.Meta.ApplyUploadPart(meta.UploadPart{
		ProposedAtUnixMS: atMS,
		Bucket:           bucket,
		Key:              key,
		UploadID:         uid,
		PartNumber:       partNumber,
		DataID:           dataID,
		Size:             size,
		ETag:             etag,
		Checksum:         checksum,
	})
	if applyErr != nil {
		_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
		writeError(w, r, applyErr)
		return
	}
	if !res.ReplacedDataID.IsZero() {
		_ = g.cfg.Blobs.Remove(res.ReplacedDataID) // best effort; otherwise an orphan for GC
	}
	w.Header().Set("ETag", quoteETag(etag))
	w.WriteHeader(http.StatusOK)
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// completeMultipartUpload assembles the upload. The client's part list is
// parsed here; the authoritative validation against the stored parts
// happens inside apply, where no time-of-check gap exists.
func (g *Gateway) completeMultipartUpload(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket, key string, uid meta.VersionID) {
	body, err := readBody(r, id)
	if err != nil {
		if errors.Is(err, sigv4.ErrSignatureMismatch) || errors.Is(err, sigv4.ErrMalformed) {
			writeAuthError(w, r, err)
		} else {
			writeError(w, r, err)
		}
		return
	}
	var req completeMultipartUploadRequest
	if xml.Unmarshal(body, &req) != nil || len(req.Parts) == 0 {
		writeError(w, r, errMalformedXML)
		return
	}

	parts := make([]meta.CompletedPart, len(req.Parts))
	md5s := make([]byte, 0, md5.Size*len(req.Parts))
	for i, p := range req.Parts {
		if p.PartNumber < 1 || p.PartNumber > meta.MaxPartNumber {
			writeError(w, r, meta.ErrInvalidPartNumber)
			return
		}
		raw, err := hex.DecodeString(strings.Trim(strings.TrimSpace(p.ETag), `"`))
		if err != nil || len(raw) != md5.Size {
			writeError(w, r, meta.ErrInvalidPart)
			return
		}
		parts[i] = meta.CompletedPart{PartNumber: uint32(p.PartNumber), ETag: raw}
		md5s = append(md5s, raw...)
	}
	// The multipart composite (ADR-0019): MD5 of the concatenated binary
	// part MD5s, rendered hex plus "-N" — what every sync tool verifies.
	composite := md5.Sum(md5s)

	vid, now := g.cfg.Meta.MintVersionID()
	res, applyErr := g.cfg.Meta.ApplyCompleteMultipartUpload(meta.CompleteMultipartUpload{
		ProposedAtUnixMS: now.UnixMilli(),
		Bucket:           bucket,
		Key:              key,
		UploadID:         uid,
		VersionID:        vid,
		ETag:             composite[:],
		Parts:            parts,
	})
	if applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	// DiscardedDataIDs carries unused parts and any replaced null version.
	for _, dataID := range res.DiscardedDataIDs {
		_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
	}
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Xmlns:    s3Xmlns,
		Location: "http://" + r.Host + "/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     objectETag(composite[:], len(parts)),
	})
}

func (g *Gateway) abortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string, uid meta.VersionID) {
	res, applyErr := g.cfg.Meta.ApplyAbortMultipartUpload(meta.AbortMultipartUpload{
		ProposedAtUnixMS: g.cfg.Clock.Now().UnixMilli(),
		Bucket:           bucket,
		Key:              key,
		UploadID:         uid,
	})
	if applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	for _, dataID := range res.PartDataIDs {
		_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
	}
	w.WriteHeader(http.StatusNoContent)
}

type partEntry struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type listPartsResult struct {
	XMLName              xml.Name    `xml:"ListPartsResult"`
	Xmlns                string      `xml:"xmlns,attr"`
	Bucket               string      `xml:"Bucket"`
	Key                  string      `xml:"Key"`
	UploadID             string      `xml:"UploadId"`
	InitiatorID          string      `xml:"Initiator>ID"`
	OwnerID              string      `xml:"Owner>ID"`
	StorageClass         string      `xml:"StorageClass"`
	PartNumberMarker     int         `xml:"PartNumberMarker"`
	NextPartNumberMarker int         `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int         `xml:"MaxParts"`
	IsTruncated          bool        `xml:"IsTruncated"`
	Parts                []partEntry `xml:"Part"`
}

func (g *Gateway) listParts(w http.ResponseWriter, r *http.Request, bucket, key string, uid meta.VersionID) {
	q := r.URL.Query()
	maxParts, ok := parseMaxKeys(q.Get("max-parts"))
	if !ok {
		writeError(w, r, errInvalidArgument)
		return
	}
	marker := 0
	if s := q.Get("part-number-marker"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			writeError(w, r, errInvalidArgument)
			return
		}
		marker = min(n, meta.MaxPartNumber)
	}

	parts, exists := g.cfg.Meta.ListUploadParts(bucket, key, uid, uint32(marker), maxParts+1)
	if !exists {
		writeError(w, r, meta.ErrNoSuchUpload)
		return
	}
	truncated := len(parts) > maxParts
	if truncated {
		parts = parts[:maxParts]
	}

	out := listPartsResult{
		Xmlns:            s3Xmlns,
		Bucket:           bucket,
		Key:              key,
		UploadID:         uploadIDString(uid),
		InitiatorID:      "hamster",
		OwnerID:          "hamster",
		StorageClass:     "STANDARD",
		PartNumberMarker: marker,
		MaxParts:         maxParts,
		IsTruncated:      truncated,
	}
	if truncated {
		out.NextPartNumberMarker = int(parts[len(parts)-1].PartNumber)
	}
	for _, p := range parts {
		out.Parts = append(out.Parts, partEntry{
			PartNumber:   int(p.PartNumber),
			LastModified: iso8601(p.UploadedUnixMS),
			ETag:         quoteETag(p.ETag),
			Size:         p.Size,
		})
	}
	writeXML(w, http.StatusOK, out)
}

type uploadEntry struct {
	Key          string `xml:"Key"`
	UploadID     string `xml:"UploadId"`
	InitiatorID  string `xml:"Initiator>ID"`
	OwnerID      string `xml:"Owner>ID"`
	StorageClass string `xml:"StorageClass"`
	Initiated    string `xml:"Initiated"`
}

type listMultipartUploadsResult struct {
	XMLName            xml.Name      `xml:"ListMultipartUploadsResult"`
	Xmlns              string        `xml:"xmlns,attr"`
	Bucket             string        `xml:"Bucket"`
	KeyMarker          string        `xml:"KeyMarker"`
	UploadIDMarker     string        `xml:"UploadIdMarker"`
	NextKeyMarker      string        `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string        `xml:"NextUploadIdMarker,omitempty"`
	Prefix             string        `xml:"Prefix"`
	MaxUploads         int           `xml:"MaxUploads"`
	EncodingType       string        `xml:"EncodingType,omitempty"`
	IsTruncated        bool          `xml:"IsTruncated"`
	Uploads            []uploadEntry `xml:"Upload"`
}

func (g *Gateway) listMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	if q.Get("delimiter") != "" {
		// Honest refusal: delimiter grouping for uploads is unimplemented,
		// and no tool in the compatibility set sends it.
		writeError(w, r, errNotImplemented)
		return
	}
	prefix := q.Get("prefix")
	encode := q.Get("encoding-type") == "url"
	maxUploads, ok := parseMaxKeys(q.Get("max-uploads"))
	if !ok {
		writeError(w, r, errInvalidArgument)
		return
	}
	keyMarker := q.Get("key-marker")
	var uidMarker meta.VersionID
	// S3 semantics: upload-id-marker is meaningful only with key-marker.
	if s := q.Get("upload-id-marker"); s != "" && keyMarker != "" {
		if uidMarker, ok = parseUploadID(s); !ok {
			writeError(w, r, errInvalidArgument)
			return
		}
	}

	if _, ok := g.cfg.Meta.GetBucket(bucket); !ok {
		writeError(w, r, meta.ErrNoSuchBucket)
		return
	}
	ls := g.cfg.Meta.ListUploads(bucket, prefix, keyMarker, uidMarker, maxUploads+1)
	truncated := len(ls) > maxUploads
	if truncated {
		ls = ls[:maxUploads]
	}

	out := listMultipartUploadsResult{
		Xmlns:          s3Xmlns,
		Bucket:         bucket,
		KeyMarker:      listEncode(keyMarker, encode),
		UploadIDMarker: q.Get("upload-id-marker"),
		Prefix:         listEncode(prefix, encode),
		MaxUploads:     maxUploads,
		IsTruncated:    truncated,
	}
	if encode {
		out.EncodingType = "url"
	}
	if truncated {
		last := ls[len(ls)-1]
		out.NextKeyMarker = listEncode(last.Key, encode)
		out.NextUploadIDMarker = uploadIDString(last.Upload.UploadID)
	}
	for _, u := range ls {
		out.Uploads = append(out.Uploads, uploadEntry{
			Key:          listEncode(u.Key, encode),
			UploadID:     uploadIDString(u.Upload.UploadID),
			InitiatorID:  "hamster",
			OwnerID:      "hamster",
			StorageClass: "STANDARD",
			Initiated:    iso8601(u.Upload.CreatedUnixMS),
		})
	}
	writeXML(w, http.StatusOK, out)
}
