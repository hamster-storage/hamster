package gateway

import (
	"encoding/xml"
	"errors"
	"net/http"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sigv4"
)

// DeleteObjects (POST /bucket?delete). S3 batch delete is explicitly not
// atomic: every key is its own delete with its own outcome, reported per
// key in the response — exactly the proposal model, so the handler is a
// loop over ApplyDeleteObject. Up to 1000 keys, like S3.

const maxDeleteObjects = 1000

type deleteObjectsRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
	} `xml:"Object"`
}

type deletedEntry struct {
	Key          string `xml:"Key"`
	DeleteMarker bool   `xml:"DeleteMarker,omitempty"`
}

type deleteErrorEntry struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type deleteObjectsResult struct {
	XMLName xml.Name           `xml:"DeleteResult"`
	Xmlns   string             `xml:"xmlns,attr"`
	Deleted []deletedEntry     `xml:"Deleted"`
	Errors  []deleteErrorEntry `xml:"Error"`
}

func (g *Gateway) deleteObjects(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket string) {
	body, err := readBody(r, id)
	if err != nil {
		if errors.Is(err, sigv4.ErrSignatureMismatch) || errors.Is(err, sigv4.ErrMalformed) {
			writeAuthError(w, r, err)
		} else {
			writeError(w, r, err)
		}
		return
	}
	var req deleteObjectsRequest
	if xml.Unmarshal(body, &req) != nil || len(req.Objects) == 0 || len(req.Objects) > maxDeleteObjects {
		writeError(w, r, errMalformedXML)
		return
	}

	if _, ok := g.cfg.Meta.GetBucket(bucket); !ok {
		writeError(w, r, meta.ErrNoSuchBucket)
		return
	}

	// One delete per key, each its own committed mutation — S3 batch
	// delete is explicitly not atomic, and the result reports what each
	// delete freed.
	out := deleteObjectsResult{Xmlns: s3Xmlns}
	for _, o := range req.Objects {
		if o.VersionID != "" {
			// Version-addressed deletes arrive with the versioning API
			// (v0.5); deleting the current version instead would be a
			// silent wrong answer.
			s3e := mapError(errNotImplemented)
			out.Errors = append(out.Errors, deleteErrorEntry{Key: o.Key, Code: s3e.Code, Message: s3e.Message})
			continue
		}
		vid, now := g.cfg.Meta.MintVersionID()
		res, err := g.cfg.Meta.ApplyDeleteObject(meta.DeleteObject{
			ProposedAtUnixMS: now.UnixMilli(),
			Bucket:           bucket,
			Key:              o.Key,
			VersionID:        vid,
		})
		if err != nil {
			s3e := mapError(err)
			out.Errors = append(out.Errors, deleteErrorEntry{Key: o.Key, Code: s3e.Code, Message: s3e.Message})
			continue
		}
		for _, dataID := range res.RemovedDataIDs {
			_ = g.cfg.Blobs.Remove(dataID) // best effort; otherwise an orphan for GC
		}
		if !req.Quiet {
			out.Deleted = append(out.Deleted, deletedEntry{Key: o.Key, DeleteMarker: res.MarkerCreated})
		}
	}
	writeXML(w, http.StatusOK, out)
}
