package gateway

import (
	"encoding/xml"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sigv4"
)

// Object lock (ADR-0006): the bucket-level configuration (?object-lock) and
// per-object retention (?retention, the x-amz-object-lock-* PUT/response headers).
// Legal holds (the ?legal-hold subresource) follow in a later v0.6 pass.

// retentionModeString renders an object-lock mode for the wire. RetentionNone has
// no S3 spelling — it means "no lock".
func retentionModeString(m meta.RetentionMode) string {
	switch m {
	case meta.RetentionGovernance:
		return "GOVERNANCE"
	case meta.RetentionCompliance:
		return "COMPLIANCE"
	}
	return ""
}

// parseRetentionMode parses an S3 object-lock mode. ok is false for any value
// that is not GOVERNANCE or COMPLIANCE (including the empty string).
func parseRetentionMode(s string) (meta.RetentionMode, bool) {
	switch s {
	case "GOVERNANCE":
		return meta.RetentionGovernance, true
	case "COMPLIANCE":
		return meta.RetentionCompliance, true
	}
	return meta.RetentionNone, false
}

// defaultRetentionXML is the bucket default retention rule: a mode plus exactly
// one of Days or Years (the S3 shape — never an absolute date).
type defaultRetentionXML struct {
	Mode  string `xml:"Mode,omitempty"`
	Days  uint32 `xml:"Days,omitempty"`
	Years uint32 `xml:"Years,omitempty"`
}

type objectLockRuleXML struct {
	DefaultRetention *defaultRetentionXML `xml:"DefaultRetention,omitempty"`
}

// objectLockConfiguration is the ?object-lock subresource body, for both
// PutObjectLockConfiguration and GetObjectLockConfiguration.
type objectLockConfiguration struct {
	XMLName           xml.Name           `xml:"ObjectLockConfiguration"`
	Xmlns             string             `xml:"xmlns,attr,omitempty"`
	ObjectLockEnabled string             `xml:"ObjectLockEnabled,omitempty"`
	Rule              *objectLockRuleXML `xml:"Rule,omitempty"`
}

func (g *Gateway) getObjectLockConfiguration(w http.ResponseWriter, r *http.Request, bucket string) {
	cfg, ok := g.cfg.Meta.GetBucket(bucket)
	if !ok {
		writeError(w, r, meta.ErrNoSuchBucket)
		return
	}
	if !cfg.ObjectLockEnabled {
		writeError(w, r, errObjectLockNotFound)
		return
	}
	out := objectLockConfiguration{Xmlns: s3Xmlns, ObjectLockEnabled: "Enabled"}
	if cfg.DefaultRetentionMode != meta.RetentionNone {
		out.Rule = &objectLockRuleXML{DefaultRetention: &defaultRetentionXML{
			Mode:  retentionModeString(cfg.DefaultRetentionMode),
			Days:  cfg.DefaultRetentionDays,
			Years: cfg.DefaultRetentionYears,
		}}
	}
	writeXML(w, http.StatusOK, out)
}

func (g *Gateway) putObjectLockConfiguration(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket string) {
	cfg, ok := g.cfg.Meta.GetBucket(bucket)
	if !ok {
		writeError(w, r, meta.ErrNoSuchBucket)
		return
	}
	if !cfg.ObjectLockEnabled {
		// Object lock must be enabled at bucket creation; it cannot be turned on
		// here. Refusing is honest — silently enabling would change durability
		// semantics behind the operator's back.
		writeError(w, r, errObjectLockNotEnabled)
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
	var req objectLockConfiguration
	if xml.Unmarshal(body, &req) != nil {
		writeError(w, r, errMalformedXML)
		return
	}
	if req.ObjectLockEnabled != "" && req.ObjectLockEnabled != "Enabled" {
		writeError(w, r, errMalformedXML)
		return
	}
	var mode meta.RetentionMode
	var days, years uint32
	if req.Rule != nil && req.Rule.DefaultRetention != nil {
		dr := req.Rule.DefaultRetention
		m, ok := parseRetentionMode(dr.Mode)
		if !ok {
			writeError(w, r, errMalformedXML)
			return
		}
		// Exactly one of Days or Years, both positive — the apply layer enforces
		// this too, but a malformed request is a 400, not a 500.
		if (dr.Days == 0) == (dr.Years == 0) {
			writeError(w, r, errMalformedXML)
			return
		}
		mode, days, years = m, dr.Days, dr.Years
	}
	if applyErr := g.cfg.Meta.ApplySetObjectLockConfiguration(meta.SetObjectLockConfiguration{
		ProposedAtUnixMS:      g.cfg.Clock.Now().UnixMilli(),
		Bucket:                bucket,
		DefaultRetentionMode:  mode,
		DefaultRetentionDays:  days,
		DefaultRetentionYears: years,
	}); applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// parseLockHeaders reads the x-amz-object-lock-* request headers on a PUT. Mode
// and retain-until come as a pair (both or neither); legal hold is ON/OFF. An
// empty set is the zero values. A malformed pair is an error.
func parseLockHeaders(h http.Header) (mode meta.RetentionMode, retainUntil int64, legalHold bool, err error) {
	modeStr := h.Get("x-amz-object-lock-mode")
	dateStr := h.Get("x-amz-object-lock-retain-until-date")
	if modeStr != "" || dateStr != "" {
		m, ok := parseRetentionMode(modeStr)
		if !ok || dateStr == "" {
			return 0, 0, false, errInvalidArgument
		}
		t, perr := time.Parse(time.RFC3339, dateStr)
		if perr != nil {
			return 0, 0, false, errInvalidArgument
		}
		mode, retainUntil = m, t.UnixMilli()
	}
	legalHold = strings.EqualFold(h.Get("x-amz-object-lock-legal-hold"), "ON")
	return mode, retainUntil, legalHold, nil
}

// hasLockHeaders reports whether a request carries any x-amz-object-lock-* PUT
// header — used to refuse them on the cluster path until pass 4 plumbs the lock
// fields through coord.Put.
func hasLockHeaders(h http.Header) bool {
	return h.Get("x-amz-object-lock-mode") != "" ||
		h.Get("x-amz-object-lock-retain-until-date") != "" ||
		h.Get("x-amz-object-lock-legal-hold") != ""
}

// setLockHeaders writes the x-amz-object-lock-* response headers for a version's
// retention and legal-hold state on GET/HEAD.
func setLockHeaders(w http.ResponseWriter, e meta.VersionEntry) {
	if e.RetentionMode != meta.RetentionNone {
		w.Header().Set("x-amz-object-lock-mode", retentionModeString(e.RetentionMode))
		w.Header().Set("x-amz-object-lock-retain-until-date", iso8601(e.RetainUntilUnixMS))
	}
	if e.LegalHold {
		w.Header().Set("x-amz-object-lock-legal-hold", "ON")
	} else {
		w.Header().Set("x-amz-object-lock-legal-hold", "OFF")
	}
}

// resolveTarget resolves the version an object-level lock operation addresses:
// a specific ?versionId, or the current version.
func (g *Gateway) resolveTarget(bucket, key, versionID string) (meta.VersionEntry, error) {
	if versionID != "" {
		return g.resolveVersion(bucket, key, versionID)
	}
	return g.lookupCurrent(bucket, key)
}

// retentionXML is the ?retention subresource body, for both PutObjectRetention
// and GetObjectRetention.
type retentionXML struct {
	XMLName         xml.Name `xml:"Retention"`
	Xmlns           string   `xml:"xmlns,attr,omitempty"`
	Mode            string   `xml:"Mode,omitempty"`
	RetainUntilDate string   `xml:"RetainUntilDate,omitempty"`
}

func (g *Gateway) getObjectRetention(w http.ResponseWriter, r *http.Request, bucket, key string) {
	entry, err := g.resolveTarget(bucket, key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	if entry.RetentionMode == meta.RetentionNone {
		writeError(w, r, errNoRetention)
		return
	}
	writeXML(w, http.StatusOK, retentionXML{
		Xmlns:           s3Xmlns,
		Mode:            retentionModeString(entry.RetentionMode),
		RetainUntilDate: iso8601(entry.RetainUntilUnixMS),
	})
}

func (g *Gateway) putObjectRetention(w http.ResponseWriter, r *http.Request, id *sigv4.Identity, bucket, key string) {
	entry, err := g.resolveTarget(bucket, key, r.URL.Query().Get("versionId"))
	if err != nil {
		writeError(w, r, err)
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
	var req retentionXML
	if xml.Unmarshal(body, &req) != nil {
		writeError(w, r, errMalformedXML)
		return
	}
	mode, ok := parseRetentionMode(req.Mode)
	if !ok {
		writeError(w, r, errMalformedXML)
		return
	}
	t, perr := time.Parse(time.RFC3339, req.RetainUntilDate)
	if perr != nil {
		writeError(w, r, errMalformedXML)
		return
	}
	if applyErr := g.cfg.Meta.ApplyUpdateRetention(meta.UpdateRetention{
		ProposedAtUnixMS:  g.cfg.Clock.Now().UnixMilli(),
		Bucket:            bucket,
		Key:               key,
		VersionID:         entry.VersionID,
		Mode:              mode,
		RetainUntilUnixMS: t.UnixMilli(),
		BypassGovernance:  strings.EqualFold(r.Header.Get("x-amz-bypass-governance-retention"), "true"),
	}); applyErr != nil {
		writeError(w, r, applyErr)
		return
	}
	w.WriteHeader(http.StatusOK)
}
