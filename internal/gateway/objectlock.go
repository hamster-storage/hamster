package gateway

import (
	"encoding/xml"
	"errors"
	"net/http"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sigv4"
)

// Object lock (ADR-0006): the bucket-level configuration surface. Per-object
// retention and legal holds (the ?retention and ?legal-hold subresources) follow
// in later v0.6 passes; the retention-mode helpers here are shared with them.

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
