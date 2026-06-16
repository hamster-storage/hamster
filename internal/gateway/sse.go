package gateway

import (
	"net/http"
	"strings"

	"github.com/hamster-storage/hamster/internal/meta"
)

// The SSE-S3 surface (ADR-0021). Hamster manages encryption keys
// cluster-wide, so the server-managed SSE-S3 shape is the one it exposes:
// AES256 on the standard header. SSE-KMS and customer-provided keys (SSE-C)
// are refused honestly; the per-object DEK machinery leaves room for SSE-C
// later. The cluster's replicated posture — not the request header — decides
// whether an object is actually encrypted, so a request that asks for
// encryption the server cannot provide is refused rather than silently
// stored as plaintext.

const sseHeader = "x-amz-server-side-encryption"

// parseSSEHeaders validates a write's server-side-encryption request headers.
// encryptionOn reports whether this server actually encrypts at rest. An
// explicit AES256 request is accepted only when it does; aws:kms and any SSE-C
// customer-key header is refused.
func parseSSEHeaders(h http.Header, encryptionOn bool) error {
	for k := range h {
		if strings.HasPrefix(strings.ToLower(k), sseHeader+"-customer-") {
			return errSSECNotSupported
		}
	}
	switch v := h.Get(sseHeader); {
	case v == "":
		return nil
	case strings.EqualFold(v, "aws:kms"):
		return errSSEKMSNotSupported
	case !strings.EqualFold(v, "AES256"):
		return errInvalidSSE
	case !encryptionOn:
		// AES256 asked for, but this server does not encrypt — refuse rather
		// than store plaintext under the impression it is protected.
		return errSSENotEnabled
	default:
		return nil
	}
}

// setSSEHeader sets the SSE response header when the served version is
// encrypted at rest — the header AWS clients read back on PUT, GET, and HEAD.
func setSSEHeader(w http.ResponseWriter, e meta.VersionEntry) {
	if e.EncAlgorithm == meta.EncAES256GCM {
		w.Header().Set(sseHeader, "AES256")
	}
}

// encryptionOn reports the cluster's encryption posture, tolerating an unset
// callback (the single-node preview and any unencrypted cluster).
func (g *Gateway) encryptionOn() bool {
	return g.cfg.EncryptionEnabled != nil && g.cfg.EncryptionEnabled()
}
