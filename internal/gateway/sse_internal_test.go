package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamster-storage/hamster/internal/meta"
)

// TestParseSSEHeaders covers every branch of the SSE-S3 request validation
// (ADR-0021): AES256 accepted only when the server encrypts, KMS and SSE-C
// refused, a bad value rejected, and no header always fine.
func TestParseSSEHeaders(t *testing.T) {
	hdr := func(kv ...string) http.Header {
		h := http.Header{}
		for i := 0; i+1 < len(kv); i += 2 {
			h.Set(kv[i], kv[i+1])
		}
		return h
	}
	cases := []struct {
		name    string
		h       http.Header
		encOn   bool
		wantErr error
	}{
		{"none", hdr(), false, nil},
		{"none-encrypting", hdr(), true, nil},
		{"aes256 when on", hdr(sseHeader, "AES256"), true, nil},
		{"aes256 lowercase when on", hdr(sseHeader, "aes256"), true, nil},
		{"aes256 when off", hdr(sseHeader, "AES256"), false, errSSENotEnabled},
		{"kms", hdr(sseHeader, "aws:kms"), true, errSSEKMSNotSupported},
		{"bad value", hdr(sseHeader, "rot13"), true, errInvalidSSE},
		{"customer key", hdr(sseHeader+"-customer-algorithm", "AES256"), true, errSSECNotSupported},
		{"customer key off", hdr(sseHeader+"-customer-key", "abc"), false, errSSECNotSupported},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseSSEHeaders(c.h, c.encOn); got != c.wantErr {
				t.Fatalf("parseSSEHeaders = %v, want %v", got, c.wantErr)
			}
		})
	}
}

// TestSetSSEHeader: the response header is set only for an encrypted version.
func TestSetSSEHeader(t *testing.T) {
	for _, c := range []struct {
		alg  meta.EncAlgorithm
		want string
	}{
		{meta.EncNone, ""},
		{meta.EncAES256GCM, "AES256"},
	} {
		w := httptest.NewRecorder()
		setSSEHeader(w, meta.VersionEntry{EncAlgorithm: c.alg})
		if got := w.Header().Get(sseHeader); got != c.want {
			t.Errorf("alg %d: header %q, want %q", c.alg, got, c.want)
		}
	}
}

// TestEncryptionOn: the gateway tolerates an unset posture callback.
func TestEncryptionOn(t *testing.T) {
	if (&Gateway{}).encryptionOn() {
		t.Error("nil EncryptionEnabled should report off")
	}
	on := &Gateway{cfg: Config{EncryptionEnabled: func() bool { return true }}}
	if !on.encryptionOn() {
		t.Error("EncryptionEnabled true should report on")
	}
}
