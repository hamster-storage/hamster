package gateway

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestShedMapsTo429 proves the load-shedding refusal (ADR-0039 part 4) renders
// as HTTP 429 Too Many Requests with a Retry-After header and the distinct
// TooManyRequests S3 code — the retryable throttling response SDKs, rclone, and
// restic back off on.
func TestShedMapsTo429(t *testing.T) {
	// The error reaches the gateway wrapped, the way the cluster layer wraps a
	// coordinator ErrShed onto ErrTooManyRequests.
	wrapped := fmt.Errorf("at capacity: %w", ErrTooManyRequests)

	s3e := mapError(wrapped)
	if s3e.Status != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", s3e.Status)
	}
	if s3e.Code != "TooManyRequests" {
		t.Fatalf("code %q, want TooManyRequests", s3e.Code)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", nil)
	writeError(rec, req, wrapped)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("response status %d, want 429", rec.Code)
	}
	if ra := rec.Header().Get("Retry-After"); ra == "" {
		t.Fatal("429 response carried no Retry-After header")
	}
	var body errorResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("429 body is not a valid S3 error envelope: %v", err)
	}
	if body.Code != "TooManyRequests" {
		t.Fatalf("XML error code %q, want TooManyRequests", body.Code)
	}
}

// TestUnavailableStillMapsTo503 proves the durability-floor / non-leader refusal
// stays the existing 503 SlowDown, kept fully distinct from the 429 shed: a
// different code, a different status, and no Retry-After (ADR-0039 part 4 — "do
// not merge or replace the 503 path").
func TestUnavailableStillMapsTo503(t *testing.T) {
	wrapped := fmt.Errorf("below the floor: %w", ErrUnavailable)

	s3e := mapError(wrapped)
	if s3e.Status != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", s3e.Status)
	}
	if s3e.Code != "SlowDown" {
		t.Fatalf("code %q, want SlowDown", s3e.Code)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", nil)
	writeError(rec, req, wrapped)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("response status %d, want 503", rec.Code)
	}
	if ra := rec.Header().Get("Retry-After"); ra != "" {
		t.Fatalf("503 SlowDown carried a Retry-After header %q; that hint belongs to the 429", ra)
	}
}
