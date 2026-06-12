package gateway

import (
	"errors"
	"io"
	"strings"
	"testing"
)

// The 5 GiB payload limit cannot be pushed through HTTP in a unit test, so
// the backstop reader is proven directly: bytes up to the limit flow
// through untouched, the read that crosses it fails with EntityTooLarge.
func TestCapReader(t *testing.T) {
	under, err := io.ReadAll(&capReader{r: strings.NewReader("12345678"), remaining: 8})
	if err != nil || string(under) != "12345678" {
		t.Fatalf("at the limit: %q, %v", under, err)
	}

	_, err = io.ReadAll(&capReader{r: strings.NewReader("123456789"), remaining: 8})
	var s3e *s3Error
	if !errors.As(err, &s3e) || s3e != errEntityTooLarge {
		t.Fatalf("over the limit: %v, want EntityTooLarge", err)
	}
}
