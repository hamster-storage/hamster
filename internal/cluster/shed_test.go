package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/gateway"
)

// TestMapCoordErrShedVsRefused proves the cluster→gateway error mapping keeps the
// two retryable refusals distinct (ADR-0039 part 4): a coordinator shed becomes
// the gateway's 429 ErrTooManyRequests, while the durability-floor refusal and
// the unreadable read stay the existing 503 ErrUnavailable. The wrapped
// coordinator error's identity survives the mapping in both directions.
func TestMapCoordErrShedVsRefused(t *testing.T) {
	shed := mapCoordErr(fmt.Errorf("admission: %w", coord.ErrShed))
	if !errors.Is(shed, gateway.ErrTooManyRequests) {
		t.Fatalf("ErrShed mapped to %v, want gateway.ErrTooManyRequests (429)", shed)
	}
	if errors.Is(shed, gateway.ErrUnavailable) {
		t.Fatal("ErrShed leaked into the 503 ErrUnavailable path; the two must stay distinct")
	}

	for _, e := range []error{coord.ErrRefused, coord.ErrUnreadable} {
		mapped := mapCoordErr(fmt.Errorf("floor: %w", e))
		if !errors.Is(mapped, gateway.ErrUnavailable) {
			t.Fatalf("%v mapped to %v, want gateway.ErrUnavailable (503)", e, mapped)
		}
		if errors.Is(mapped, gateway.ErrTooManyRequests) {
			t.Fatalf("%v leaked into the 429 path; it must stay 503", e)
		}
	}

	// An unrelated error passes through untouched for the 500 it is.
	other := errors.New("boom")
	if got := mapCoordErr(other); !errors.Is(got, other) {
		t.Fatalf("an unrelated error was rewritten: %v", got)
	}
}
