package main

import (
	"strings"
	"testing"
)

// TestDownsizeWarning: a drain that crosses a storage-profile boundary is
// flagged with the right before/after profile and the real per-step trade; a
// same-profile drain is not flagged at all.
func TestDownsizeWarning(t *testing.T) {
	// 6→5: 4+2 → 3+2 — same 2-failure tolerance, only efficiency changes.
	msg, ds := downsizeWarning("n6", 6)
	if !ds {
		t.Fatal("6→5 should be a downsize (4+2 no longer fits on 5)")
	}
	if !strings.Contains(msg, "4+2 to 3+2") || !strings.Contains(msg, "unchanged") {
		t.Fatalf("6→5 warning wrong:\n%s", msg)
	}

	// 8→7 and 7→6: still 4+2 (width 6 fits) — not a downsize, no prompt.
	if _, ds := downsizeWarning("n8", 8); ds {
		t.Fatal("8→7 should not be a downsize")
	}
	if _, ds := downsizeWarning("n7", 7); ds {
		t.Fatal("7→6 should not be a downsize (4+2 fits on 6)")
	}

	// 5→4: 3+2 → 2+1 — tolerance drops from 2 to 1, which must be called out.
	msg, ds = downsizeWarning("n5", 5)
	if !ds || !strings.Contains(msg, "3+2 to 2+1") || !strings.Contains(msg, "REDUCED") {
		t.Fatalf("5→4 should reduce durability:\n%s", msg)
	}
}
