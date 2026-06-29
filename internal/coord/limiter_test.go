package coord

import "testing"

// TestLimiterAcquireRelease: tryAcquire admits up to the current limit and then
// sheds; release frees a slot. Pure accounting, no coordinator.
func TestLimiterAcquireRelease(t *testing.T) {
	l := newLimiter()
	l.limit = 3 // pin a small limit so the boundary is easy to hit

	for i := 0; i < 3; i++ {
		if !l.tryAcquire() {
			t.Fatalf("acquire %d: shed below the limit (inflight=%d limit=%d)", i, l.inflight, l.limit)
		}
	}
	if l.inflight != 3 {
		t.Fatalf("inflight=%d, want 3", l.inflight)
	}
	// At the limit: the next admission sheds and leaves state unchanged.
	if l.tryAcquire() {
		t.Fatal("acquire at the limit was admitted; want shed")
	}
	if l.inflight != 3 {
		t.Fatalf("a shed changed inflight to %d, want 3", l.inflight)
	}
	// A release frees exactly one slot; a healthy gradient keeps the limit growing,
	// so admission opens again.
	l.release(1)
	if l.inflight != 2 {
		t.Fatalf("after release inflight=%d, want 2", l.inflight)
	}
	if !l.tryAcquire() {
		t.Fatal("acquire after a release was shed; want admitted")
	}
}

// TestLimiterGrowsUnderHealthyGradient: a gradient at or above the shed threshold
// is headroom, so each completion grows the limit additively up to the ceiling.
func TestLimiterGrowsUnderHealthyGradient(t *testing.T) {
	l := newLimiter()
	start := l.limit
	if start <= 0 || start >= limiterCeiling {
		t.Fatalf("start %d should be modest: in (0, %d)", start, limiterCeiling)
	}
	// One healthy completion grows by exactly the additive step.
	l.inflight = 1
	l.release(1)
	if l.limit != start+limiterIncrease {
		t.Fatalf("after one healthy release limit=%d, want %d", l.limit, start+limiterIncrease)
	}
	// Many healthy completions grow it to the ceiling and clamp there.
	for i := 0; i < limiterCeiling*2; i++ {
		l.release(gradientShedThreshold) // exactly at the threshold counts as headroom
	}
	if l.limit != limiterCeiling {
		t.Fatalf("limit=%d after sustained headroom, want the ceiling %d", l.limit, limiterCeiling)
	}
}

// TestLimiterShrinksUnderDegradedGradient: a gradient below the threshold is
// queuing, so each completion shrinks the limit multiplicatively down to the
// floor — and never below it, so a node always makes forward progress.
func TestLimiterShrinksUnderDegradedGradient(t *testing.T) {
	l := newLimiter()
	l.limit = 100
	prev := l.limit
	l.release(0) // a fully degraded gradient
	if l.limit >= prev {
		t.Fatalf("degraded release did not shrink: %d -> %d", prev, l.limit)
	}
	if l.limit != int(float64(prev)*limiterDecrease) {
		t.Fatalf("shrink=%d, want %d (multiplicative)", l.limit, int(float64(prev)*limiterDecrease))
	}
	// Sustained degradation drives it to the floor and pins it there.
	for i := 0; i < 1000; i++ {
		l.release(0)
	}
	if l.limit != limiterFloor {
		t.Fatalf("limit=%d under sustained degradation, want the floor %d", l.limit, limiterFloor)
	}
	if limiterFloor <= 0 {
		t.Fatalf("floor must be > 0 so the limit can never reach zero; got %d", limiterFloor)
	}
	// At the floor the node still admits — forward progress is guaranteed.
	if !l.tryAcquire() {
		t.Fatal("a limiter at the floor refused all admission; the floor must guarantee progress")
	}
}

// TestLimiterClampBounds: just below the threshold shrinks, just at/above grows,
// and the limit is always clamped to [floor, ceiling] from either direction.
func TestLimiterClampBounds(t *testing.T) {
	// From the ceiling, a healthy completion cannot exceed it.
	l := newLimiter()
	l.limit = limiterCeiling
	l.release(1)
	if l.limit != limiterCeiling {
		t.Fatalf("grew past the ceiling: %d", l.limit)
	}
	// From the floor, a degraded completion cannot drop below it.
	l.limit = limiterFloor
	l.release(0)
	if l.limit != limiterFloor {
		t.Fatalf("shrank below the floor: %d", l.limit)
	}
	// Threshold boundary: exactly at the threshold grows; just below shrinks.
	l.limit = 50
	l.release(gradientShedThreshold)
	if l.limit <= 50 {
		t.Fatalf("at the threshold the limit did not grow: %d", l.limit)
	}
	l.limit = 50
	l.release(gradientShedThreshold - 0.0001)
	if l.limit >= 50 {
		t.Fatalf("just below the threshold the limit did not shrink: %d", l.limit)
	}
}

// TestLimiterReleaseDoesNotUnderflow: release on an idle limiter never drives
// inflight negative — a defensive guard against a double release.
func TestLimiterReleaseDoesNotUnderflow(t *testing.T) {
	l := newLimiter()
	l.release(1)
	if l.inflight != 0 {
		t.Fatalf("inflight=%d after releasing an idle limiter, want 0", l.inflight)
	}
}
