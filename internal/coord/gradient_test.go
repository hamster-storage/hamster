package coord

import (
	"math"
	"testing"
)

// The RTT gradient tracker (ADR-0039 part 2) is pure and deterministic: given a
// sequence of service-time samples it computes minRTT (a re-probing long-window
// minimum that can rise), curRTT (a short-window EWMA), and the clamped gradient.
// These tests feed known sequences and assert each value, with no simulator —
// the tracker has no clock or randomness of its own.

const eps = 1e-12

func approx(a, b float64) bool { return math.Abs(a-b) <= eps }

// expectedEWMA mirrors update's curRTT: seed with the first sample, then fold in
// the rest at curAlpha. Used to assert curRTT independently of the tracker.
func expectedEWMA(samples []float64) float64 {
	cur := samples[0]
	for _, s := range samples[1:] {
		cur = curAlpha*s + (1-curAlpha)*cur
	}
	return cur
}

// TestGradientWarmup: before gradientWarmup samples the gradient reads 1
// (healthy), so a node never falsely sheds at startup. curRTT and minRTT still
// track from the first sample.
func TestGradientWarmup(t *testing.T) {
	tr := newTracker()
	// Feed rising samples so curRTT climbs well above minRTT — yet while under
	// warmup the gradient must stay pinned at 1.
	got := []float64{0.010, 0.020, 0.040, 0.080}
	for i, s := range got {
		tr.update(s)
		if tr.n >= gradientWarmup {
			t.Fatalf("test fed %d samples, expected fewer than the warmup %d", tr.n, gradientWarmup)
		}
		if g := tr.gradient(); g != 1 {
			t.Errorf("after %d samples gradient = %v, want 1 (warmup)", i+1, g)
		}
	}
	// minRTT is the floor of what it has seen; curRTT is the EWMA.
	if !approx(tr.minRTT(), 0.010) {
		t.Errorf("minRTT = %v, want 0.010", tr.minRTT())
	}
	if want := expectedEWMA(got); !approx(tr.curRTT(), want) {
		t.Errorf("curRTT = %v, want %v", tr.curRTT(), want)
	}
}

// TestGradientFallsAsLatencyRises: once warmed up, a curRTT that climbs above the
// floor drives the gradient down toward 0. The floor (minRTT) is held by the
// initial fast samples; only curRTT moves.
func TestGradientFallsAsLatencyRises(t *testing.T) {
	tr := newTracker()
	// Establish a 10ms floor across enough samples to clear warmup.
	for range gradientWarmup {
		tr.update(0.010)
	}
	if !approx(tr.minRTT(), 0.010) {
		t.Fatalf("minRTT = %v, want 0.010 after warmup", tr.minRTT())
	}
	if g := tr.gradient(); !approx(g, 1) {
		t.Fatalf("gradient = %v, want 1 while curRTT == minRTT", g)
	}

	// Now latency climbs and stays high: feed a long run of slow samples so the
	// EWMA converges near the new level. The gradient must fall well below 1.
	for range 50 {
		tr.update(0.100)
	}
	if !approx(tr.minRTT(), 0.010) {
		t.Errorf("minRTT = %v, want the floor still 0.010 (curRTT rise must not move it)", tr.minRTT())
	}
	g := tr.gradient()
	wantG := min(max(tr.minRTT()/tr.curRTT(), 0), 1)
	if !approx(g, wantG) {
		t.Errorf("gradient = %v, want clamp(minRTT/curRTT) = %v", g, wantG)
	}
	if g >= 0.5 {
		t.Errorf("gradient = %v, want it well below 1 with curRTT ~10x minRTT", g)
	}
}

// TestMinRTTRisesAfterReprobe: minRTT is a re-probing long-window minimum, not a
// permanent all-time minimum. When the genuine floor rises, the old fast samples
// age out (held by at most the current plus previous window) and minRTT climbs
// to the new floor — the signal part 5's degradation detection watches.
func TestMinRTTRisesAfterReprobe(t *testing.T) {
	tr := newTracker()

	// Window 1: a fast floor of 10ms, completing the window (it becomes prevMin).
	for range minWindowSamples {
		tr.update(0.010)
	}
	if !approx(tr.minRTT(), 0.010) {
		t.Fatalf("after window 1 minRTT = %v, want 0.010", tr.minRTT())
	}

	// Partway through window 2 the floor has genuinely risen to 50ms. The
	// previous (window-1) minimum still pins minRTT at 10ms — it has not yet risen.
	for range minWindowSamples / 2 {
		tr.update(0.050)
	}
	if !approx(tr.minRTT(), 0.010) {
		t.Errorf("mid window 2 minRTT = %v, want still 0.010 (window 1 not yet aged out)", tr.minRTT())
	}

	// Complete window 2 at the new floor: the 10ms window rolls out of prevMin,
	// so minRTT rises to the new 50ms floor.
	for range minWindowSamples / 2 {
		tr.update(0.050)
	}
	if !approx(tr.minRTT(), 0.050) {
		t.Errorf("after window 2 minRTT = %v, want the risen floor 0.050", tr.minRTT())
	}
}

// TestEmptyTracker: before any sample, minRTT/curRTT are 0 and the gradient is 1
// (healthy), so the accessors are safe to read at startup.
func TestEmptyTracker(t *testing.T) {
	tr := newTracker()
	if tr.minRTT() != 0 || tr.curRTT() != 0 {
		t.Errorf("empty tracker minRTT=%v curRTT=%v, want 0/0", tr.minRTT(), tr.curRTT())
	}
	if g := tr.gradient(); g != 1 {
		t.Errorf("empty tracker gradient = %v, want 1", g)
	}
}

// TestCoordinatorAccessors: the per-op accessors route to the right tracker and
// an unknown op reads as healthy (gradient 1, RTTs 0).
func TestCoordinatorAccessors(t *testing.T) {
	c := New(Config{})
	for range gradientWarmup {
		c.gradients[opGet].update(0.020)
	}
	if !approx(c.MinRTT(opGet), 0.020) {
		t.Errorf("MinRTT(GET) = %v, want 0.020", c.MinRTT(opGet))
	}
	if c.MinRTT(opPut) != 0 {
		t.Errorf("MinRTT(PUT) = %v, want 0 (no samples)", c.MinRTT(opPut))
	}
	if g := c.Gradient("BOGUS"); g != 1 {
		t.Errorf("Gradient(unknown) = %v, want 1", g)
	}
	if c.MinRTT("BOGUS") != 0 || c.CurRTT("BOGUS") != 0 {
		t.Errorf("unknown-op RTTs = %v/%v, want 0/0", c.MinRTT("BOGUS"), c.CurRTT("BOGUS"))
	}
}
