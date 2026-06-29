package coord

import "testing"

// Node-degradation detection (ADR-0039 part 5) is pure and deterministic: it
// watches minRTT — the best-case service FLOOR — climb above an established
// baseline and stay there. These tests feed known floor sequences and assert the
// flag, with no simulator (the detector has no clock or randomness of its own).
//
// The decisive distinction the detector must draw — and the reason it is a
// separate detector from the Phase-4 limiter — is load vs degradation:
//   - load saturation moves only curRTT (the floor stays flat); it must NOT flag.
//   - degradation moves the floor (minRTT itself rises); it must flag.

// warmDetector fills a full baseline window at floor so the detector has a
// trustworthy established best case (havePrev) before the test proper begins.
func warmDetector(d *degradedDetector, floor float64) {
	for range degradedBaselineWindow {
		d.update(floor)
	}
}

// TestDegradedWarmup: before a full baseline window completes the detector reads
// healthy no matter what the floor does, so a node never falsely flags at
// startup with too little history to judge.
func TestDegradedWarmup(t *testing.T) {
	d := newDegradedDetector()
	// Feed a wildly rising floor, but stay under one baseline window.
	for i := range degradedBaselineWindow - 1 {
		d.update(0.001 * float64(i+1))
		if d.isDegraded() {
			t.Fatalf("flagged degraded after %d samples, before a full baseline window (%d)", i+1, degradedBaselineWindow)
		}
	}
	if d.havePrev {
		t.Fatalf("havePrev set before a full baseline window completed")
	}
}

// TestDegradationFlagsOnRisingFloor: at steady load a sustained rise of the floor
// (minRTT) above the baseline factor flags degraded after the sustain window —
// the failing-drive / throttled-volume signature.
func TestDegradationFlagsOnRisingFloor(t *testing.T) {
	d := newDegradedDetector()
	const floor = 0.001 // 1ms established best case
	warmDetector(d, floor)
	if d.isDegraded() {
		t.Fatalf("degraded right after warmup at a flat floor")
	}
	if got := d.baseline(); !approx(got, floor) {
		t.Fatalf("baseline = %v, want the established floor %v", got, floor)
	}

	// The floor genuinely rises to 5x — well past the 2x factor — and stays there.
	const bad = 0.005
	// Just under the sustain window of elevated samples: not yet flagged.
	for range degradedSustain - 1 {
		d.update(bad)
		if d.isDegraded() {
			t.Fatalf("flagged before the sustain window elapsed (a single spike must not flag)")
		}
	}
	// One more elevated sample reaches the sustain window: now flagged.
	d.update(bad)
	if !d.isDegraded() {
		t.Fatalf("not flagged after %d sustained elevated samples at %vx the floor", degradedSustain, bad/floor)
	}
	// The baseline still holds the old good floor (it has not aged out yet), so
	// the detector is measuring the rise, not re-anchoring on it.
	if got := d.baseline(); !approx(got, floor) {
		t.Errorf("baseline drifted to %v during degradation, want it still %v", got, floor)
	}
}

// TestDegradationClearsWhenFloorRecovers: a single spike below the sustain window
// never flags, and once the floor returns under the factor the flag clears at
// once — the elevated run resets the instant a healthy sample lands.
func TestDegradationClearsWhenFloorRecovers(t *testing.T) {
	d := newDegradedDetector()
	const floor = 0.002
	warmDetector(d, floor)

	// A short spike of elevated samples, shorter than the sustain window: a slow
	// seek or a GC pause. Must never flag.
	for range degradedSustain - 1 {
		d.update(0.010)
	}
	if d.isDegraded() {
		t.Fatalf("a spike shorter than the sustain window flagged degraded")
	}

	// Drive a real, sustained degradation so the flag latches.
	for range degradedSustain {
		d.update(0.010)
	}
	if !d.isDegraded() {
		t.Fatalf("sustained elevation did not flag")
	}

	// The floor recovers: the very next healthy sample resets the elevated run, so
	// the flag clears immediately.
	d.update(floor)
	if d.isDegraded() {
		t.Fatalf("flag did not clear the instant the floor recovered")
	}
}

// TestLoadDoesNotFlagDegraded is the discriminator (ADR-0039 part 5, the crux):
// a pure-LOAD pattern — frequent slow samples but a floor that never moves —
// drives curRTT well above minRTT yet must NOT flag degraded, because the
// detector reads only the floor (minRTT), which load leaves flat. It is fed
// through the REAL gradient tracker exactly as the coordinator wires it
// (observeLatency: t.update(seconds) then d.update(t.minRTT())), so the test
// proves the wiring, not just the detector in isolation.
func TestLoadDoesNotFlagDegraded(t *testing.T) {
	tr := newTracker()
	d := newDegradedDetector()

	feed := func(seconds float64) {
		tr.update(seconds)
		d.update(tr.minRTT()) // exactly the coordinator's observeLatency wiring
	}

	const floor = 0.001 // the true service-time floor, which load never moves
	// A load pattern: most samples queue and run slow (10x the floor), but —
	// crucially — some still reach the un-queued floor, the signature of load
	// (the limiter sheds excess, so there is always headroom for a fast sample).
	// Run it long enough to clear the baseline warmup and then some.
	totalLoad := degradedBaselineWindow + 4*degradedSustain
	for i := range totalLoad {
		if i%5 == 0 {
			feed(floor) // an un-queued sample reaches the floor
		} else {
			feed(0.010) // a queued sample: high latency, but it does not move the floor
		}
	}

	// Load is genuinely present: curRTT sits well above the floor (the gradient
	// would be shedding). Yet the floor — and so the degradation detector — is
	// unmoved.
	if tr.curRTT() <= tr.minRTT() {
		t.Fatalf("test bug: curRTT %v not above minRTT %v — no load present to discriminate against", tr.curRTT(), tr.minRTT())
	}
	if !approx(tr.minRTT(), floor) {
		t.Fatalf("minRTT = %v, want the floor still %v under pure load (load must not move the floor)", tr.minRTT(), floor)
	}
	if d.isDegraded() {
		t.Fatalf("pure load flagged degraded — the detector must watch minRTT (the floor), NOT curRTT")
	}

	// Now the floor itself rises at the SAME steady load (a drive degrades). minRTT
	// climbs, and the detector flags — the other side of the discriminator.
	const bad = 0.005
	for range 4 * minWindowSamples { // enough for the tracker's minRTT to rise to the new floor
		feed(bad)
	}
	if !approx(tr.minRTT(), bad) {
		t.Fatalf("minRTT = %v, want it risen to the new floor %v", tr.minRTT(), bad)
	}
	if !d.isDegraded() {
		t.Fatalf("a sustained rise of the floor at steady load did not flag degraded")
	}
}

// TestDegradedCoordinatorAccessors: the per-op and aggregate accessors route to
// the right detector, and an unknown op / a fresh coordinator reads not-degraded.
func TestDegradedCoordinatorAccessors(t *testing.T) {
	c := New(Config{})
	if c.Degraded() || c.DegradedOp(opPut) || c.DegradedOp(opGet) {
		t.Fatalf("a fresh coordinator reads degraded")
	}
	if c.DegradedOp("BOGUS") {
		t.Fatalf("unknown op reads degraded")
	}
	// Drive the GET detector into degradation directly and confirm the aggregate
	// Degraded() reflects it (any op degraded ⇒ node degraded).
	d := c.detectors[opGet]
	warmDetector(d, 0.001)
	for range degradedSustain {
		d.update(0.005)
	}
	if !c.DegradedOp(opGet) {
		t.Fatalf("DegradedOp(GET) false after a sustained floor rise")
	}
	if c.DegradedOp(opPut) {
		t.Fatalf("DegradedOp(PUT) true though only GET degraded")
	}
	if !c.Degraded() {
		t.Fatalf("aggregate Degraded() false though GET is degraded")
	}
}
