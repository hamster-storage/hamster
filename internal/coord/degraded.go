package coord

// Node-degradation detection (ADR-0039 part 5). This is the load shedder's
// counterpart, watching the *opposite* signal — and it is DETECTION ONLY: it
// flags a node whose own service floor has degraded and exposes that as a metric
// and a candidate `degraded` state in `cluster status`. It takes no automatic
// action. Nothing here evicts, drains, or changes shedding; auto-eviction on
// latency could cascade a healthy cluster into collapse (ADR-0039 says so
// explicitly), so the operator decides.
//
// The key distinction it draws — the whole reason it is a separate detector from
// the Phase-4 limiter:
//
//   - Load saturation raises latency *together with* arrival rate: curRTT climbs
//     while minRTT — the best-case floor, the min over a window — stays flat,
//     because under load there are still un-queued samples reaching the old
//     floor. That is NOT degradation; the adaptive limiter already handles it by
//     shedding excess concurrency.
//   - Degradation raises the *floor itself*: a failing drive, a throttled volume,
//     a dying NIC makes even the best-case service time climb at steady load.
//     minRTT rises. THIS is what to detect.
//
// So the detector watches minRTT (the gradient tracker's re-probing long-window
// minimum, which is built to be able to rise — gradient.go) and NEVER curRTT.
// Pure load moves only curRTT, which this detector never reads, so by
// construction load cannot trip it. It maintains a much longer-lived baseline of
// that floor — the *established* best case — and flags `degraded` when the
// current floor stays a configurable factor above the baseline for a sustained
// run of samples (sustained, not a single spike: one slow seek or a GC pause
// must not flag).
//
// Loop-owned and deterministic, like liveness and the gradient/limiter trackers:
// fed from the same per-operation completion path observeLatency drives (no
// second timing source — it consumes the minRTT the gradient tracker already
// computed from the latency sample), so it has no clock or randomness of its own
// and behaves identically under the simulation harness.

// The detection sensitivity is configurable (ADR-0039 consequences: "the
// degradation sensitivity [is] configurable, defaulting sensibly"). These are
// vars, not consts, so a test can tighten them — like liveness's downCooldown —
// and so a future operator knob can set them; none has any format effect.
var (
	// degradedBaselineWindow is the long window over which the detector
	// establishes the floor it judges against — a two-window rolling minimum of
	// minRTT, so the baseline remembers the best floor for up to twice this many
	// samples. Deliberately an order of magnitude longer than the gradient's own
	// re-probe window (minWindowSamples), so the baseline is the *established*
	// best case, not a recent one: when the floor genuinely rises, minRTT climbs
	// within a couple of its short windows while this baseline still holds the old
	// good floor, giving a long window in which current/baseline reads elevated.
	// (A floor that stays degraded longer than the baseline's memory eventually
	// becomes the new baseline and the flag clears — this is a transition
	// detector surfaced for an operator, not a permanent latch, and nothing
	// automatic depends on it.)
	degradedBaselineWindow = 2000

	// degradedFactor is how far above the established baseline the current floor
	// must climb to count as degradation: at 2.0 the best-case service time has
	// doubled with no help from queuing (load inflates curRTT, never minRTT, so it
	// cannot produce this). Well above normal floor jitter, so only a genuine
	// floor shift trips it.
	degradedFactor = 2.0

	// degradedSustain is how many consecutive elevated samples are required before
	// the detector flags degraded: a sustained rise of the floor, not a single
	// spike (one slow drive seek, a GC pause, a one-off retransmit). The counter
	// resets to zero the instant the floor returns under the factor, so the flag
	// clears as soon as the floor recovers.
	degradedSustain = 50
)

// degradedDetector is one operation type's degradation state (ADR-0039 part 5).
// Loop-owned, so no lock; deterministic given the order and values of the minRTT
// floor samples fed in. It holds a two-window rolling minimum of minRTT (the
// baseline — the established floor), an elevated-sample run length, and the
// latched flag. It judges only once a full baseline window has completed
// (havePrev), so a node never reads degraded before it has a trustworthy floor.
type degradedDetector struct {
	// baseline: a two-window rolling minimum of minRTT, mirroring the gradient
	// tracker's minRTT construction but over a much longer window. winMin is the
	// minimum of the current (accumulating) window; prevMin is the minimum of the
	// last completed window; the baseline is the smaller of the two, so an old
	// good floor ages out only after at most two long windows.
	winN     int     // samples in the current baseline window
	winMin   float64 // minimum minRTT in the current window
	prevMin  float64 // minimum minRTT in the last completed window
	havePrev bool    // a full baseline window has completed (the warmup gate)

	elevated int  // consecutive samples with minRTT >= factor*baseline
	degraded bool // the sustained-elevation flag
}

func newDegradedDetector() *degradedDetector { return &degradedDetector{} }

// update folds one operation's current minRTT floor (in seconds, the value the
// gradient tracker exposes after this same sample) into the detector. Call on
// the loop, right after the gradient tracker updates, so the two share one
// timing source. It rolls the baseline window, then judges the current floor
// against the established baseline.
func (d *degradedDetector) update(minRTT float64) {
	// Extend the current baseline window's minimum, rolling every
	// degradedBaselineWindow samples so the established floor decays slowly and
	// can itself rise once a degraded floor persists past the window.
	if d.winN == 0 || minRTT < d.winMin {
		d.winMin = minRTT
	}
	d.winN++
	if d.winN >= degradedBaselineWindow {
		d.prevMin = d.winMin
		d.havePrev = true
		d.winN = 0 // the next sample seeds a fresh window
	}

	base := d.baseline()
	// Before a full baseline window has completed, or with no usable floor yet,
	// read healthy: there is no trustworthy established best case to judge against.
	if !d.havePrev || base <= 0 {
		d.elevated = 0
		d.degraded = false
		return
	}
	if minRTT >= degradedFactor*base {
		d.elevated++
	} else {
		d.elevated = 0
	}
	d.degraded = d.elevated >= degradedSustain
}

// baseline returns the established best-case floor: the smaller of the current
// and previous baseline-window minima, or 0 before any sample.
func (d *degradedDetector) baseline() float64 {
	switch {
	case d.winN > 0 && d.havePrev:
		return min(d.winMin, d.prevMin)
	case d.winN > 0:
		return d.winMin
	case d.havePrev:
		return d.prevMin
	default:
		return 0
	}
}

// isDegraded reports the latched degradation flag for this op.
func (d *degradedDetector) isDegraded() bool { return d.degraded }

// DegradedOp reports whether op's (PUT/GET) service floor is currently judged
// degraded (ADR-0039 part 5): minRTT sustained a factor above its established
// baseline. False for an unknown op or before a trustworthy baseline. Loop-owned:
// call on the node's loop. Detection only — it triggers no action.
func (c *Coordinator) DegradedOp(op string) bool {
	if d := c.detectors[op]; d != nil {
		return d.isDegraded()
	}
	return false
}

// Degraded reports whether this node's data-plane service floor is degraded for
// ANY tracked operation (ADR-0039 part 5): a node-level health signal for the
// metric and `cluster status`. It is a self-assessment of this node's own floor
// (a failing local drive, a throttled volume), distinct from the liveness view
// of *other* nodes being down. Loop-owned. Detection only — a degraded node is
// still up and serving; nothing here evicts, drains, or changes shedding.
func (c *Coordinator) Degraded() bool {
	for _, op := range GradientOps() {
		if c.DegradedOp(op) {
			return true
		}
	}
	return false
}
