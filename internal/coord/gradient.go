package coord

// Latency-gradient tracking (ADR-0039 part 2). For each data-plane operation
// type the coordinator maintains a no-load baseline and a recent estimate of
// service time, and derives the gradient between them:
//
//   - minRTT: the best-case service time with no queuing — a long-window
//     minimum. It is NOT a permanent all-time minimum: it re-probes every long
//     window so it tracks the *current* floor and can RISE again when the
//     genuine floor rises (a degrading drive, a throttled volume). Part 5's
//     degradation detection watches minRTT climb, so it must be able to climb.
//   - curRTT: a short-window estimate of recent latency — an EWMA over the most
//     recent samples, deterministic given the sample order.
//   - gradient = clamp(minRTT/curRTT, 0..1): ≈1 when healthy (recent latency is
//     near the floor) and falls toward 0 as queuing inflates curRTT above the
//     floor.
//
// This phase only computes and exposes the signal; it sheds nothing yet (that
// is part 3/4). The tracker is loop-owned and fed from the same per-operation
// latency sample observeLatency reports (one timing source), so it is fully
// deterministic under the simulation harness — no clock or randomness of its
// own, just the seconds the loop hands it.

const (
	// minWindowSamples is the long window over which minRTT is re-probed: the
	// running minimum rolls every this-many samples, so a genuine rise in the
	// floor surfaces within two windows rather than being pinned forever by one
	// old fast sample. Long enough to almost certainly catch a no-queue sample,
	// short enough that real degradation shows up promptly. No format effect.
	minWindowSamples = 200

	// curAlpha is the EWMA weight on the newest sample for curRTT: a short
	// window where the last handful of samples dominate (~1/curAlpha samples'
	// worth of memory), so curRTT tracks recent latency closely. No format effect.
	curAlpha = 0.2

	// gradientWarmup is how many samples must accumulate before the gradient is
	// computed from data; before then it reads 1 (healthy), so a node never
	// falsely reads as overloaded at startup with too few samples to judge.
	gradientWarmup = 5
)

// tracker is the per-operation RTT gradient state (ADR-0039 part 2). Loop-owned,
// so no lock; deterministic given the order and values of the samples fed in.
type tracker struct {
	n int // total samples seen, for the warmup gate

	// minRTT: a rolling two-window minimum. winMin is the minimum of the
	// current (still accumulating) window; prevMin is the minimum of the last
	// completed window. The published minimum is the smaller of the two, so an
	// old fast sample ages out after at most two windows and minRTT can rise.
	winN     int     // samples in the current window
	winMin   float64 // minimum of the current window
	prevMin  float64 // minimum of the previous completed window
	havePrev bool    // a previous window exists

	// curRTT: an EWMA over recent samples.
	cur  float64
	curN int // samples folded into cur (0 = none yet)
}

func newTracker() *tracker { return &tracker{} }

// update folds one successful operation's service time (in seconds) into the
// tracker. Call on the loop, from the same point observeLatency reports the
// sample, so the histogram and the gradient see identical samples.
func (t *tracker) update(seconds float64) {
	t.n++

	// curRTT: EWMA. Seed with the first sample so curRTT is meaningful at once.
	if t.curN == 0 {
		t.cur = seconds
	} else {
		t.cur = curAlpha*seconds + (1-curAlpha)*t.cur
	}
	t.curN++

	// minRTT: extend the current window's minimum, then roll the window every
	// minWindowSamples so the floor is re-probed and can rise.
	if t.winN == 0 || seconds < t.winMin {
		t.winMin = seconds
	}
	t.winN++
	if t.winN >= minWindowSamples {
		t.prevMin = t.winMin
		t.havePrev = true
		t.winN = 0 // the next sample seeds a fresh window
	}
}

// minRTT returns the current best-case service time: the smaller of the current
// and previous window minima, or 0 before any sample.
func (t *tracker) minRTT() float64 {
	switch {
	case t.winN > 0 && t.havePrev:
		return min(t.winMin, t.prevMin)
	case t.winN > 0:
		return t.winMin
	case t.havePrev:
		return t.prevMin
	default:
		return 0
	}
}

// curRTT returns the short-window latency estimate, or 0 before any sample.
func (t *tracker) curRTT() float64 { return t.cur }

// gradient returns clamp(minRTT/curRTT, 0..1): 1 (healthy) before warmup or when
// curRTT is non-positive, else the clamped ratio.
func (t *tracker) gradient() float64 {
	if t.n < gradientWarmup || t.cur <= 0 {
		return 1
	}
	g := t.minRTT() / t.cur
	return min(max(g, 0), 1)
}

// GradientOps lists the operation labels the gradient tracker maintains, so the
// metrics layer can register one gauge series per operation. Matches the method
// labels on hamster_s3_requests_total and the latency histogram.
func GradientOps() []string { return []string{opPut, opGet} }

// Gradient returns the latency gradient for op (PUT/GET): ≈1 healthy, →0 as
// queuing grows; 1 for an unknown op or before warmup. Loop-owned: call on the
// node's loop. For the metrics layer (ADR-0039 part 2) and, later, the limiter.
func (c *Coordinator) Gradient(op string) float64 {
	if t := c.gradients[op]; t != nil {
		return t.gradient()
	}
	return 1
}

// MinRTT returns op's best-case service time in seconds (the long-window
// minimum), or 0 if unknown / no samples yet. Loop-owned.
func (c *Coordinator) MinRTT(op string) float64 {
	if t := c.gradients[op]; t != nil {
		return t.minRTT()
	}
	return 0
}

// CurRTT returns op's short-window latency estimate in seconds, or 0 if unknown
// / no samples yet. Loop-owned.
func (c *Coordinator) CurRTT(op string) float64 {
	if t := c.gradients[op]; t != nil {
		return t.curRTT()
	}
	return 0
}
