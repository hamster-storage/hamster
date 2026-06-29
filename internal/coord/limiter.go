package coord

import "errors"

// Adaptive load shedding (ADR-0039 parts 3 + 4). Each data-plane operation type
// (PUT, GET) carries a dynamic concurrency limit and a live in-flight count. A
// new request is admitted only while in-flight is below the current limit;
// otherwise it is shed at admission — before any coordinator work — with
// ErrShed, which the gateway maps to HTTP 429. The limit is not a fixed number:
// it grows while the latency gradient (ADR-0039 part 2) reports headroom and
// shrinks as queuing inflates latency, so the node finds its own moving ceiling
// (a cgroup, the drive, the NIC, a noisy neighbour) without any tuning.
//
// The whole thing is loop-owned, like liveness and the gradient trackers: every
// acquire, release, and limit update runs on the node's event loop, so the
// state is deterministic under the simulation harness — no wall clock, no goroutine
// races. Shedding only ever refuses a *new* request; an already-admitted
// operation always runs to completion, and a 429 is fully retryable and never
// touches durability or a committed object (critical invariant 1).

const (
	// limiterFloor is the hard lower bound on the limit. It is strictly > 0, so a
	// single slow operation can never drive the limit to zero and lock the node
	// out: forward progress is always guaranteed (ADR-0039 part 3, "bounded below
	// by a small floor"). No format effect.
	limiterFloor = 4

	// limiterCeiling caps the limit so a healthy run does not let it grow without
	// bound (each in-flight op holds memory and open shard streams). High enough
	// to be out of the way on real hardware; the latency gradient, not this cap,
	// is what normally holds the limit. No format effect.
	limiterCeiling = 256

	// limiterStart is the initial limit: modest, not the ceiling, so a node ramps
	// up from a safe point and discovers headroom rather than starting saturated
	// and shedding its way down. No format effect.
	limiterStart = 16

	// limiterIncrease is the additive step the limit grows by on each completion
	// while the gradient shows headroom (AIMD's additive increase). Slow growth
	// keeps the limit from overshooting the real ceiling. No format effect.
	limiterIncrease = 1

	// limiterDecrease is the multiplicative factor the limit shrinks by on each
	// completion once the gradient falls below the threshold (AIMD's
	// multiplicative decrease): a fast back-off so the node sheds quickly when
	// latency climbs, exactly the metastable-collapse avoidance the ADR wants. No
	// format effect.
	limiterDecrease = 0.9

	// gradientShedThreshold is the gradient below which the node is judged to be
	// queuing rather than serving with headroom. The gradient is
	// clamp(minRTT/curRTT, 0..1): at 0.5, recent latency has doubled the no-load
	// floor — the signal that added load is inflating latency, not throughput. At
	// or above it the limit grows; below it the limit shrinks. No format effect.
	gradientShedThreshold = 0.5
)

// ErrShed is the admission-control refusal (ADR-0039 part 4): the node is at its
// current concurrency limit, so a *new* request is refused immediately, before
// any coordinator work. It is distinct from ErrRefused (the durability-floor
// SlowDown/503): a shed is always safe and fully retryable — it never deletes,
// shortens, or blocks a committed object and never touches durability. The
// gateway maps it to HTTP 429 Too Many Requests with Retry-After.
var ErrShed = errors.New("coord: node at capacity, request shed at admission (TooManyRequests)")

// limiter is one operation type's adaptive concurrency limit and live in-flight
// count. Loop-owned, so no lock; deterministic given the order of acquires,
// releases, and the gradients fed to release.
type limiter struct {
	limit    int
	inflight int
}

func newLimiter() *limiter { return &limiter{limit: limiterStart} }

// tryAcquire admits one operation if in-flight is below the current limit,
// incrementing in-flight and returning true; otherwise it sheds, returning false
// without changing state. Call on the loop, as the first thing an operation does.
func (l *limiter) tryAcquire() bool {
	if l.inflight >= l.limit {
		return false
	}
	l.inflight++
	return true
}

// release marks one admitted operation complete: it decrements in-flight and
// updates the limit from gradient (ADR-0039 part 3). At or above the threshold
// the node has headroom, so the limit grows additively toward the ceiling; below
// it the node is queuing, so the limit shrinks multiplicatively toward the floor.
// The limit is always clamped to [limiterFloor, limiterCeiling], so it can never
// reach zero. Call on the loop, on every terminal of an admitted operation
// (success, error, abort) — exactly once — so in-flight never leaks.
func (l *limiter) release(gradient float64) {
	if l.inflight > 0 {
		l.inflight--
	}
	if gradient >= gradientShedThreshold {
		if l.limit < limiterCeiling {
			l.limit += limiterIncrease
		}
		return
	}
	l.limit = max(limiterFloor, int(float64(l.limit)*limiterDecrease))
}

// tryAcquire admits an operation of type op against its limiter, or sheds it.
// Loop-owned. An unknown op (no limiter) is admitted — the limiter never invents
// a refusal for an operation it does not model.
func (c *Coordinator) tryAcquire(op string) bool {
	if l := c.limiters[op]; l != nil {
		return l.tryAcquire()
	}
	return true
}

// release completes one admitted operation of type op, feeding the limiter the
// current gradient so it adapts. Loop-owned; call once per admitted operation, on
// every terminal.
func (c *Coordinator) release(op string) {
	if l := c.limiters[op]; l != nil {
		l.release(c.Gradient(op))
	}
}

// Limit returns op's current adaptive concurrency limit (ADR-0039), or 0 for an
// unknown op. Loop-owned: call on the node's loop. For the metrics layer.
func (c *Coordinator) Limit(op string) int {
	if l := c.limiters[op]; l != nil {
		return l.limit
	}
	return 0
}

// Inflight returns op's current admitted-but-not-yet-completed count (ADR-0039),
// or 0 for an unknown op. Loop-owned: call on the node's loop. For the metrics
// layer.
func (c *Coordinator) Inflight(op string) int {
	if l := c.limiters[op]; l != nil {
		return l.inflight
	}
	return 0
}
