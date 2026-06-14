package coord

import (
	"slices"
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// downCooldown is how long a node stays considered down after a failed
// data-plane operation before one operation is allowed to re-probe it. A
// variable, not a constant, so tests can tighten it; it has no format effect.
// Sized comfortably above one PUT's retransmit budget (maxAttempts * rto), so
// detecting a still-down node and re-arming costs at most one timeout per
// window rather than one per PUT.
var downCooldown = 30 * time.Second

// liveness is a passive, loop-owned failure detector: a node that fails a
// data-plane operation is considered down for downCooldown, so a PUT skips it
// instead of opening a stream that would only time out — until the window
// lapses and one operation re-probes it. A success clears it immediately.
//
// It is deliberately passive: the only inputs are observed operation outcomes
// and the loop clock, so it is deterministic under the simulation harness and
// needs no heartbeat protocol. The cost is that the first operation to reach a
// freshly-down node still pays its timeout — that attempt is how the detector
// learns. GET already abandons stragglers, so reads pay nothing regardless.
type liveness struct {
	downUntil map[seam.NodeID]time.Time
}

func newLiveness() *liveness {
	return &liveness{downUntil: make(map[seam.NodeID]time.Time)}
}

// record folds one operation outcome in: a success clears any down mark, a
// failure (re)arms the cooldown from now.
func (l *liveness) record(id seam.NodeID, ok bool, now time.Time) {
	if ok {
		delete(l.downUntil, id)
		return
	}
	l.downUntil[id] = now.Add(downCooldown)
}

// isDown reports whether id is within its cooldown window at now. A lapsed
// window is cleared so the next operation re-probes the node — one attempt
// that either clears the mark (recovered) or re-arms it (still down).
func (l *liveness) isDown(id seam.NodeID, now time.Time) bool {
	u, ok := l.downUntil[id]
	if !ok {
		return false
	}
	if now.Before(u) {
		return true
	}
	delete(l.downUntil, id)
	return false
}

// down returns the node IDs currently considered down, in ID order — the
// local node's runtime view, for status reporting. Lapsed windows are pruned.
func (l *liveness) down(now time.Time) []seam.NodeID {
	var out []seam.NodeID
	for id := range l.downUntil {
		if l.isDown(id, now) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}
