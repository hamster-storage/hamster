// Package sys provides the production implementations of the seam
// interfaces (internal/seam): thin, boring adapters over the operating
// system. The design rule from docs/SIMULATION.md is "no logic in the
// adapters" — nothing in this package makes a decision, so everything that
// does is exercised by the simulator.
package sys

import (
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// Clock implements seam.Clock on the real time package.
//
// time.AfterFunc runs callbacks on a runtime goroutine, not on a node event
// loop; wrap a Clock with LoopClock to restore the seam.Clock delivery
// contract.
type Clock struct{}

// Now implements seam.Clock.
func (Clock) Now() time.Time { return time.Now() }

// AfterFunc implements seam.Clock.
func (Clock) AfterFunc(d time.Duration, fn func()) seam.Timer {
	return timer{time.AfterFunc(d, fn)}
}

type timer struct{ t *time.Timer }

func (t timer) Stop() bool { return t.t.Stop() }
