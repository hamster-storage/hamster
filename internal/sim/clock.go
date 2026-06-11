package sim

import (
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// nodeClock is a node's view of virtual time. Per-node skew and drift (the
// fault model's Clock entry) will layer in here; for now all nodes read the
// same virtual clock.
type nodeClock struct {
	s    *Sim
	slot *slot
}

func (c *nodeClock) Now() time.Time { return c.s.now }

// AfterFunc schedules fn as an event in the global queue. The callback
// captures the node's epoch at scheduling time: if the node crashes before
// the timer fires, the epoch moves on and the callback is dropped, because a
// process's timers die with the process.
func (c *nodeClock) AfterFunc(d time.Duration, fn func()) seam.Timer {
	epoch := c.slot.epoch
	return c.s.schedule(d, func() {
		if c.slot.epoch != epoch || c.slot.node == nil {
			return
		}
		fn()
	})
}
