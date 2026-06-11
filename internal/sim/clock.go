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

// AfterFunc schedules fn as an event in the global queue. The callback is
// epoch-fenced: if the node crashes before the timer fires, it is dropped,
// because a process's timers die with the process.
func (c *nodeClock) AfterFunc(d time.Duration, fn func()) seam.Timer {
	return c.s.schedule(d, c.slot.fence(fn))
}
