package sim

// nodeLoop implements seam.Loop. Under simulation the global event queue is
// every node's event loop: Post schedules fn as an immediate event, so it
// runs at the current virtual time, after events already queued for that
// instant — dispatch stays deterministic. Like timers, posted work is
// epoch-fenced and dies if the node crashes before it runs.
type nodeLoop struct {
	s    *Sim
	slot *slot
}

func (l *nodeLoop) Post(fn func()) {
	l.s.schedule(0, l.slot.fence(fn))
}
