package coord

import (
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// The continuous background scrubber. RepairSweep and Optimize are one-shot,
// operator- or transition-driven; production durability also needs a scrub that
// runs on its own, forever, finding bitrot and rebuilding lost shards before a
// read ever trips over them (ERASURE-CODING.md "one system"). This is that — and
// it is loop-owned and seam-clock-paced, so it runs identically under the
// simulator (ADR-0009), the only way a durability mechanism earns trust here.
//
// Shape, stated honestly (the v0.4 first pass): the scrubber walks the keyspace
// one object per Pace, healing exactly what a RepairSweep would for that object,
// and idles PassInterval between full passes. Per-object pacing — not a single
// long held pass — is deliberate: it yields the single-flight guard between
// objects so an operator optimize or a transition migration slots in within one
// object's work, never waiting out a whole pass. It scrubs only as the leader
// (rebuilds commit through Raft) and only in steady state (a layout transition is
// driveTransitionClose's to drive; the scrubber stands aside while one is open).
// Byte-rate throttling, persisted progress across restarts, and prioritizing the
// least-recently-scrubbed object are later refinements; the snapshot-per-pass
// keyspace walk is the correctness floor.

// ScrubConfig tunes the continuous scrubber.
type ScrubConfig struct {
	// Pace is the delay between scrubbing consecutive objects — the throttle that
	// keeps the background scrub off the data path's and the loop's back.
	Pace time.Duration
	// PassInterval is the idle delay after a full pass (or when there is nothing
	// to scrub) before the next pass begins.
	PassInterval time.Duration
}

type scrubber struct {
	c       *Coordinator
	cfg     ScrubConfig
	pending []sweepItem // the current pass's remaining work, snapshot at pass start
	timer   seam.Timer
	stopped bool

	// Cumulative counts since start (loop-owned, read via ScrubStats).
	scrubbed int // objects examined
	healed   int // objects that needed a rebuild, migration, or re-encode
	passes   int // full passes completed
}

// ScrubStats reports the background scrubber's cumulative progress: objects
// examined, objects that needed healing, and full passes completed. Zero if the
// scrubber was never started. Loop-owned: call it on the node's loop.
func (c *Coordinator) ScrubStats() (scrubbed, healed, passes int) {
	if c.scrub == nil {
		return 0, 0, 0
	}
	return c.scrub.scrubbed, c.scrub.healed, c.scrub.passes
}

// StartScrub begins the continuous background scrub. Call once after building the
// coordinator; a second call is a no-op. Loop-owned.
func (c *Coordinator) StartScrub(cfg ScrubConfig) {
	if c.scrub != nil {
		return
	}
	c.scrub = &scrubber{c: c, cfg: cfg}
	c.scrub.arm(cfg.Pace)
}

// StopScrub halts the background scrub. Loop-owned.
func (c *Coordinator) StopScrub() {
	if c.scrub == nil {
		return
	}
	c.scrub.stopped = true
	if c.scrub.timer != nil {
		c.scrub.timer.Stop()
	}
}

// arm schedules the next tick after d (loop-delivered by the seam clock).
func (s *scrubber) arm(d time.Duration) {
	if s.stopped {
		return
	}
	s.timer = s.c.cfg.Clock.AfterFunc(d, s.tick)
}

// ready reports whether the scrubber may run now: not while another sweep holds
// the guard, only as the leader, and only in steady state — a layout transition
// is driveTransitionClose's to drive (it migrates shards), and a forming cluster
// has no layout yet.
func (s *scrubber) ready() bool {
	if s.c.sweeping {
		return false
	}
	if _, leader := s.c.cfg.Raft.Leader(); !leader {
		return false
	}
	layout, ok := s.c.cfg.Layout()
	return ok && len(layout.Previous) == 0
}

// tick scrubs one object and reschedules. Not-ready ticks re-arm after a Pace and
// touch nothing, so a leadership change or an open transition simply pauses the
// scrub where it stood.
func (s *scrubber) tick() {
	if s.stopped {
		return
	}
	if !s.ready() {
		s.arm(s.cfg.Pace)
		return
	}
	if len(s.pending) == 0 {
		s.pending, _ = s.c.collectSweepWork()
		if len(s.pending) == 0 {
			s.arm(s.cfg.PassInterval) // nothing stored yet — idle, then look again
			return
		}
	}
	item := s.pending[0]
	s.pending = s.pending[1:]
	if !s.c.beginSweep() {
		s.pending = append([]sweepItem{item}, s.pending...) // lost the race; retry
		s.arm(s.cfg.Pace)
		return
	}
	s.scrubbed++
	op := &sweepOp{
		c:    s.c,
		work: []sweepItem{item},
		done: func(r RepairReport, _ error) {
			s.c.endSweep()
			if r.RebuiltShards > 0 || r.MigratedShards > 0 || r.ReEncoded > 0 {
				s.healed++
			}
			next := s.cfg.Pace
			if len(s.pending) == 0 {
				s.passes++
				next = s.cfg.PassInterval
			}
			s.arm(next)
		},
	}
	op.nextItem()
}
