package sys

import (
	"sync"
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// Loop implements seam.Loop: one goroutine running posted functions
// serially, in FIFO order. It is the production half of the
// single-threaded-control-plane contract (docs/SIMULATION.md) — under
// simulation, the global event queue provides the same serialization.
//
// The queue is unbounded because Post must never block: a bounded queue
// could deadlock a loop function posting to its own loop, and backpressure
// is a decision for core logic (the write-ack floor), never the runtime.
type Loop struct {
	mu      sync.Mutex
	queue   []func()
	stopped bool
	wake    chan struct{}
	done    chan struct{}
}

// NewLoop starts a loop. The caller owns it and must Stop it.
func NewLoop() *Loop {
	l := &Loop{
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	go l.run()
	return l
}

// Post implements seam.Loop. Posts after Stop are discarded, matching the
// simulator: work posted to a dead process never runs.
func (l *Loop) Post(fn func()) {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return
	}
	l.queue = append(l.queue, fn)
	l.mu.Unlock()
	l.signal()
}

// Stop halts the loop after the currently running function, discards any
// queued work, and waits for the loop goroutine to exit. Stopping twice is
// fine. Stop must be called from outside the loop: a loop function calling
// Stop would deadlock waiting for its own return.
func (l *Loop) Stop() {
	l.mu.Lock()
	l.stopped = true
	l.mu.Unlock()
	l.signal()
	<-l.done
}

func (l *Loop) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

func (l *Loop) run() {
	defer close(l.done)
	for {
		l.mu.Lock()
		if l.stopped {
			l.mu.Unlock()
			return
		}
		if len(l.queue) == 0 {
			l.mu.Unlock()
			<-l.wake
			continue
		}
		fn := l.queue[0]
		l.queue[0] = nil // release the func for GC
		l.queue = l.queue[1:]
		if len(l.queue) == 0 {
			l.queue = nil // release the backing array too
		}
		l.mu.Unlock()
		fn()
	}
}

// LoopClock wraps a Clock so AfterFunc callbacks are posted to a node's
// loop instead of running on a runtime goroutine, restoring the seam.Clock
// delivery contract on the real runtime.
//
// Stop on a returned Timer reports false once the underlying timer has
// fired, even if the posted callback has not yet run — fired but not yet
// delivered is "committed to run" under the seam.Timer contract.
func LoopClock(c seam.Clock, l seam.Loop) seam.Clock {
	return loopClock{c: c, l: l}
}

type loopClock struct {
	c seam.Clock
	l seam.Loop
}

func (lc loopClock) Now() time.Time { return lc.c.Now() }

func (lc loopClock) AfterFunc(d time.Duration, fn func()) seam.Timer {
	return lc.c.AfterFunc(d, func() { lc.l.Post(fn) })
}
