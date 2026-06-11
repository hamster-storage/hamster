package sim

import "time"

type eventState uint8

const (
	pending eventState = iota
	fired
	stopped
)

// event is one scheduled occurrence in the global queue. Events are ordered
// by virtual time, with the scheduling sequence number as the deterministic
// tiebreak. An event doubles as the seam.Timer handle for AfterFunc.
type event struct {
	at    time.Time
	seq   uint64
	fn    func()
	state eventState
}

// Stop implements seam.Timer. The event stays in the heap and is skipped at
// dispatch.
func (e *event) Stop() bool {
	if e.state != pending {
		return false
	}
	e.state = stopped
	return true
}

// eventQueue is a min-heap over (at, seq).
type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if !q[i].at.Equal(q[j].at) {
		return q[i].at.Before(q[j].at)
	}
	return q[i].seq < q[j].seq
}

func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventQueue) Push(x any) { *q = append(*q, x.(*event)) }

func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	ev := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return ev
}
