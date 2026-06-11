package sim

import (
	"fmt"
	"slices"
	"time"

	"github.com/hamster-storage/hamster/internal/seam"
)

// NetConfig is the network fault model for a run. The zero value is a
// perfect network: instant, ordered, lossless — the degenerate baseline.
// Real schedules set latency jitter so the PRNG explores message orderings,
// plus drops and duplicates. Partitions are dynamic (Sim.Partition), not
// configured here.
type NetConfig struct {
	// MinLatency and MaxLatency bound per-message delivery delay; the
	// actual delay is drawn uniformly from the range.
	MinLatency, MaxLatency time.Duration

	// DropProb is the probability a message copy is silently lost.
	DropProb float64

	// DuplicateProb is the probability a message is delivered twice.
	DuplicateProb float64
}

func (c NetConfig) validate() {
	switch {
	case c.MinLatency < 0 || c.MaxLatency < c.MinLatency:
		panic(fmt.Sprintf("sim: invalid latency range [%v, %v]", c.MinLatency, c.MaxLatency))
	case c.DropProb < 0 || c.DropProb > 1:
		panic(fmt.Sprintf("sim: DropProb %v outside [0, 1]", c.DropProb))
	case c.DuplicateProb < 0 || c.DuplicateProb > 1:
		panic(fmt.Sprintf("sim: DuplicateProb %v outside [0, 1]", c.DuplicateProb))
	}
}

// nodeTransport implements seam.Transport for one sending node. Sending
// enqueues a delivery event; the PRNG draws the latency and decides drops
// and duplicates at send time, while partitions are checked at delivery
// time (Sim.deliver) so they affect messages already in flight.
type nodeTransport struct {
	s    *Sim
	from seam.NodeID
}

func (t *nodeTransport) Send(to seam.NodeID, msg []byte) {
	s := t.s
	msg = slices.Clone(msg) // the sender may reuse its buffer
	copies := 1
	if s.rng.Float64() < s.net.DuplicateProb {
		copies = 2
	}
	for range copies {
		if s.rng.Float64() < s.net.DropProb {
			continue
		}
		delay := s.net.MinLatency
		if jitter := s.net.MaxLatency - s.net.MinLatency; jitter > 0 {
			delay += time.Duration(s.rng.Int64N(int64(jitter) + 1))
		}
		s.schedule(delay, func() {
			s.deliver(t.from, to, msg)
		})
	}
}
