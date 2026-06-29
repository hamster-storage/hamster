package coord_test

import (
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/coord"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/metrics"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sim"
)

// Per-operation latency measurement (ADR-0039 part 1). The coordinator times
// each PUT and GET on the loop through the seam clock, from admission (beginPut /
// GetEntry) to completion (the success terminal), and reports the service time
// via the ObserveLatency hook — the same coord→cluster decoupling the streaming
// load gauges use. These tests prove the timing is exactly seam-clock-driven and
// therefore deterministic and simulator-controlled: the observed duration equals
// the simulated clock elapsed across the operation, to the nanosecond.

// putCapturingClock drives a PUT through the leader, capturing the two seam-clock
// instants the coordinator times against: start, read in the same posted closure
// immediately before co.Put (== beginPut's Clock.Now, no time advances between),
// and end, read inside the done callback (== the observeLatency Clock.Now of the
// same loop dispatch). end.Sub(start) is the exact duration the coordinator
// observes.
func (c *cluster) putCapturingClock(key string, body []byte) (start, end time.Time) {
	c.t.Helper()
	id := c.leader()
	clock := c.worlds[id].Clock
	done := false
	c.worlds[id].Loop.Post(func() {
		start = clock.Now()
		c.nodes[id].co.Put(bucket, key, body, coord.PutOptions{}, func(_ coord.PutResult, e error) {
			if e != nil {
				c.t.Errorf("put %q: %v", key, e)
			}
			end = clock.Now()
			done = true
		})
	})
	for range 5000 {
		c.s.Run(tick)
		if done {
			return start, end
		}
	}
	c.t.Fatal("put never finished")
	return
}

// getCapturingClock is putCapturingClock's read counterpart.
func (c *cluster) getCapturingClock(key string, off, length int64) (start, end time.Time) {
	c.t.Helper()
	id := c.leader()
	clock := c.worlds[id].Clock
	done := false
	c.worlds[id].Loop.Post(func() {
		start = clock.Now()
		c.nodes[id].co.Get(bucket, key, off, length, func(_ []byte, e error) {
			if e != nil {
				c.t.Errorf("get %q: %v", key, e)
			}
			end = clock.Now()
			done = true
		})
	})
	for range 5000 {
		c.s.Run(tick)
		if done {
			return start, end
		}
	}
	c.t.Fatal("get never finished")
	return
}

// TestLatencyObservedThroughSeamClock: a PUT and a GET each produce exactly one
// latency observation, on the leader, labeled by operation, and the observed
// duration equals the simulated seam-clock elapsed across the operation to the
// nanosecond — so advancing the simulated clock by a known amount produces that
// exact observed duration. The observation is also recorded into a real
// request-latency histogram (the wiring internal/cluster uses), whose snapshot
// shows count, sum, and bucket placement matching the observation.
func TestLatencyObservedThroughSeamClock(t *testing.T) {
	// A latent network so each operation spans real virtual time — the seam
	// clock advances across the PUT and GET, which is exactly what the recorded
	// duration must capture.
	c := newCluster(t, 1, sim.NetConfig{MinLatency: 5 * time.Millisecond, MaxLatency: 10 * time.Millisecond}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})

	// A real histogram, wired exactly as internal/cluster wires the hook.
	reg := metrics.NewRegistry()
	hist := reg.NewHistogram("hamster_s3_request_duration_seconds",
		"Data-plane S3 operation latency in seconds, by method.",
		metrics.DefaultLatencyBuckets, "method")

	type obs struct {
		id      seam.NodeID
		op      string
		seconds float64
	}
	var observed []obs
	c.onLatency = func(id seam.NodeID, op string, seconds float64) {
		observed = append(observed, obs{id, op, seconds})
		hist.Observe(seconds, op)
	}

	lead := c.leader()
	body := randomBody(1, 200_000) // multi-window: a non-trivial, multi-tick op
	pStart, pEnd := c.putCapturingClock("obj", body)
	gStart, gEnd := c.getCapturingClock("obj", 0, -1)

	putWant := pEnd.Sub(pStart).Seconds()
	getWant := gEnd.Sub(gStart).Seconds()
	if putWant <= 0 || getWant <= 0 {
		t.Fatalf("expected positive durations, got put=%v get=%v", putWant, getWant)
	}

	// Exactly one observation per operation, with the right label and value.
	if len(observed) != 2 {
		t.Fatalf("expected 2 observations (one PUT, one GET), got %d: %+v", len(observed), observed)
	}
	gotPut, gotGet := false, false
	for _, o := range observed {
		switch o.op {
		case "PUT":
			gotPut = true
			if o.seconds != putWant {
				t.Errorf("PUT latency = %v, want seam-clock elapsed %v", o.seconds, putWant)
			}
		case "GET":
			gotGet = true
			if o.seconds != getWant {
				t.Errorf("GET latency = %v, want seam-clock elapsed %v", o.seconds, getWant)
			}
		default:
			t.Errorf("unexpected op label %q", o.op)
		}
		if o.id != lead {
			t.Errorf("observation from %s, expected the leader %s", o.id, lead)
		}
	}
	if !gotPut || !gotGet {
		t.Fatalf("expected both a PUT and a GET observation, got %+v", observed)
	}

	// The histogram snapshot reflects the two observations: one per method, with
	// the sum and bucket placement matching the recorded durations.
	checkHist(t, reg, "PUT", putWant)
	checkHist(t, reg, "GET", getWant)
}

// checkHist asserts the request-latency histogram has exactly one observation of
// value want under the given method label: count 1, sum want, and the cumulative
// bucket counts 1 from the first boundary covering want onward.
func checkHist(t *testing.T, reg *metrics.Registry, method string, want float64) {
	t.Helper()
	var hv *metrics.HistogramValue
	for _, f := range reg.Snapshot() {
		if f.Name != "hamster_s3_request_duration_seconds" {
			continue
		}
		for _, s := range f.Samples {
			if len(s.Labels) == 1 && s.Labels[0] == method {
				hv = s.Histogram
			}
		}
	}
	if hv == nil {
		t.Fatalf("no histogram sample for method %q", method)
	}
	if hv.Count != 1 {
		t.Errorf("%s count = %d, want 1", method, hv.Count)
	}
	if hv.Sum != want {
		t.Errorf("%s sum = %v, want %v", method, hv.Sum, want)
	}
	// Counts are cumulative: 1 once the boundary covers want, 0 before.
	for i, b := range hv.Bounds {
		wantCount := uint64(0)
		if want <= b {
			wantCount = 1
		}
		if hv.Counts[i] != wantCount {
			t.Errorf("%s bucket le=%v count = %d, want %d (observation %v)", method, b, hv.Counts[i], wantCount, want)
		}
	}
	// The +Inf bucket (final element) always holds every observation.
	if hv.Counts[len(hv.Counts)-1] != 1 {
		t.Errorf("%s +Inf bucket = %d, want 1", method, hv.Counts[len(hv.Counts)-1])
	}
}

// TestLatencyNotObservedOnError: only a completed operation is a service-time
// sample (ADR-0039) — a failed GET (a missing key) produces no observation, so
// the error path never pollutes the latency baseline the load shedder builds on.
func TestLatencyNotObservedOnError(t *testing.T) {
	c := newCluster(t, 2, sim.NetConfig{}, 6, profile(t, "4+2"))
	count := 0
	c.onLatency = func(_ seam.NodeID, _ string, _ float64) { count++ }

	if _, err := c.get("does-not-exist", 0, -1); err == nil {
		t.Fatal("expected an error reading a missing key")
	}
	if count != 0 {
		t.Fatalf("error path observed latency %d times, want 0", count)
	}
}
