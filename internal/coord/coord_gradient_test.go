package coord_test

import (
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sim"
)

// Per-operation RTT gradient (ADR-0039 part 2). The coordinator maintains, per
// op type, minRTT (a re-probing long-window minimum) and curRTT (a short-window
// EWMA), and derives the clamped gradient. This sim test proves the tracker is
// fed from the SAME seam-clock latency sample observeLatency reports — minRTT
// equals the minimum of the observed PUT/GET latencies — and that the accessors
// are internally consistent, all through the deterministic harness.

// readGradient reads a node's per-op gradient/minRTT/curRTT on its loop.
func (c *cluster) readGradient(id seam.NodeID, op string) (gradient, minRTT, curRTT float64) {
	c.t.Helper()
	c.worlds[id].Loop.Post(func() {
		gradient = c.nodes[id].co.Gradient(op)
		minRTT = c.nodes[id].co.MinRTT(op)
		curRTT = c.nodes[id].co.CurRTT(op)
	})
	c.s.Run(0) // single-threaded sim: dispatch the posted read now
	return
}

// TestGradientTracksObservedLatency: over a latent network, the per-op gradient
// state updates from the exact same seam-clock samples the latency observer sees.
// minRTT equals the minimum observed latency (proving a single timing source
// shared with observeLatency), curRTT sits at or above minRTT, the gradient is
// the clamped ratio, and it reads 1 (healthy) before warmup.
func TestGradientTracksObservedLatency(t *testing.T) {
	c := newCluster(t, 1, sim.NetConfig{MinLatency: 2 * time.Millisecond, MaxLatency: 12 * time.Millisecond}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	lead := c.leader()

	// Capture every latency sample the coordinator observes on the leader — the
	// same value its gradient tracker is fed.
	var putObs, getObs []float64
	c.onLatency = func(id seam.NodeID, op string, seconds float64) {
		if id != lead {
			return
		}
		switch op {
		case "PUT":
			putObs = append(putObs, seconds)
		case "GET":
			getObs = append(getObs, seconds)
		}
	}

	// Before any operation the tracker is healthy: gradient 1, RTTs 0.
	if g, mn, cur := c.readGradient(lead, "PUT"); g != 1 || mn != 0 || cur != 0 {
		t.Fatalf("pre-op PUT gradient/min/cur = %v/%v/%v, want 1/0/0", g, mn, cur)
	}

	// One PUT: still under warmup, so the gradient reads 1 even though a sample
	// has landed (minRTT now tracks it).
	if _, err := c.put("obj0", randomBody(100, 50_000)); err != nil {
		t.Fatalf("put obj0: %v", err)
	}
	if len(putObs) != 1 {
		t.Fatalf("after one PUT observed %d samples, want 1", len(putObs))
	}
	if g, _, _ := c.readGradient(lead, "PUT"); g != 1 {
		t.Errorf("after one PUT gradient = %v, want 1 (warmup)", g)
	}

	// Drive enough PUTs and GETs to clear warmup, varied sizes so latencies differ.
	for i := 1; i <= 8; i++ {
		key := "obj" + string(rune('0'+i))
		if _, err := c.put(key, randomBody(uint64(100+i), 20_000*i)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		if _, err := c.get(key, 0, -1); err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
	}

	assertOp := func(op string, obs []float64) {
		t.Helper()
		if len(obs) == 0 {
			t.Fatalf("%s: no observed samples", op)
		}
		g, mn, cur := c.readGradient(lead, op)

		// minRTT equals the minimum observed latency: fewer ops than a re-probe
		// window, so the window minimum is the overall minimum — and it can only be
		// that if the tracker was fed exactly the observed samples (single source).
		wantMin := obs[0]
		for _, s := range obs[1:] {
			wantMin = min(wantMin, s)
		}
		if mn != wantMin {
			t.Errorf("%s minRTT = %v, want min observed %v (single timing source)", op, mn, wantMin)
		}
		// curRTT is an EWMA of samples each >= minRTT, so it never dips below it.
		if cur < mn {
			t.Errorf("%s curRTT %v < minRTT %v", op, cur, mn)
		}
		// The gradient is the clamped ratio of the two exposed values.
		wantG := min(max(mn/cur, 0), 1)
		if g != wantG {
			t.Errorf("%s gradient = %v, want clamp(minRTT/curRTT) = %v", op, g, wantG)
		}
		if g <= 0 || g > 1 {
			t.Errorf("%s gradient = %v, want in (0,1]", op, g)
		}
	}
	assertOp("PUT", putObs)
	assertOp("GET", getObs)
}
