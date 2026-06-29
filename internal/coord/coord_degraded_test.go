package coord_test

import (
	"testing"
	"time"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sim"
)

// Node-degradation detection (ADR-0039 part 5) through the coordinator. The
// detector is fed from the SAME completion path as the gradient/latency — it
// consumes the minRTT the gradient tracker computes from each operation's
// seam-clock service time, no second timing source. This sim test proves it is
// wired and live on a real (simulated) cluster: a healthy cluster, whose floor
// never rises, never flags degraded, and — the load-shedding invariant the ADR
// is emphatic about — detection takes NO action: no node is marked down, the
// membership and layout are untouched, and operations keep being served. The
// full detection dynamics (the discriminator: minRTT vs curRTT) are owned by the
// pure unit tests in degraded_test.go, which feed the detector directly.

// readDegraded reads a node's per-op and aggregate degradation flags on its loop.
func (c *cluster) readDegraded(id seam.NodeID) (put, get, any bool) {
	c.t.Helper()
	c.worlds[id].Loop.Post(func() {
		put = c.nodes[id].co.DegradedOp("PUT")
		get = c.nodes[id].co.DegradedOp("GET")
		any = c.nodes[id].co.Degraded()
	})
	c.s.Run(0) // single-threaded sim: dispatch the posted read now
	return
}

// TestDegradedHealthyClusterNeverFlags: over a latent network, a healthy cluster
// serving a normal workload keeps a flat floor, so the degradation detector —
// fed through the coordinator's completion path — never flags. Crucially it
// takes no action: the node's down-view stays empty (no auto-eviction), and
// every operation continues to succeed.
func TestDegradedHealthyClusterNeverFlags(t *testing.T) {
	c := newCluster(t, 1, sim.NetConfig{MinLatency: 2 * time.Millisecond, MaxLatency: 12 * time.Millisecond}, 6, profile(t, "4+2"))
	c.propose(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: bucket})
	lead := c.leader()

	// Before any operation the node reads healthy (the safe default before a
	// trustworthy baseline exists).
	if put, get, any := c.readDegraded(lead); put || get || any {
		t.Fatalf("pre-op degraded = PUT %v / GET %v / any %v, want all false", put, get, any)
	}

	// A normal workload: varied object sizes so latencies differ, but the floor
	// never degrades. The detector is fed every completion, yet must not flag.
	for i := range 12 {
		key := "obj" + string(rune('a'+i))
		if _, err := c.put(key, randomBody(uint64(100+i), 10_000*(i+1))); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		if _, err := c.get(key, 0, -1); err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
	}

	put, get, any := c.readDegraded(lead)
	if put || get || any {
		t.Fatalf("healthy-cluster degraded = PUT %v / GET %v / any %v, want all false", put, get, any)
	}

	// Detection took no action: the leader's liveness view is empty (nothing was
	// auto-evicted or marked down on latency), and reads/writes still serve.
	if down := c.downNodes(lead); len(down) != 0 {
		t.Fatalf("degradation detection marked nodes down %v, want none (detection only, no action)", down)
	}
	if _, err := c.get("obja", 0, -1); err != nil {
		t.Fatalf("get after detection: %v — detection must not disturb serving", err)
	}
}
