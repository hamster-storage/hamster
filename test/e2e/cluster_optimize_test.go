//go:build e2e

package e2e

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
)

// TestClusterOptimizeAfterGrowth is the upsize re-encode (ADR-0004, ADR-0031):
// data written to a small cluster is widened to the grown cluster's profile by
// an explicit `optimize`, never automatically. A 4-node cluster (2+1)
// grows to five (3+2 territory); after optimize, every object tolerates two
// failures where 2+1 tolerated one — proven by killing two nodes and still
// reading. Objects are all above the 128 KiB small-object threshold, so they are
// genuinely erasure-coded, not k=1 copies.
func TestClusterOptimizeAfterGrowth(t *testing.T) {
	env := []string{"HAMSTER_ACCESS_KEY_ID=e2e-opt", "HAMSTER_SECRET_ACCESS_KEY=e2e-opt-secret"}
	cl := startCluster(t, "e2e-opt", 4, env)
	c := &s3Client{t: t, akid: "e2e-opt", secret: "e2e-opt-secret", region: "us-east-1"}

	// Store objects at 2+1 across the four-node cluster.
	c.mutate(cl.alive(), "PUT", "/vault", nil, http.StatusOK)
	rng := rand.New(rand.NewPCG(11, 4))
	bodies := map[string][]byte{}
	for i, size := range []int{300 << 10, 600 << 10, 1<<20 + 7, 2<<20 + 3, 800 << 10} {
		key := fmt.Sprintf("obj-%d", i)
		bodies[key] = randBytes(rng, size)
		c.mutate(cl.alive(), "PUT", "/vault/"+key, bodies[key], http.StatusOK)
	}
	for key, body := range bodies {
		c.getEventually(cl.alive(), "/vault/"+key, body)
	}

	// Grow to five nodes. The join opens a transition that migrates the existing
	// objects to their five-node placement — still 2+1 until an optimize.
	cl.join("n5")
	waitStatus(t, cl.adminDir, "five voters", func(rows []statusRow) bool {
		return len(rows) == 5 && voterCount(rows) == 5
	})
	for key, body := range bodies {
		c.getEventually(cl.alive(), "/vault/"+key, body)
	}

	// A single optimize call does the right thing — no polling, no sleep. The
	// leader reconciles the new node in asynchronously, so optimizing this soon
	// after the join would otherwise race a node count about to change; instead the
	// leader reports "still reconciling" as a retryable refusal and the command
	// waits it out, then re-encodes 2+1 → 3+2. The "re-encoded" line proves it
	// widened rather than no-op'ing against a stale layout.
	out := run(t, "optimize", "-data-dir", cl.adminDir)
	if !strings.Contains(out, "re-encoded") {
		t.Fatalf("optimize did not widen the data (stale-layout no-op?):\n%s", out)
	}
	t.Logf("optimize widened the data: %s", strings.TrimSpace(out))

	// A second optimize is a clean no-op: everything already fits the profile.
	if out := run(t, "optimize", "-data-dir", cl.adminDir); strings.Contains(out, "re-encoded") {
		t.Fatalf("second optimize re-encoded again; should have been a no-op:\n%s", out)
	}

	// The objects are now 3+2 and tolerate two failures: kill two non-leader nodes
	// and every object still reads, reconstructed at the wider profile — durability
	// the original 2+1 could not give.
	rows := waitStatus(t, cl.adminDir, "a leader", func(rows []statusRow) bool { return leaderOf(rows) != "" })
	lead := leaderOf(rows)
	killed := 0
	for _, id := range []string{"n5", "n4", "n3", "n2", "n1"} {
		if killed == 2 {
			break
		}
		if id == lead {
			continue
		}
		cl.kill(id)
		killed++
	}
	for key, body := range bodies {
		c.getEventually(cl.alive(), "/vault/"+key, body)
	}
}
