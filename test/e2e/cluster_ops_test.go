//go:build e2e

package e2e

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestClusterReplace is suite point 4, the replace operation in isolation: a
// fresh node joins with -replaces, swapping in for an existing member at the
// same cluster size (ADR-0004). The old node's shards migrate to the new one —
// a pure migrate, profile unchanged — and the old node is evicted automatically.
// Four nodes (2+1); n4 is replaced by n5, and every object still reads.
func TestClusterReplace(t *testing.T) {
	env := []string{"HAMSTER_ACCESS_KEY_ID=e2e-rep", "HAMSTER_SECRET_ACCESS_KEY=e2e-rep-secret"}
	cl := startCluster(t, "e2e-rep", 4, env)
	c := &s3Client{t: t, akid: "e2e-rep", secret: "e2e-rep-secret", region: "us-east-1"}

	// Store objects of a spread of sizes across the 2+1 cluster.
	c.mutate(cl.alive(), "PUT", "/vault", nil, http.StatusOK)
	rng := rand.New(rand.NewPCG(5, 5))
	bodies := map[string][]byte{}
	for i, size := range []int{1 << 10, 64 << 10, 300 << 10, 1<<20 + 9, 2<<20 + 3} {
		key := fmt.Sprintf("obj-%d", i)
		bodies[key] = randBytes(rng, size)
		c.mutate(cl.alive(), "PUT", "/vault/"+key, bodies[key], http.StatusOK)
	}
	for key, body := range bodies {
		c.getEventually(cl.alive(), "/vault/"+key, body)
	}

	// Replace n4 with a fresh n5 at constant size. The intent rides the join, so
	// the swap is atomic — n5 is never counted as a fifth member.
	cl.join("n5", "-replaces", "n4")

	// The cluster converges back to four members: n5 in, n4 evicted.
	cl.markStopped("n4") // n4 self-stops on eviction; stop routing S3 to it
	waitStatus(t, cl.adminDir, "n5 in and n4 gone", func(rows []statusRow) bool {
		if len(rows) != 4 {
			return false
		}
		var hasN5, hasN4 bool
		for _, r := range rows {
			switch r.node {
			case "n5":
				hasN5 = true
			case "n4":
				hasN4 = true
			}
		}
		return hasN5 && !hasN4
	})

	// Every object still reads — migrated n4 → n5 at the same profile.
	for key, body := range bodies {
		c.getEventually(cl.alive(), "/vault/"+key, body)
	}
}

// TestClusterOneLayoutOpAtATime is suite point 4's back-to-back case: only one
// layout change runs at a time (ADR-0004, layoutOpInProgress). A drain is
// started, a second drain is refused while it is in flight, and once the first
// converges and the node is removed the guard releases — the cluster is left
// quiescent and readable, not deadlocked.
func TestClusterOneLayoutOpAtATime(t *testing.T) {
	env := []string{"HAMSTER_ACCESS_KEY_ID=e2e-one", "HAMSTER_SECRET_ACCESS_KEY=e2e-one-secret"}
	cl := startCluster(t, "e2e-one", 4, env)
	c := &s3Client{t: t, akid: "e2e-one", secret: "e2e-one-secret", region: "us-east-1"}

	// A little data so the drain opens a real transition (hasStoredObjects).
	c.mutate(cl.alive(), "PUT", "/vault", nil, http.StatusOK)
	rng := rand.New(rand.NewPCG(3, 9))
	bodies := map[string][]byte{}
	for i, size := range []int{64 << 10, 400 << 10, 1<<20 + 1} {
		key := fmt.Sprintf("obj-%d", i)
		bodies[key] = randBytes(rng, size)
		c.mutate(cl.alive(), "PUT", "/vault/"+key, bodies[key], http.StatusOK)
	}
	for key, body := range bodies {
		c.getEventually(cl.alive(), "/vault/"+key, body)
	}

	// First op: drain n4 (4→3 active, still 2+1, so no prompt). Sets n4 draining
	// and opens the transition.
	run(t, "cluster", "drain", "-data-dir", cl.adminDir, "-node", "n4")

	// Second op while the first is in flight: drain n3 is refused. (-reencode only
	// skips the interactive downsize prompt; the layout guard refuses before any
	// re-encode is attempted, because n4 is already draining.)
	out, err := tryRun(t, "cluster", "drain", "-data-dir", cl.adminDir, "-node", "n3", "-reencode")
	if err == nil {
		t.Fatalf("second drain was accepted while the first was in flight:\n%s", out)
	}
	if !strings.Contains(out, "draining") && !strings.Contains(out, "transition") && !strings.Contains(out, "progress") {
		t.Fatalf("second drain refused, but not for the one-at-a-time guard:\n%s", out)
	}
	t.Logf("second drain correctly refused: %s", strings.TrimSpace(out))

	// n3 was never drained — it is still an active member.
	rows := waitStatus(t, cl.adminDir, "n3 still a member", func(rows []statusRow) bool {
		return len(rows) == 4
	})
	for _, r := range rows {
		if r.node == "n3" && r.role != "voter" {
			t.Fatalf("n3 lost its voter role to a refused drain: %+v", r)
		}
	}

	// The guard releases: once n4's migration converges it is removable, leaving a
	// quiescent three-node cluster — not a wedged one.
	cl.markStopped("n4")
	removed := false
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := tryRun(t, "cluster", "remove", "-data-dir", cl.adminDir, "-node", "n4"); err == nil {
			removed = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !removed {
		t.Fatal("remove never succeeded — the first drain did not converge after the second was refused")
	}
	waitStatus(t, cl.adminDir, "three members, none draining", func(rows []statusRow) bool {
		if len(rows) != 3 {
			return false
		}
		for _, r := range rows {
			if r.node == "n4" {
				return false
			}
		}
		return true
	})
	for key, body := range bodies {
		c.getEventually(cl.alive(), "/vault/"+key, body)
	}
}
