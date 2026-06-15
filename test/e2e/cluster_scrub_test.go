//go:build e2e

package e2e

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestClusterScrubHealsBitrot proves the background scrubber heals silent
// corruption in a real cluster, with no operator action. An object is stored,
// then one of its shard files is bitrotted on disk (a byte flipped, its commit
// marker left intact — exactly what a failing disk serves). The leader's
// continuous scrub must, on its own, find the mismatch and rebuild the shard;
// the proof is its repair log line. The deterministic signal is that log entry,
// polled for — not a fixed sleep.
func TestClusterScrubHealsBitrot(t *testing.T) {
	env := []string{"HAMSTER_ACCESS_KEY_ID=e2e-scrub", "HAMSTER_SECRET_ACCESS_KEY=e2e-scrub-secret"}
	cl := startCluster(t, "e2e-scrub", 3, env)
	c := &s3Client{t: t, akid: "e2e-scrub", secret: "e2e-scrub-secret", region: "us-east-1"}

	// Store one object (> 128 KiB, so it is erasure-coded across the three nodes).
	c.mutate(cl.alive(), "PUT", "/vault", nil, http.StatusOK)
	body := make([]byte, 300<<10)
	for i := range body {
		body[i] = byte(i * 7)
	}
	c.mutate(cl.alive(), "PUT", "/vault/obj", body, http.StatusOK)
	c.getEventually(cl.alive(), "/vault/obj", body)

	// Bitrot one shard on n2: flip a payload byte, leave the .ok marker, so the
	// holder still reports the shard committed but its hash no longer matches what
	// the metadata recorded — the case scrub exists to catch.
	corruptOneShard(t, cl.dirs["n2"])

	// The leader scrubs continuously and must heal it on its own. Poll the leader's
	// log for the repair line — no operator sweep, no fixed sleep.
	rows := waitStatus(t, cl.adminDir, "a leader", func(rows []statusRow) bool { return leaderOf(rows) != "" })
	leadProc := cl.procs[leaderOf(rows)]
	healed := false
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(leadProc.out.String(), "scrub repaired vault/obj") {
			healed = true
			break
		}
		time.Sleep(time.Second)
	}
	if !healed {
		t.Fatalf("scrubber never logged repair of the bitrotted shard:\n%s", leadProc.out.String())
	}

	// The object still reads, intact, after the rebuild.
	c.getEventually(cl.alive(), "/vault/obj", body)
}

// corruptOneShard flips a byte in one shard file under dataDir/shards, leaving
// its .ok commit marker — silent bitrot, as a failing disk would serve it.
func corruptOneShard(t *testing.T, dataDir string) {
	t.Helper()
	dir := filepath.Join(dataDir, "shards")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasSuffix(name, ".ok") {
			continue // skip the commit markers; corrupt the shard itself
		}
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil || len(data) == 0 {
			continue
		}
		data[len(data)/2] ^= 0xff // a whole-file hash, so any flip is detected
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("writing corrupted shard: %v", err)
		}
		return
	}
	t.Fatalf("no shard file found under %s", dir)
}
