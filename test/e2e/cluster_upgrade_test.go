//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestClusterRollingUpgrade is the end-to-end upgrade suite (ADR-0034,
// SIMULATION.md "the upgrade suite"): the layer-2 proof that a Hamster cluster
// rolls from one version to the next, node by node, without losing data or
// availability — and that the effective protocol generation auto-rolls etcd-style
// only once the last node has upgraded.
//
// Two binaries are built from this same source with different stamps: "N"
// (v0.9.0, generation 1) and "N+1" (v0.10.0, generation 2). v0.9 adds no
// coordinated format change, so the same source at two generations exercises the
// roll machinery honestly; the suite gains a real format-N binary the day a
// generation boundary actually lands. A three-node cluster starts on N, stores
// versioned and COMPLIANCE-locked data, then each node is taken down (honoring
// `can-stop`) and restarted on N+1. Throughout, the stored data stays
// readable and intact; the locked version stays WORM; and the effective
// generation holds at 1 until the final node lands, then rolls to 2.
func TestClusterRollingUpgrade(t *testing.T) {
	env := []string{"HAMSTER_ACCESS_KEY_ID=e2e-up", "HAMSTER_SECRET_ACCESS_KEY=e2e-up-secret"}
	binN := buildHamster(t, "v0.9.0", 1)
	binNext := buildHamster(t, "v0.10.0", 2)

	root := t.TempDir()
	nodes := []string{"n1", "n2", "n3"}
	dirs := map[string]string{}
	s3 := map[string]string{}
	procs := map[string]*proc{}
	for _, id := range nodes {
		dirs[id] = filepath.Join(root, id)
		s3[id] = freeAddr(t)
	}
	alive := func() []string {
		var out []string
		for _, id := range nodes {
			if procs[id] != nil {
				out = append(out, s3[id])
			}
		}
		return out
	}

	// Form a three-node cluster on binary N, every node serving S3.
	runBin(t, binN, "init", "-data-dir", dirs["n1"], "-cluster", "e2e-up", "-node", "n1", "-listen", freeAddr(t))
	procs["n1"] = startBin(t, binN, env, "serve", "-data-dir", dirs["n1"], "-s3", s3["n1"])
	waitStatusBin(t, binN, dirs["n1"], "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})
	for _, id := range nodes[1:] {
		token := strings.TrimSpace(runBin(t, binN, "token", "-data-dir", dirs["n1"]))
		procs[id] = startBin(t, binN, env, "serve", "-data-dir", dirs[id], "-node", id,
			"-listen", freeAddr(t), "-token", token, "-s3", s3[id])
	}
	waitStatusBin(t, binN, dirs["n1"], "three voters", func(rows []statusRow) bool {
		return len(rows) == 3 && voterCount(rows) == 3
	})

	// The whole cluster starts at generation 1.
	waitGen(t, binN, dirs["n1"], "effective generation 1 at the start", func(eff int, rolling bool) bool {
		return eff == 1 && !rolling
	})

	// Store a known workload in a lock-enabled (so versioned) bucket: two versions
	// of one key, a COMPLIANCE-locked object, and a plain object.
	c := &s3Client{t: t, akid: "e2e-up", secret: "e2e-up-secret", region: "us-east-1"}
	keepV1 := bodyOf(0x11, 60<<10)
	keepV2 := bodyOf(0x22, 80<<10)
	locked := bodyOf(0x33, 120<<10)
	plain := bodyOf(0x44, 40<<10)

	putHeaders(c, alive(), "PUT", "/vault", nil, map[string]string{"x-amz-bucket-object-lock-enabled": "true"})
	c.mutate(alive(), "PUT", "/vault/keep", keepV1, http.StatusOK)
	c.mutate(alive(), "PUT", "/vault/keep", keepV2, http.StatusOK)
	lockResp := putHeaders(c, alive(), "PUT", "/vault/locked", locked, map[string]string{
		"x-amz-object-lock-mode":              "COMPLIANCE",
		"x-amz-object-lock-retain-until-date": "2099-01-01T00:00:00Z",
	})
	lockedVID := lockResp.Header.Get("x-amz-version-id")
	if lockedVID == "" {
		t.Fatal("locked PUT returned no version id")
	}
	c.mutate(alive(), "PUT", "/vault/plain", plain, http.StatusOK)

	// readsIntact asserts every object survives, current bytes and lock alike —
	// run between every roll step to prove continuous availability and zero loss.
	readsIntact := func(stage string) {
		t.Helper()
		c.getEventually(alive(), "/vault/keep", keepV2)
		c.getEventually(alive(), "/vault/locked", locked)
		c.getEventually(alive(), "/vault/plain", plain)
		// The COMPLIANCE lock survives the upgrade (invariant 4): the version still
		// refuses deletion at the leader.
		assertWORM(c, alive(), "/vault/locked?"+canonicalQuery(map[string]string{"versionId": lockedVID}), stage)
	}
	readsIntact("before the roll")

	// Roll node by node to N+1. Followers first, the founder last — so the final
	// node carries the generation forward and leadership moves to an upgraded peer.
	for _, id := range []string{"n3", "n2", "n1"} {
		// A node that is up to query status/can-stop from (not the one being rolled).
		probe := "n1"
		if id == "n1" {
			probe = "n2"
		}

		// The interlock must say it is safe to stop this node (full health: quorum
		// holds, no other node down, no transition open).
		waitCanStop(t, binNext, dirs[probe], id)

		// Take it down and bring it back on the next binary, from its own disk.
		procs[id].interrupt(t)
		procs[id] = nil
		readsIntact("with " + id + " down")
		procs[id] = startBin(t, binNext, env, "serve", "-data-dir", dirs[id], "-s3", s3[id])
		waitStatusBin(t, binNext, dirs[probe], id+" back among three voters", func(rows []statusRow) bool {
			return len(rows) == 3 && voterCount(rows) == 3 && !anyDown(rows)
		})
		readsIntact("after " + id + " upgraded")

		if id != "n1" {
			// Still one node (the founder) behind, so the effective generation holds
			// at 1 while the cluster reports a roll in progress.
			waitGen(t, binNext, dirs[probe], "effective holds at 1 mid-roll ("+id+")", func(eff int, rolling bool) bool {
				return eff == 1 && rolling
			})
		}
	}

	// The last node has upgraded: the effective generation auto-rolls to 2, no
	// manual finalize, and the roll-in-progress note clears.
	waitGen(t, binNext, dirs["n1"], "effective generation rolls to 2", func(eff int, rolling bool) bool {
		return eff == 2 && !rolling
	})
	readsIntact("after the full roll")

	for _, id := range nodes {
		if procs[id] != nil {
			procs[id].interrupt(t)
		}
	}
}

// buildHamster builds cmd/hamster with a stamped release version and protocol
// generation (ADR-0034), so the upgrade suite can run two generations from one
// source.
func buildHamster(t *testing.T, version string, generation int) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "hamster-e2e-"+version+"-*")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "hamster")
	ldflags := fmt.Sprintf("-X main.version=%s -X main.protocolGenerationStr=%d", version, generation)
	cmd := exec.Command("go", "build", "-ldflags", ldflags, "-o", bin, "github.com/hamster-storage/hamster/cmd/hamster")
	cmd.Dir = "../.."
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building hamster %s (gen %d): %v\n%s", version, generation, err, out)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return bin
}

// bodyOf builds a deterministic, distinguishable object body.
func bodyOf(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed ^ byte(i)
	}
	return b
}

// anyDown reports whether any member's STATE is down.
func anyDown(rows []statusRow) bool {
	for _, r := range rows {
		if r.down {
			return true
		}
	}
	return false
}

// assertWORM proves a COMPLIANCE-locked version still refuses deletion (invariant
// 4): it polls the live nodes until the leader answers 403, tolerating a
// non-leader's 503, and fails if any node actually deletes it.
func assertWORM(c *s3Client, addrs []string, path, stage string) {
	c.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		for _, addr := range addrs {
			resp, _ := c.do(addr, "DELETE", path, nil)
			if resp == nil {
				continue
			}
			switch resp.StatusCode {
			case http.StatusForbidden:
				return // WORM intact — the leader refused
			case http.StatusOK, http.StatusNoContent:
				c.t.Fatalf("%s: COMPLIANCE-locked version was deleted (status %d)", stage, resp.StatusCode)
			}
			// 503 from a non-leader — try the next node.
		}
		time.Sleep(300 * time.Millisecond)
	}
	c.t.Fatalf("%s: no node refused deleting the locked version with 403", stage)
}

// putHeaders drives one header-bearing write to whichever live node commits it
// (the leader; non-leaders answer 503), returning that response. Like writeH but
// against an explicit address set, for the self-contained upgrade harness.
func putHeaders(c *s3Client, addrs []string, method, path string, body []byte, hdrs map[string]string) *http.Response {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		for _, addr := range addrs {
			resp, rb := c.doH(addr, method, path, body, hdrs)
			if resp == nil {
				continue
			}
			if resp.StatusCode == http.StatusOK {
				return resp
			}
			if resp.StatusCode == http.StatusServiceUnavailable {
				continue
			}
			c.t.Fatalf("%s %s on %s: status %d\n%s", method, path, addr, resp.StatusCode, rb)
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatalf("%s %s: no node committed before the deadline", method, path)
	return nil
}

// waitCanStop polls the interlock until it reports the node safe to stop
// (ADR-0034) — full health may take a moment to settle after a prior step.
func waitCanStop(t *testing.T, binPath, dir, node string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		out, err := exec.Command(binPath, "can-stop", "-data-dir", dir, "-node", node).CombinedOutput()
		last = string(out)
		if err == nil && strings.Contains(last, "YES") {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("interlock never cleared %s for stop; last: %s", node, last)
}

// waitGen polls `status` for the effective protocol generation and
// whether a roll is in progress (ADR-0034), until pred holds.
func waitGen(t *testing.T, binPath, dir, what string, pred func(effective int, rolling bool) bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastEff int
	var lastRolling bool
	for time.Now().Before(deadline) {
		out, err := exec.Command(binPath, "status", "-data-dir", dir).CombinedOutput()
		if err == nil {
			lastEff, lastRolling = parseGen(string(out))
			if pred(lastEff, lastRolling) {
				return
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("waiting for %s; last effective=%d rolling=%v", what, lastEff, lastRolling)
}

// parseGen extracts the effective generation and the roll-in-progress flag from
// `status` output (ADR-0034). The effective value is the number directly
// after the word "effective" (the "...: effective N" line) — matched precisely so
// the roll-in-progress line, which also says "effective generation", is not read
// as the value. "span generation" appears only on the roll-in-progress line.
func parseGen(out string) (effective int, rolling bool) {
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		for i, f := range fields {
			if f == "effective" && i+1 < len(fields) {
				if n, err := strconv.Atoi(strings.TrimRight(fields[i+1], ":;,.")); err == nil {
					effective = n
				}
			}
		}
		if strings.Contains(line, "span generation") {
			rolling = true
		}
	}
	return
}
