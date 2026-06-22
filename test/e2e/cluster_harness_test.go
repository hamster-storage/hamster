//go:build e2e

package e2e

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// cluster is a running e2e cluster of real hamster processes, each serving the
// S3 API — the shared harness the object and operation suites build on.
// startCluster founds an n-node cluster and waits for it to settle; alive lists
// the S3 endpoints of the nodes still running.
type cluster struct {
	t        *testing.T
	root     string
	env      []string
	nodes    []string
	dirs     map[string]string
	procs    map[string]*proc
	s3Addrs  map[string]string
	adminDir string // n1's data dir — for status and admin commands (they auto-redirect to the leader)

	mu sync.Mutex
}

// startCluster founds an n-node cluster named name (n1 the founder), every node
// serving S3, and waits for all n members with the voter count the five-voter
// cap allows. The node count selects the auto storage profile (3-4 → 2+1, 5 →
// 3+2, 6 → 4+2), so a caller parametrizes coverage by passing n.
func startCluster(t *testing.T, name string, n int, env []string) *cluster {
	t.Helper()
	if n < 1 {
		t.Fatalf("startCluster: n=%d", n)
	}
	root := t.TempDir()
	c := &cluster{
		t:       t,
		root:    root,
		env:     env,
		dirs:    map[string]string{},
		procs:   map[string]*proc{},
		s3Addrs: map[string]string{},
	}
	for i := 1; i <= n; i++ {
		c.nodes = append(c.nodes, fmt.Sprintf("n%d", i))
	}

	c.adminDir = filepath.Join(root, "n1")
	c.dirs["n1"] = c.adminDir
	c.s3Addrs["n1"] = freeAddr(t)
	run(t, "cluster", "init", "-data-dir", c.dirs["n1"], "-cluster", name, "-node", "n1", "-listen", freeAddr(t))
	c.procs["n1"] = start(t, env, "cluster", "run", "-data-dir", c.dirs["n1"], "-s3", c.s3Addrs["n1"])
	waitStatus(t, c.dirs["n1"], "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})

	for _, id := range c.nodes[1:] {
		token := strings.TrimSpace(run(t, "cluster", "token", "-data-dir", c.dirs["n1"]))
		c.dirs[id] = filepath.Join(root, id)
		c.s3Addrs[id] = freeAddr(t)
		c.procs[id] = start(t, env, "cluster", "run", "-data-dir", c.dirs[id], "-node", id,
			"-listen", freeAddr(t), "-token", token, "-s3", c.s3Addrs[id])
	}
	voters := min(n, 5) // the five-voter cap; the rest stay learners
	waitStatus(t, c.dirs["n1"], fmt.Sprintf("%d members, %d voters", n, voters), func(rows []statusRow) bool {
		return len(rows) == n && voterCount(rows) == voters
	})
	return c
}

// alive lists the S3 addresses of the nodes still running.
func (c *cluster) alive() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, id := range c.nodes {
		if c.procs[id] != nil {
			out = append(out, c.s3Addrs[id])
		}
	}
	return out
}

// leaderS3 returns the S3 endpoint of the current leader — the strongly
// consistent node to read from, since every write commits there (leader-only in
// v0.3) and the gateway reads its local replica.
func (c *cluster) leaderS3() string {
	rows := waitStatus(c.t, c.adminDir, "a leader", func(rows []statusRow) bool {
		return leaderOf(rows) != ""
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.s3Addrs[leaderOf(rows)]
}

// followerS3 returns the S3 endpoint of a current non-leader voter — the node a
// forwarding test targets to prove a write that does not land on the leader is
// still committed (ADR-0037), not bounced.
func (c *cluster) followerS3() string {
	rows := waitStatus(c.t, c.adminDir, "a follower", func(rows []statusRow) bool {
		if leaderOf(rows) == "" {
			return false
		}
		for _, r := range rows {
			if !r.leader && !r.down {
				return true
			}
		}
		return false
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range rows {
		if !r.leader && !r.down {
			return c.s3Addrs[r.node]
		}
	}
	return ""
}

// join brings up a new node and joins it with a fresh token, serving S3.
// extraArgs carries flags like -replaces. It returns once the process is
// started — not yet a settled member; the caller waits on status.
func (c *cluster) join(id string, extraArgs ...string) {
	c.t.Helper()
	token := strings.TrimSpace(run(c.t, "cluster", "token", "-data-dir", c.adminDir))
	dir := filepath.Join(c.root, id)
	s3 := freeAddr(c.t)
	args := append([]string{"cluster", "run", "-data-dir", dir, "-node", id,
		"-listen", freeAddr(c.t), "-token", token, "-s3", s3}, extraArgs...)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dirs[id] = dir
	c.s3Addrs[id] = s3
	c.procs[id] = start(c.t, c.env, args...)
	for _, n := range c.nodes {
		if n == id {
			return
		}
	}
	c.nodes = append(c.nodes, id)
}

// markStopped drops a node from the alive set — for one the cluster evicted and
// that has self-stopped.
func (c *cluster) markStopped(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.procs[id] = nil
}

// kill stops a node (SIGINT, clean shutdown) and drops it from the alive set —
// for simulating a failure mid-test.
func (c *cluster) kill(id string) {
	c.mu.Lock()
	p := c.procs[id]
	c.procs[id] = nil
	c.mu.Unlock()
	if p != nil {
		p.interrupt(c.t)
	}
}
