//go:build e2e

package e2e

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

// TestClusterGrowthKeepsDataReadable: objects written to a small cluster stay
// readable after it grows (ADR-0004). Placement is positional, so adding a node
// reshuffles which node holds shard i — without a transition that would strand
// the existing objects. A join with data therefore opens a transition: reads
// dual-read each object at its old home while repair migrates it to the new one.
//
// A 3-node cluster (2+1) grown to four (still 2+1, so this isolates the growth
// reshuffle from any profile change). Every object must read both right after the
// join — over the dual-read — and once the cluster has settled.
func TestClusterGrowthKeepsDataReadable(t *testing.T) {
	const (
		akid   = "e2e-grow"
		secret = "e2e-grow-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	root := t.TempDir()
	dirs := map[string]string{}
	procs := map[string]*proc{}
	s3Addrs := map[string]string{}

	dirs["n1"] = filepath.Join(root, "n1")
	s3Addrs["n1"] = freeAddr(t)
	run(t, "cluster", "init", "-data-dir", dirs["n1"], "-cluster", "e2e-grow", "-node", "n1", "-listen", freeAddr(t))
	procs["n1"] = start(t, env, "cluster", "run", "-data-dir", dirs["n1"], "-s3", s3Addrs["n1"])
	waitStatus(t, dirs["n1"], "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})

	join := func(id string) {
		token := strings.TrimSpace(run(t, "cluster", "token", "-data-dir", dirs["n1"]))
		dirs[id] = filepath.Join(root, id)
		s3Addrs[id] = freeAddr(t)
		procs[id] = start(t, env, "cluster", "run", "-data-dir", dirs[id], "-node", id,
			"-listen", freeAddr(t), "-token", token, "-s3", s3Addrs[id])
	}
	join("n2")
	join("n3")
	waitStatus(t, dirs["n1"], "three voters", func(rows []statusRow) bool {
		return len(rows) == 3 && voterCount(rows) == 3
	})

	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	alive := func() []string {
		var out []string
		for _, id := range []string{"n1", "n2", "n3", "n4"} {
			if procs[id] != nil {
				out = append(out, s3Addrs[id])
			}
		}
		return out
	}

	// Store objects of a spread of sizes across the 2+1 cluster.
	c.mutate(alive(), "PUT", "/vault", nil, http.StatusOK)
	rng := rand.New(rand.NewPCG(13, 0))
	bodies := map[string][]byte{}
	for i, size := range []int{1 << 10, 64 << 10, 200 << 10, 1 << 20, 3<<20 + 5} {
		key := fmt.Sprintf("obj-%d", i)
		bodies[key] = randBytes(rng, size)
		c.mutate(alive(), "PUT", "/vault/"+key, bodies[key], http.StatusOK)
	}
	for key, body := range bodies {
		c.getEventually(alive(), "/vault/"+key, body)
	}

	// Grow to four nodes. The join opens a transition (data is at risk), so every
	// object must still read — first over the dual-read, immediately.
	join("n4")
	waitStatus(t, dirs["n1"], "four members", func(rows []statusRow) bool { return len(rows) == 4 })
	for key, body := range bodies {
		c.getEventually(alive(), "/vault/"+key, body)
	}

	// And once the migration converges, with the original three nodes still up.
	waitStatus(t, dirs["n1"], "four voters settled", func(rows []statusRow) bool {
		return len(rows) == 4 && voterCount(rows) == 4
	})
	for key, body := range bodies {
		c.getEventually(alive(), "/vault/"+key, body)
	}
}
