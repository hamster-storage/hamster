//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net/http"
	"testing"
)

// TestClusterProfileLadder runs the erasure-coded data path across every storage
// profile on the auto ladder, selected by node count (ADR-0015): 3 nodes → 2+1,
// 5 → 3+2, 6 → 4+2. Each profile writes objects spanning the small-object k=1
// rule, a single stripe, and several stripes; verifies listing, whole-object
// reads, and ranges; then proves the profile's durability — losing m nodes,
// every object still reads, reconstructed from the k survivors. Width equals the
// node count at each rung, so every lost node drops a shard from every object,
// forcing reconstruction rather than relying on lucky placement.
//
// The breadth of the listing surface (pagination, prefixes, delimiters) is
// gateway logic independent of the profile and is covered once by
// TestClusterObjects; this test is the EC matrix.
func TestClusterProfileLadder(t *testing.T) {
	cases := []struct {
		name        string
		nodes, k, m int
	}{
		{"2+1", 3, 2, 1},
		{"3+2", 5, 3, 2},
		{"4+2", 6, 4, 2},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			env := []string{"HAMSTER_ACCESS_KEY_ID=e2e-lad", "HAMSTER_SECRET_ACCESS_KEY=e2e-lad-secret"}
			cl := startCluster(t, "e2e-lad-"+tc.name, tc.nodes, env)
			c := &s3Client{t: t, akid: "e2e-lad", secret: "e2e-lad-secret", region: "us-east-1"}
			lead := cl.leaderS3() // strongly consistent reads for list/range

			// Write a spread of sizes: a small-object k=1 copy, a single stripe, and
			// several stripes (the last for the range reads).
			c.mutate(cl.alive(), "PUT", "/vault", nil, http.StatusOK)
			rng := rand.New(rand.NewPCG(uint64(tc.nodes), 99))
			const bigKey = "big"
			bodies := map[string][]byte{}
			put := func(key string, size int) {
				bodies[key] = randBytes(rng, size)
				c.mutate(cl.alive(), "PUT", "/vault/"+key, bodies[key], http.StatusOK)
			}
			put("small", 4<<10) // < 128 KiB: stored as k=1 copies
			put("a", 200<<10)
			put("b", 512<<10)
			put("c", 1<<20+7)
			put(bigKey, 2<<20+11) // multi-stripe, exercised by the ranges

			// Listing sees every object.
			if got := c.listAllV2(lead, "vault", nil, 100); len(got) != len(bodies) {
				t.Fatalf("listing: %d keys, want %d", len(got), len(bodies))
			}

			// Whole-object reads are bit-identical.
			for key, body := range bodies {
				c.getEventually(cl.alive(), "/vault/"+key, body)
			}

			// Ranges over the multi-stripe object: a leading slice, one crossing a
			// megabyte boundary, and a suffix — each 206 and exact, exercising the
			// random-access reconstruct at this profile.
			big := bodies[bigKey]
			n := int64(len(big))
			for _, rg := range []struct {
				hdr        string
				first, end int64
			}{
				{"bytes=0-99", 0, 100},
				{"bytes=1048570-1049005", 1048570, 1049006},
				{"bytes=-128", n - 128, n},
			} {
				status, b := c.getRange(lead, "/vault/"+bigKey, rg.hdr)
				if status != http.StatusPartialContent || !bytes.Equal(b, big[rg.first:rg.end]) {
					t.Fatalf("range %s: status %d, %d bytes want %d", rg.hdr, status, len(b), rg.end-rg.first)
				}
			}

			// Durability: lose m nodes (skipping the leader so reads keep a serving
			// node), and every object still reads — reconstructed from the k
			// survivors at this profile.
			rows := waitStatus(t, cl.adminDir, "a leader", func(rows []statusRow) bool { return leaderOf(rows) != "" })
			leadNode := leaderOf(rows)
			killed := 0
			for i := tc.nodes; i >= 1 && killed < tc.m; i-- {
				id := fmt.Sprintf("n%d", i)
				if id == leadNode {
					continue
				}
				cl.kill(id)
				killed++
			}
			t.Logf("%s: killed %d of %d nodes; every object must still read", tc.name, killed, tc.nodes)
			for key, body := range bodies {
				c.getEventually(cl.alive(), "/vault/"+key, body)
			}
		})
	}
}
