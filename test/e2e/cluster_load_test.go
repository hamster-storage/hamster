//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// object is one stored key and the exact bytes a read must return.
type object struct {
	key  string
	body []byte
}

// TestClusterDrainUnderLoad is suite point 5: an operation runs while a client
// never stops — five reads and one write per iteration — and the contract is
// zero hard errors throughout. A GET always returns the bytes it stored (lag and
// a non-leader's 503 are retries, not failures; wrong bytes or a persistent
// non-200 are failures); a PUT always lands. The operation is a drain that opens
// a layout transition: shards migrate off the draining node while live traffic
// reads through the dual-read and writes steer onto the survivors.
//
// Four nodes (2+1) draining to three active (still 2+1, so no re-encode) isolate
// the transition's effect on live traffic from any profile change. The drained
// node is then decommissioned, proving the migration converged underneath the
// load.
func TestClusterDrainUnderLoad(t *testing.T) {
	const (
		akid   = "e2e-load"
		secret = "e2e-load-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	root := t.TempDir()
	nodes := []string{"n1", "n2", "n3", "n4"}
	dirs := map[string]string{}
	procs := map[string]*proc{}
	s3Addrs := map[string]string{}

	dirs["n1"] = filepath.Join(root, "n1")
	s3Addrs["n1"] = freeAddr(t)
	run(t, "cluster", "init", "-data-dir", dirs["n1"], "-cluster", "e2e-load", "-node", "n1", "-listen", freeAddr(t))
	procs["n1"] = start(t, env, "cluster", "run", "-data-dir", dirs["n1"], "-s3", s3Addrs["n1"])
	waitStatus(t, dirs["n1"], "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})
	for _, id := range nodes[1:] {
		token := strings.TrimSpace(run(t, "cluster", "token", "-data-dir", dirs["n1"]))
		dirs[id] = filepath.Join(root, id)
		s3Addrs[id] = freeAddr(t)
		procs[id] = start(t, env, "cluster", "run", "-data-dir", dirs[id], "-node", id,
			"-listen", freeAddr(t), "-token", token, "-s3", s3Addrs[id])
	}
	waitStatus(t, dirs["n1"], "four voters", func(rows []statusRow) bool {
		return len(rows) == 4 && voterCount(rows) == 4
	})

	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	// procs changes on the test goroutine (remove) while the load goroutine reads
	// it through alive — guard the shared map.
	var procMu sync.Mutex
	alive := func() []string {
		procMu.Lock()
		defer procMu.Unlock()
		var out []string
		for _, id := range nodes {
			if procs[id] != nil {
				out = append(out, s3Addrs[id])
			}
		}
		return out
	}

	// Seed the bucket with objects of a spread of sizes, all verified readable.
	c.mutate(alive(), "PUT", "/vault", nil, http.StatusOK)
	rng := rand.New(rand.NewPCG(42, 0))
	var seeded []object
	for i, size := range []int{1 << 10, 8 << 10, 64 << 10, 300 << 10, 1<<20 + 7} {
		key := fmt.Sprintf("seed-%d", i)
		body := randBytes(rng, size)
		c.mutate(alive(), "PUT", "/vault/"+key, body, http.StatusOK)
		seeded = append(seeded, object{key, body})
	}
	for _, o := range seeded {
		c.getEventually(alive(), "/vault/"+o.key, o.body)
	}

	// The background load. Errors are recorded, never fatal: t.Fatal off the test
	// goroutine is illegal, and we want the count, not the first one.
	stop := make(chan struct{})
	liveCh := make(chan []object, 1)
	var (
		reads, writes atomic.Int64
		errMu         sync.Mutex
		loadErrs      []string
	)
	fail := func(format string, a ...any) {
		errMu.Lock()
		loadErrs = append(loadErrs, fmt.Sprintf(format, a...))
		errMu.Unlock()
	}
	// get reads one object from any live node, retrying transient failures (a
	// node coming/going, a follower's lag, a non-leader 503) for up to 20s. Wrong
	// bytes or a persistent non-200 are recorded failures.
	get := func(key string, want []byte) {
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			for _, addr := range alive() {
				resp, body := c.do(addr, "GET", "/vault/"+key, nil)
				if resp == nil || resp.StatusCode == http.StatusServiceUnavailable {
					continue
				}
				if resp.StatusCode != http.StatusOK {
					fail("GET %s: status %d", key, resp.StatusCode)
					return
				}
				if !bytes.Equal(body, want) {
					fail("GET %s: %d bytes, want %d — corruption, not lag", key, len(body), len(want))
					return
				}
				reads.Add(1)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		fail("GET %s: no node served it within 20s", key)
	}
	// put writes one object, retrying a non-leader's 503 across nodes for up to
	// 30s. Any other non-200 is a recorded failure.
	put := func(key string, body []byte) bool {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			for _, addr := range alive() {
				resp, respBody := c.do(addr, "PUT", "/vault/"+key, body)
				if resp == nil || resp.StatusCode == http.StatusServiceUnavailable {
					continue
				}
				if resp.StatusCode != http.StatusOK {
					fail("PUT %s: status %d\n%s", key, resp.StatusCode, respBody)
					return false
				}
				writes.Add(1)
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		fail("PUT %s: no node accepted it within 30s", key)
		return false
	}

	go func() {
		lrng := rand.New(rand.NewPCG(7, 7))
		live := append([]object(nil), seeded...)
		n := 0
		for {
			select {
			case <-stop:
				liveCh <- live
				return
			default:
			}
			for i := 0; i < 5; i++ {
				o := live[lrng.IntN(len(live))]
				get(o.key, o.body)
			}
			key := fmt.Sprintf("load-%d", n)
			body := randBytes(lrng, 4<<10+lrng.IntN(512<<10))
			if put(key, body) {
				live = append(live, object{key, body})
			}
			n++
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// The operation under load: drain n4 (4→3 active, still 2+1, so no re-encode),
	// then decommission it once its shards have migrated to the other three.
	run(t, "cluster", "drain", "-data-dir", dirs["n1"], "-node", "n4")

	// Removal is refused until the migration converges (the transition is open).
	// Poll until it lands — the convergence signal under live traffic.
	removed := false
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := tryRun(t, "cluster", "remove", "-data-dir", dirs["n1"], "-node", "n4"); err == nil {
			removed = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !removed {
		close(stop)
		<-liveCh
		t.Fatal("remove never succeeded — the drain migration did not converge")
	}
	procMu.Lock()
	procs["n4"] = nil // the removed node self-stops; stop routing S3 to it
	procMu.Unlock()

	// Let the load run a little past convergence, then stop and join it.
	time.Sleep(2 * time.Second)
	close(stop)
	written := <-liveCh

	if r, w := reads.Load(), writes.Load(); r == 0 || w == 0 {
		t.Fatalf("background load did not run: %d reads, %d writes", r, w)
	} else {
		t.Logf("background load: %d reads, %d writes, no errors", r, w)
	}
	errMu.Lock()
	if len(loadErrs) > 0 {
		t.Fatalf("background load hit %d errors during the drain:\n%s",
			len(loadErrs), strings.Join(loadErrs, "\n"))
	}
	errMu.Unlock()

	// Three members remain, and the data still reads — seeded objects migrated off
	// n4, and a sample of what the load wrote during the transition — now off the
	// three survivors.
	waitStatus(t, dirs["n1"], "three members after remove", func(rows []statusRow) bool {
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
	for _, o := range seeded {
		c.getEventually(alive(), "/vault/"+o.key, o.body)
	}
	sample := written
	if len(sample) > 12 {
		sample = sample[len(sample)-12:] // the most recent writes, into the transition
	}
	for _, o := range sample {
		c.getEventually(alive(), "/vault/"+o.key, o.body)
	}
}
