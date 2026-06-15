//go:build e2e

package e2e

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestClusterS3 is pass 5's payoff: a six-node cluster of real processes
// over loopback mTLS, every node serving the S3 API, objects erasure
// coded 4+2 across all six. Then a node dies mid-workload and S3 clients
// keep working: reads reconstruct, writes ack at the floor. Two nodes
// dead and writes refuse honestly while reads still serve — the
// documented ack rule, observed through real sockets.
func TestClusterS3(t *testing.T) {
	const (
		akid   = "e2e-cluster"
		secret = "e2e-cluster-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	root := t.TempDir()
	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6"}
	dirs := map[string]string{}
	procs := map[string]*proc{}
	s3Addrs := map[string]string{}

	dirs["n1"] = filepath.Join(root, "n1")
	s3Addrs["n1"] = freeAddr(t)
	run(t, "cluster", "init", "-data-dir", dirs["n1"], "-cluster", "e2e-s3", "-node", "n1",
		"-listen", freeAddr(t))
	procs["n1"] = start(t, env, "cluster", "run", "-data-dir", dirs["n1"], "-s3", s3Addrs["n1"])
	waitStatus(t, dirs["n1"], "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})

	for _, id := range nodes[1:] {
		token := strings.TrimSpace(run(t, "cluster", "token", "-data-dir", dirs["n1"]))
		dirs[id] = filepath.Join(root, id)
		s3Addrs[id] = freeAddr(t)
		procs[id] = start(t, env, "cluster", "run", "-data-dir", dirs[id], "-node", id,
			"-listen", freeAddr(t), "-token", token,
			"-s3", s3Addrs[id])
	}
	// Five voters (the cap) plus one learner.
	waitStatus(t, dirs["n1"], "six members, five voters", func(rows []statusRow) bool {
		return len(rows) == 6 && voterCount(rows) == 5
	})

	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	alive := func() []string {
		var out []string
		for _, id := range nodes {
			if procs[id] != nil {
				out = append(out, s3Addrs[id])
			}
		}
		return out
	}

	// Create the bucket and store objects across the healthy cluster.
	c.mutate(alive(), "PUT", "/vault", nil, http.StatusOK)
	rng := rand.New(rand.NewPCG(99, 0))
	bodies := map[string][]byte{
		"small":  randBytes(rng, 10<<10),   // 1+2: full copies
		"medium": randBytes(rng, 600<<10),  // 4+2, one stripe and change
		"large":  randBytes(rng, 3<<20+17), // multi-stripe
	}
	for key, body := range bodies {
		c.mutate(alive(), "PUT", "/vault/"+key, body, http.StatusOK)
	}
	for key, body := range bodies {
		c.getEventually(alive(), "/vault/"+key, body)
	}

	// A node dies mid-workload. Kill a non-leader so the metadata plane
	// keeps its head; the data plane loses a holder of every object.
	rows := waitStatus(t, dirs["n1"], "a settled leader", func(rows []statusRow) bool {
		return leaderOf(rows) != ""
	})
	victim := ""
	for _, id := range []string{"n6", "n5", "n4", "n3", "n2"} {
		if id != leaderOf(rows) {
			victim = id
			break
		}
	}
	procs[victim].interrupt(t)
	procs[victim] = nil
	t.Logf("killed %s; cluster degraded", victim)

	// Reads reconstruct around the loss.
	for key, body := range bodies {
		c.getEventually(alive(), "/vault/"+key, body)
	}
	// Writes still ack — five durable shards, the k+1 floor holds.
	// (Discovering the dead node costs the shard-write timeout; the
	// generous deadline is that, not slack.)
	degraded := randBytes(rng, 1<<20)
	c.mutate(alive(), "PUT", "/vault/degraded", degraded, http.StatusOK)
	c.getEventually(alive(), "/vault/degraded", degraded)

	// The write went to the leader (leader-only in v0.3), whose PUT paid the
	// dead holder's timeout and recorded it down — so the leader's own
	// `cluster status` now surfaces the victim as down (ADR-0027 liveness).
	leadDir := dirs[leaderOf(rows)]
	waitStatus(t, leadDir, "the leader's status to show "+victim+" down", func(rows []statusRow) bool {
		for _, r := range rows {
			if r.node == victim {
				return r.down
			}
		}
		return false
	})

	// A second death puts the cluster below the floor: writes refuse
	// with SlowDown, reads keep serving at exactly k.
	rows = waitStatus(t, dirs["n1"], "a leader after the loss", func(rows []statusRow) bool {
		return leaderOf(rows) != ""
	})
	victim2 := ""
	for _, id := range []string{"n5", "n4", "n3", "n2", "n6"} {
		if id != leaderOf(rows) && procs[id] != nil {
			victim2 = id
			break
		}
	}
	procs[victim2].interrupt(t)
	procs[victim2] = nil
	t.Logf("killed %s; below the write floor", victim2)

	c.mutate(alive(), "PUT", "/vault/refused", randBytes(rng, 256<<10), http.StatusServiceUnavailable)
	for key, body := range bodies {
		c.getEventually(alive(), "/vault/"+key, body)
	}
}

func randBytes(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.UintN(256))
	}
	return b
}

// s3Client is a minimal, independently written SigV4 signer — enough for
// header-signed PUT/GET/DELETE with a computed payload hash.
type s3Client struct {
	t      *testing.T
	akid   string
	secret string
	region string
}

func (c *s3Client) sign(req *http.Request, payload []byte) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateScope := now.Format("20060102")
	payloadHash := sha256.Sum256(payload)
	hexPayload := hex.EncodeToString(payloadHash[:])

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", hexPayload)

	canonicalHeaders := "host:" + req.Host + "\n" +
		"x-amz-content-sha256:" + hexPayload + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonical := strings.Join([]string{
		req.Method, req.URL.EscapedPath(), req.URL.RawQuery,
		canonicalHeaders, signedHeaders, hexPayload,
	}, "\n")
	scope := dateScope + "/" + c.region + "/s3/aws4_request"
	canonicalHash := sha256.Sum256([]byte(canonical))
	toSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, hex.EncodeToString(canonicalHash[:]),
	}, "\n")

	mac := func(key, data []byte) []byte {
		h := hmac.New(sha256.New, key)
		h.Write(data)
		return h.Sum(nil)
	}
	kDate := mac([]byte("AWS4"+c.secret), []byte(dateScope))
	kRegion := mac(kDate, []byte(c.region))
	kService := mac(kRegion, []byte("s3"))
	kSigning := mac(kService, []byte("aws4_request"))
	sig := hex.EncodeToString(mac(kSigning, []byte(toSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.akid, scope, signedHeaders, sig))
}

// do sends one signed request to one node.
func (c *s3Client) do(addr, method, path string, body []byte) (*http.Response, []byte) {
	c.t.Helper()
	req, err := http.NewRequest(method, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	c.sign(req, body)
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, respBody
}

// mutate drives one write to whichever node will take it. Only the Raft
// leader commits in v0.3 (no proposal forwarding), so non-leaders answer
// 503 and the client moves on — when want is 503 itself, every node must
// refuse.
func (c *s3Client) mutate(addrs []string, method, path string, body []byte, want int) {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		refused := 0
		for _, addr := range addrs {
			resp, respBody := c.do(addr, method, path, body)
			if resp == nil {
				continue // node coming up or gone; try the next
			}
			if resp.StatusCode == want && want != http.StatusServiceUnavailable {
				return
			}
			if resp.StatusCode == http.StatusServiceUnavailable {
				refused++
				continue
			}
			c.t.Fatalf("%s %s on %s: status %d, want %d\n%s", method, path, addr, resp.StatusCode, want, respBody)
		}
		if want == http.StatusServiceUnavailable && refused == len(addrs) && refused > 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatalf("%s %s: no node answered %d before the deadline", method, path, want)
}

// getEventually reads the object from any node, retrying while followers
// catch up, and demands bit-identical bytes.
func (c *s3Client) getEventually(addrs []string, path string, want []byte) {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		for _, addr := range addrs {
			resp, body := c.do(addr, "GET", path, nil)
			if resp == nil || resp.StatusCode != http.StatusOK {
				continue
			}
			if !bytes.Equal(body, want) {
				c.t.Fatalf("GET %s on %s: %d bytes, want %d — corruption, not lag", path, addr, len(body), len(want))
			}
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.t.Fatalf("GET %s: no node served the object before the deadline", path)
}

// tryRun runs the binary and returns its output and error instead of failing —
// for a command that is expected to be refused until the cluster is ready.
func tryRun(t *testing.T, args ...string) (string, error) {
	t.Helper()
	out, err := exec.Command(bin(t), args...).CombinedOutput()
	return string(out), err
}

// TestClusterDownsize: a six-node 4+2 cluster shrinks to five. Draining a node
// crosses a profile boundary, so the cluster re-encodes every object 4+2 → 3+2
// in place (ADR-0031); once it converges the drained node is removable, and the
// objects still read — at the new profile, off the five remaining nodes. Proves
// the whole composition end to end over real processes: the drain CLI's
// downsize path, the leader's re-encoding sweep, convergence, and removal.
func TestClusterDownsize(t *testing.T) {
	const (
		akid   = "e2e-down"
		secret = "e2e-down-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	root := t.TempDir()
	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6"}
	dirs := map[string]string{}
	procs := map[string]*proc{}
	s3Addrs := map[string]string{}

	dirs["n1"] = filepath.Join(root, "n1")
	s3Addrs["n1"] = freeAddr(t)
	run(t, "cluster", "init", "-data-dir", dirs["n1"], "-cluster", "e2e-down", "-node", "n1", "-listen", freeAddr(t))
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
	waitStatus(t, dirs["n1"], "six members, five voters", func(rows []statusRow) bool {
		return len(rows) == 6 && voterCount(rows) == 5
	})

	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	alive := func() []string {
		var out []string
		for _, id := range nodes {
			if procs[id] != nil {
				out = append(out, s3Addrs[id])
			}
		}
		return out
	}

	c.mutate(alive(), "PUT", "/vault", nil, http.StatusOK)
	rng := rand.New(rand.NewPCG(7, 0))
	bodies := map[string][]byte{
		"a": randBytes(rng, 600<<10),
		"b": randBytes(rng, 700<<10),
		"c": randBytes(rng, 3<<20+11),
	}
	for key, body := range bodies {
		c.mutate(alive(), "PUT", "/vault/"+key, body, http.StatusOK)
	}
	for key, body := range bodies {
		c.getEventually(alive(), "/vault/"+key, body)
	}

	// Drain the learner (n6): 6→5 crosses 4+2→3+2, so the cluster re-encodes the
	// data down. -reencode takes the place of the interactive [y/N].
	run(t, "cluster", "drain", "-data-dir", dirs["n1"], "-node", "n6", "-reencode")

	// Removal is refused until the re-encode converges (the transition is open and
	// the data still does not fit five nodes). Poll until it lands.
	removed := false
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := tryRun(t, "cluster", "remove", "-data-dir", dirs["n1"], "-node", "n6"); err == nil {
			removed = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !removed {
		t.Fatal("remove never succeeded — the downsize re-encode did not converge")
	}
	procs["n6"] = nil // the removed node self-stops; stop routing S3 to it

	// Five members remain, and every object still reads — re-encoded onto them.
	waitStatus(t, dirs["n1"], "five members after the downsize", func(rows []statusRow) bool {
		if len(rows) != 5 {
			return false
		}
		for _, r := range rows {
			if r.node == "n6" {
				return false
			}
		}
		return true
	})
	for key, body := range bodies {
		c.getEventually(alive(), "/vault/"+key, body)
	}
}
