//go:build e2e

package e2e

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestClusterEncryptionAtRest is v0.7's payoff over the real binary: a
// three-node cluster, each given the same master key with -master-key-file,
// turns on encryption with `cluster encrypt`. New writes are encrypted, the
// SSE header is reported on reads, `cluster status` shows the posture, and a
// node that restarts reloads its key from its source and still decrypts.
func TestClusterEncryptionAtRest(t *testing.T) {
	const (
		akid   = "e2e-enc"
		secret = "e2e-enc-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	root := t.TempDir()

	// The master key lives in a file off the data directories — as a mounted
	// secret would. Hex-encoded 32 bytes.
	keyFile := filepath.Join(root, "master.key")
	if err := os.WriteFile(keyFile, []byte(strings.Repeat("ab", 32)), 0o600); err != nil {
		t.Fatal(err)
	}

	nodes := []string{"n1", "n2", "n3"}
	dirs := map[string]string{}
	procs := map[string]*proc{}
	s3 := map[string]string{}

	dirs["n1"] = filepath.Join(root, "n1")
	s3["n1"] = freeAddr(t)
	run(t, "cluster", "init", "-data-dir", dirs["n1"], "-cluster", "enc-e2e", "-node", "n1", "-listen", freeAddr(t))
	procs["n1"] = start(t, env, "cluster", "run", "-data-dir", dirs["n1"], "-s3", s3["n1"], "-master-key-file", keyFile)
	waitStatus(t, dirs["n1"], "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})
	for _, id := range nodes[1:] {
		tok := strings.TrimSpace(run(t, "cluster", "token", "-data-dir", dirs["n1"]))
		dirs[id] = filepath.Join(root, id)
		s3[id] = freeAddr(t)
		procs[id] = start(t, env, "cluster", "run", "-data-dir", dirs[id], "-node", id,
			"-listen", freeAddr(t), "-token", tok, "-s3", s3[id], "-master-key-file", keyFile)
	}
	waitStatus(t, dirs["n1"], "three members", func(rows []statusRow) bool { return len(rows) == 3 })

	// Off until enabled.
	if out := run(t, "cluster", "status", "-data-dir", dirs["n1"]); !strings.Contains(out, "encryption at rest: off") {
		t.Fatalf("status should show encryption off before enabling:\n%s", out)
	}
	// Enable, and confirm status reports it.
	run(t, "cluster", "encrypt", "-data-dir", dirs["n1"])
	if out := run(t, "cluster", "status", "-data-dir", dirs["n1"]); !strings.Contains(out, "encryption at rest: AES256GCM") {
		t.Fatalf("status missing encryption posture after enabling:\n%s", out)
	}

	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	leaderAddr := func() string {
		rows := waitStatus(t, dirs["n1"], "a leader", func(rows []statusRow) bool { return leaderOf(rows) != "" })
		return s3[leaderOf(rows)]
	}
	lead := leaderAddr()
	c.mutate([]string{lead}, "PUT", "/vault", nil, http.StatusOK)
	body := bytes.Repeat([]byte("encrypted-at-rest-over-the-wire-"), 4000) // ~128 KiB, multi-stripe
	c.mutate([]string{lead}, "PUT", "/vault/secret", body, http.StatusOK)

	// Read from the leader: bytes match and the SSE header is reported.
	resp, got := c.do(lead, "GET", "/vault/secret", nil)
	if resp == nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("GET from leader: resp=%v equal=%v", resp, bytes.Equal(got, body))
	}
	if h := resp.Header.Get("x-amz-server-side-encryption"); h != "AES256" {
		t.Fatalf("GET SSE header %q, want AES256", h)
	}

	// Restart a follower: it must reload its master key from its source and
	// serve the decrypted object from its own replica.
	follower := ""
	for _, id := range nodes {
		if s3[id] != lead {
			follower = id
			break
		}
	}
	procs[follower].interrupt(t)
	procs[follower] = start(t, env, "cluster", "run", "-data-dir", dirs[follower], "-s3", s3[follower], "-master-key-file", keyFile)
	waitStatus(t, dirs["n1"], "follower rejoined", func(rows []statusRow) bool { return len(rows) == 3 })

	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, got := c.do(s3[follower], "GET", "/vault/secret", nil)
		if resp != nil && resp.StatusCode == http.StatusOK && bytes.Equal(got, body) {
			if h := resp.Header.Get("x-amz-server-side-encryption"); h != "AES256" {
				t.Fatalf("restarted node GET SSE header %q, want AES256", h)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted follower never served the decrypted object (KEK reload?)")
		}
		time.Sleep(200 * time.Millisecond)
	}

	for _, p := range procs {
		p.interrupt(t)
	}
}
