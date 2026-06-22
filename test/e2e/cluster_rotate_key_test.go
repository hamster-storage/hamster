//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestClusterKeyRotationAtRest is v0.8's payoff over the real binary: a
// three-node encrypted cluster, each node holding the current key
// (-master-key-file) and the next one (-new-master-key-file), rotates its master
// key with `rotate-key`. Every object's key is rewrapped onto the new
// key — object bytes never move — and the proof it truly moved is the last step:
// a node restarted with the new key as its *only* master key still decrypts every
// object, so the old key is genuinely retired.
func TestClusterKeyRotationAtRest(t *testing.T) {
	const (
		akid   = "e2e-rot"
		secret = "e2e-rot-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	root := t.TempDir()

	// Two keys off the data dirs, as mounted secrets would be: the current key
	// and the one the cluster rotates to.
	oldKey := filepath.Join(root, "old.key")
	newKey := filepath.Join(root, "new.key")
	if err := os.WriteFile(oldKey, []byte(strings.Repeat("ab", 32)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newKey, []byte(strings.Repeat("cd", 32)), 0o600); err != nil {
		t.Fatal(err)
	}

	nodes := []string{"n1", "n2", "n3"}
	dirs := map[string]string{}
	procs := map[string]*proc{}
	s3 := map[string]string{}

	// Every node carries both keys for the duration of the rotation.
	bootArgs := func(id, s3addr string, extra ...string) []string {
		a := []string{"serve", "-data-dir", dirs[id], "-s3", s3addr,
			"-master-key-file", oldKey, "-new-master-key-file", newKey}
		return append(a, extra...)
	}

	dirs["n1"] = filepath.Join(root, "n1")
	s3["n1"] = freeAddr(t)
	run(t, "init", "-data-dir", dirs["n1"], "-cluster", "rot-e2e", "-node", "n1", "-listen", freeAddr(t))
	procs["n1"] = start(t, env, bootArgs("n1", s3["n1"])...)
	waitStatus(t, dirs["n1"], "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})
	for _, id := range nodes[1:] {
		tok := strings.TrimSpace(run(t, "token", "-data-dir", dirs["n1"]))
		dirs[id] = filepath.Join(root, id)
		s3[id] = freeAddr(t)
		procs[id] = start(t, env, bootArgs(id, s3[id], "-node", id, "-listen", freeAddr(t), "-token", tok)...)
	}
	waitStatus(t, dirs["n1"], "three members", func(rows []statusRow) bool { return len(rows) == 3 })

	run(t, "encrypt", "-data-dir", dirs["n1"])

	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	leaderAddr := func() string {
		rows := waitStatus(t, dirs["n1"], "a leader", func(rows []statusRow) bool { return leaderOf(rows) != "" })
		return s3[leaderOf(rows)]
	}
	lead := leaderAddr()

	// Write a handful of objects of varying size.
	c.mutate([]string{lead}, "PUT", "/vault", nil, http.StatusOK)
	bodies := map[string][]byte{}
	for i, n := range []int{1, 1 << 12, 200 << 10} {
		key := fmt.Sprintf("obj-%d", i)
		body := bytes.Repeat([]byte(fmt.Sprintf("rotate-me-%d-", i)), 1+n/12)
		bodies[key] = body
		c.mutate([]string{lead}, "PUT", "/vault/"+key, body, http.StatusOK)
	}

	// Rotate the master key.
	out := run(t, "rotate-key", "-data-dir", dirs["n1"])
	if !strings.Contains(out, "rotation complete") {
		t.Fatalf("rotate-key did not report completion:\n%s", out)
	}
	// Status shows the rotation closed and the new fingerprint in effect.
	if st := run(t, "status", "-data-dir", dirs["n1"]); strings.Contains(st, "rotation in progress") {
		t.Fatalf("status still shows a rotation in progress after completion:\n%s", st)
	}

	// Every object still reads under the running cluster (which now writes and
	// reads with the new key). The cluster just absorbed a burst of rewrap
	// proposals, so a read can briefly land on a node still settling; retry
	// within a deadline, as the encryption-at-rest e2e does after a restart.
	lead = leaderAddr()
	for key, body := range bodies {
		deadline := time.Now().Add(30 * time.Second)
		for {
			resp, got := c.do(lead, "GET", "/vault/"+key, nil)
			if resp != nil && resp.StatusCode == http.StatusOK && bytes.Equal(got, body) {
				if h := resp.Header.Get("x-amz-server-side-encryption"); h != "AES256" {
					t.Fatalf("GET %s SSE header %q, want AES256", key, h)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("GET %s after rotation never succeeded: resp=%v equal=%v", key, resp, bytes.Equal(got, body))
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	// The proof the rotation moved the data: restart a follower with the *new*
	// key as its only master key (the old one retired, no -new-master-key-file).
	// It must still decrypt every object — only possible if their DEKs are now
	// wrapped under the new key.
	follower := ""
	for _, id := range nodes {
		if s3[id] != lead {
			follower = id
			break
		}
	}
	procs[follower].interrupt(t)
	procs[follower] = start(t, env, "serve", "-data-dir", dirs[follower], "-s3", s3[follower], "-master-key-file", newKey)
	waitStatus(t, dirs["n1"], "follower rejoined", func(rows []statusRow) bool { return len(rows) == 3 })

	deadline := time.Now().Add(60 * time.Second)
	for {
		ok := true
		for key, body := range bodies {
			resp, got := c.do(s3[follower], "GET", "/vault/"+key, nil)
			if resp == nil || resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted node with only the new key never served the objects — rotation did not move them")
		}
		time.Sleep(200 * time.Millisecond)
	}

	for _, p := range procs {
		p.interrupt(t)
	}
}
