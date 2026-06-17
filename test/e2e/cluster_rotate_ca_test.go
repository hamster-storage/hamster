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

// TestClusterCARotation is v0.8 Track B's payoff over the real binary: a
// three-node cluster rotates its CA with `cluster rotate-ca` — a dual-trust
// rollover (ADR-0033). Every node is reissued onto the new CA and the old CA is
// dropped, with no downtime. The proof it truly took: after the rotation a node
// is restarted, and since its on-disk material is now the new CA alone, it
// rejoins and serves — only possible if the rollover reached it. Encryption is
// on too, to exercise the data path across the rotation.
func TestClusterCARotation(t *testing.T) {
	const (
		akid   = "e2e-ca"
		secret = "e2e-ca-secret"
		region = "us-east-1"
	)
	env := []string{"HAMSTER_ACCESS_KEY_ID=" + akid, "HAMSTER_SECRET_ACCESS_KEY=" + secret}
	root := t.TempDir()
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
	run(t, "cluster", "init", "-data-dir", dirs["n1"], "-cluster", "ca-e2e", "-node", "n1", "-listen", freeAddr(t))
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
	run(t, "cluster", "encrypt", "-data-dir", dirs["n1"])

	c := &s3Client{t: t, akid: akid, secret: secret, region: region}
	leaderAddr := func() string {
		rows := waitStatus(t, dirs["n1"], "a leader", func(rows []statusRow) bool { return leaderOf(rows) != "" })
		return s3[leaderOf(rows)]
	}
	lead := leaderAddr()
	c.mutate([]string{lead}, "PUT", "/vault", nil, http.StatusOK)
	body := bytes.Repeat([]byte("survives-a-ca-rotation-"), 6000)
	c.mutate([]string{lead}, "PUT", "/vault/obj", body, http.StatusOK)

	// Rotate the CA.
	out := run(t, "cluster", "rotate-ca", "-data-dir", dirs["n1"])
	if !strings.Contains(out, "rotation complete") {
		t.Fatalf("rotate-ca did not report completion:\n%s", out)
	}
	// The rotation closed: status no longer reports one in progress.
	if st := run(t, "cluster", "status", "-data-dir", dirs["n1"]); strings.Contains(st, "rotation in progress") {
		t.Fatalf("status still shows a CA rotation in progress:\n%s", st)
	}

	// The cluster keeps serving after the rotation (peers trust the new CA).
	lead = leaderAddr()
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, got := c.do(lead, "GET", "/vault/obj", nil)
		if resp != nil && resp.StatusCode == http.StatusOK && bytes.Equal(got, body) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("object not readable after CA rotation")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The proof the rollover reached every node: restart a follower. Its on-disk
	// ca.pem is now the new CA alone and its node cert is the new-CA leaf, so it
	// can only rejoin and serve if the rotation truly reissued it.
	follower := ""
	for _, id := range nodes {
		if s3[id] != lead {
			follower = id
			break
		}
	}
	procs[follower].interrupt(t)
	procs[follower] = start(t, env, "cluster", "run", "-data-dir", dirs[follower], "-s3", s3[follower], "-master-key-file", keyFile)
	waitStatus(t, dirs["n1"], "follower rejoined on the new CA", func(rows []statusRow) bool { return len(rows) == 3 })

	deadline = time.Now().Add(60 * time.Second)
	for {
		resp, got := c.do(s3[follower], "GET", "/vault/obj", nil)
		if resp != nil && resp.StatusCode == http.StatusOK && bytes.Equal(got, body) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted node on the new CA never served the object — rotation did not reach it")
		}
		time.Sleep(200 * time.Millisecond)
	}

	for _, p := range procs {
		p.interrupt(t)
	}
}
