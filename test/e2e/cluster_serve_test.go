//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestClusterServeByDefault proves the v0.11 "S3 on by default" behavior: a
// node serves the S3 API without being asked, credentials are required to do so,
// and -no-s3 runs a headless storage node that needs no credentials.
func TestClusterServeByDefault(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "n1")
	run(t, "init", "-data-dir", dir, "-cluster", "serve-default", "-node", "n1", "-listen", freeAddr(t))

	// Headless: -no-s3 boots without credentials and the node leads alone — it
	// still serves the cluster, just not S3.
	headless := start(t, nil, "serve", "-data-dir", dir, "-no-s3")
	waitStatus(t, dir, "n1 headless leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})
	headless.interrupt(t)

	// S3 is on by default, so a plain run with no credentials is refused: the node
	// must never come up serving an unauthenticated S3 endpoint.
	out, err := runNoCreds(t, "serve", "-data-dir", dir)
	if err == nil {
		t.Fatalf("plain run without credentials should fail; output:\n%s", out)
	}
	if !strings.Contains(out, "HAMSTER_ACCESS_KEY_ID") {
		t.Fatalf("expected a credentials error, got:\n%s", out)
	}

	// -no-s3 with an explicit -s3 is a contradiction and is refused.
	out, err = runNoCreds(t, "serve", "-data-dir", dir, "-no-s3", "-s3", "127.0.0.1:9999")
	if err == nil || !strings.Contains(out, "mutually exclusive") {
		t.Fatalf("expected -no-s3/-s3 conflict to be refused, got err=%v output:\n%s", err, out)
	}
}

// runNoCreds runs hamster with the parent environment stripped of any S3
// credentials, returning combined output and the exit error. A 30s context
// guards against a hang (a run that unexpectedly stays up).
func runNoCreds(t *testing.T, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin(t), args...)
	var env []string
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "HAMSTER_ACCESS_KEY_ID=") || strings.HasPrefix(e, "HAMSTER_SECRET_ACCESS_KEY=") {
			continue
		}
		env = append(env, e)
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}
