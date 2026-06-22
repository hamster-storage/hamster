//go:build e2e

// Package e2e is the layer-2 suite from docs/SIMULATION.md: the real
// hamster binary as plain child processes on localhost — real CLI, real
// data directories, real loopback mTLS, real signals. Distributed logic
// is the simulator's job; this layer proves the plumbing an operator
// actually touches. The upgrade suite (last release vs this commit)
// arrives with the feature-gate machinery (v0.8).
package e2e

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// binary builds cmd/hamster once per test run and returns its path.
var binary = sync.OnceValues(func() (string, error) {
	dir, err := os.MkdirTemp("", "hamster-e2e-*")
	if err != nil {
		return "", err
	}
	bin := filepath.Join(dir, "hamster")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/hamster-storage/hamster/cmd/hamster")
	cmd.Dir = "../.." // the module root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("building hamster: %v\n%s", err, out)
	}
	return bin, nil
})

func bin(t *testing.T) string {
	t.Helper()
	b, err := binary()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// run executes one short-lived hamster command and returns its combined
// output, failing the test on a nonzero exit.
func run(t *testing.T, args ...string) string {
	t.Helper()
	return runBin(t, bin(t), args...)
}

// runBin is run with an explicit binary path — the upgrade suite runs commands
// against a specific binary version (ADR-0034).
func runBin(t *testing.T, binPath string, args ...string) string {
	t.Helper()
	out, err := exec.Command(binPath, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("hamster %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// safeBuf collects a child process's output without racing its writes.
type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// proc is one long-running hamster child process.
type proc struct {
	cmd *exec.Cmd
	out *safeBuf
}

// start launches a long-running hamster command (cluster run, serve).
func start(t *testing.T, env []string, args ...string) *proc {
	t.Helper()
	return startBin(t, bin(t), env, args...)
}

// startBin is start with an explicit binary path — the upgrade suite starts a
// node on a chosen binary version (ADR-0034).
func startBin(t *testing.T, binPath string, env []string, args ...string) *proc {
	t.Helper()
	p := &proc{cmd: exec.Command(binPath, args...), out: &safeBuf{}}
	p.cmd.Stdout = p.out
	p.cmd.Stderr = p.out
	p.cmd.Env = append(os.Environ(), env...)
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("starting hamster %s: %v", strings.Join(args, " "), err)
	}
	t.Cleanup(func() {
		if p.cmd.ProcessState == nil {
			p.cmd.Process.Kill()
			p.cmd.Wait()
		}
	})
	return p
}

// interrupt sends SIGINT — what Ctrl-C sends — and waits for a clean exit.
func (p *proc) interrupt(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("process exited %v after SIGINT\n%s", err, p.out.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("process ignored SIGINT for 15s\n%s", p.out.String())
	}
}

// freeAddr reserves a loopback port and releases it for a node to bind.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// statusRow is one parsed line of `cluster status` output.
type statusRow struct {
	node   string
	role   string // "voter", "learner"
	leader bool
	down   bool // the answering node's STATE column for this member
}

func parseStatus(out string) []statusRow {
	var rows []statusRow
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		// A member row begins with a numeric RAFT-ID; this skips the header
		// and the failure-domain topology summary lines (ADR-0016).
		if _, err := strconv.Atoi(fields[0]); err != nil {
			continue
		}
		rows = append(rows, statusRow{
			node:   fields[1],
			role:   fields[3],
			leader: strings.Contains(line, "(leader)"),
			// STATE is the last column; "down" appears only there.
			down: slices.Contains(fields, "down"),
		})
	}
	return rows
}

// waitStatus polls `cluster status` against dataDir's node until pred
// holds on the parsed rows.
func waitStatus(t *testing.T, dataDir, what string, pred func([]statusRow) bool) []statusRow {
	t.Helper()
	return waitStatusBin(t, bin(t), dataDir, what, pred)
}

// waitStatusBin is waitStatus with an explicit binary path.
func waitStatusBin(t *testing.T, binPath, dataDir, what string, pred func([]statusRow) bool) []statusRow {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		out, err := exec.Command(binPath, "cluster", "status", "-data-dir", dataDir).CombinedOutput()
		last = string(out)
		if err == nil {
			if rows := parseStatus(last); pred(rows) {
				return rows
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("waiting for %s; last status:\n%s", what, last)
	return nil
}

func voterCount(rows []statusRow) int {
	n := 0
	for _, r := range rows {
		if r.role == "voter" {
			n++
		}
	}
	return n
}

func leaderOf(rows []statusRow) string {
	for _, r := range rows {
		if r.leader {
			return r.node
		}
	}
	return ""
}

// TestClusterLifecycle is the operator's whole v0.2 journey against the
// real binary: init, run, token joins (the one-command form), status from
// several nodes, leader failover on Ctrl-C, restart from disk, clean
// shutdown.
func TestClusterLifecycle(t *testing.T) {
	root := t.TempDir()
	dirs := map[string]string{}
	procs := map[string]*proc{}

	// n1: init and run.
	dirs["n1"] = filepath.Join(root, "n1")
	run(t, "cluster", "init", "-data-dir", dirs["n1"], "-cluster", "e2e", "-node", "n1",
		"-listen", freeAddr(t))
	// This is a membership/failover journey, not an S3 one — the nodes run
	// headless (-no-s3), so they need no credentials.
	procs["n1"] = start(t, nil, "cluster", "run", "-data-dir", dirs["n1"], "-no-s3")
	waitStatus(t, dirs["n1"], "n1 leading alone", func(rows []statusRow) bool {
		return len(rows) == 1 && rows[0].leader
	})

	// n2 and n3: token + the one-command join-and-run form.
	for _, id := range []string{"n2", "n3"} {
		token := strings.TrimSpace(run(t, "cluster", "token", "-data-dir", dirs["n1"]))
		dirs[id] = filepath.Join(root, id)
		procs[id] = start(t, nil, "cluster", "run", "-data-dir", dirs[id], "-node", id,
			"-listen", freeAddr(t), "-token", token, "-no-s3")
	}
	waitStatus(t, dirs["n1"], "three voters", func(rows []statusRow) bool {
		return len(rows) == 3 && voterCount(rows) == 3
	})

	// Status answers from a non-founder too, and agrees.
	waitStatus(t, dirs["n3"], "status from n3", func(rows []statusRow) bool {
		return len(rows) == 3 && voterCount(rows) == 3
	})

	// Failover: Ctrl-C the leader; the survivors elect a new one.
	rows := waitStatus(t, dirs["n2"], "a leader to fail over", func(rows []statusRow) bool {
		return leaderOf(rows) != ""
	})
	lead := leaderOf(rows)
	procs[lead].interrupt(t)
	var survivor string
	for _, id := range []string{"n1", "n2", "n3"} {
		if id != lead {
			survivor = id
			break
		}
	}
	waitStatus(t, dirs[survivor], "a new leader after failover", func(rows []statusRow) bool {
		l := leaderOf(rows)
		return l != "" && l != lead
	})

	// The dead node restarts from its own disk and rejoins.
	procs[lead] = start(t, nil, "cluster", "run", "-data-dir", dirs[lead], "-no-s3")
	waitStatus(t, dirs[lead], "the restarted node back among three voters", func(rows []statusRow) bool {
		return len(rows) == 3 && voterCount(rows) == 3
	})
	if !strings.Contains(procs[lead].out.String(), "cluster: membership:") {
		t.Fatalf("restarted node never logged its membership roster:\n%s", procs[lead].out.String())
	}

	// Everyone shuts down cleanly on Ctrl-C.
	for _, id := range []string{"n1", "n2", "n3"} {
		procs[id].interrupt(t)
		if !strings.Contains(procs[id].out.String(), "shutting down") {
			t.Fatalf("%s exited without its shutdown line:\n%s", id, procs[id].out.String())
		}
	}
}

// TestServeSmoke: the S3 endpoint as a real process — serves the S3 error
// envelope to an unsigned request, names its version, exits on Ctrl-C.
func TestServeSmoke(t *testing.T) {
	if v := run(t, "version"); !strings.HasPrefix(v, "hamster ") {
		t.Fatalf("version output %q", v)
	}

	addr := freeAddr(t)
	p := start(t,
		[]string{"HAMSTER_ACCESS_KEY_ID=e2e", "HAMSTER_SECRET_ACCESS_KEY=e2e-secret"},
		"serve", "-data-dir", t.TempDir(), "-listen", addr)

	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), "<Error>") {
				t.Fatalf("unsigned request: status %d, body %s", resp.StatusCode, body)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve never came up: %v\n%s", err, p.out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	p.interrupt(t)
}
