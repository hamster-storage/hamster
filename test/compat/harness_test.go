//go:build compat

package compat

import (
	"bytes"
	cryptorand "crypto/rand"
	"encoding/binary"
	"io/fs"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hamster-storage/hamster/internal/blob"
	"github.com/hamster-storage/hamster/internal/gateway"
	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/sys"
)

const (
	accessKey = "compat-access-key"
	secretKey = "compat-secret-key"
	region    = "us-east-1"
)

// server is one in-process Hamster S3 endpoint: the real gateway over a real
// disk in a temp directory — the same composition `hamster serve` builds,
// minus the listener flag plumbing.
type server struct {
	URL  string // http://127.0.0.1:port
	Host string // 127.0.0.1:port
}

func startServer(t *testing.T) *server {
	t.Helper()
	disk, err := sys.NewDisk(t.TempDir())
	if err != nil {
		t.Fatalf("disk: %v", err)
	}
	loop := sys.NewLoop()
	var seed [16]byte
	if _, err := cryptorand.Read(seed[:]); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rng := rand.New(rand.NewPCG(
		binary.LittleEndian.Uint64(seed[0:8]), binary.LittleEndian.Uint64(seed[8:16])))

	ts := httptest.NewServer(gateway.New(gateway.Config{
		Region: region,
		Lookup: func(akid string) (string, bool) {
			if akid == accessKey {
				return secretKey, true
			}
			return "", false
		},
		Store: meta.NewStore(),
		Loop:  loop,
		Clock: sys.Clock{},
		Rand:  rng,
		Blobs: blob.NewStore(disk),
	}))
	// Shutdown order per the gateway contract: HTTP first, loop second.
	t.Cleanup(func() { ts.Close(); loop.Stop() })
	return &server{URL: ts.URL, Host: strings.TrimPrefix(ts.URL, "http://")}
}

// needTool returns the path to a third-party binary, skipping the test when
// it is not installed.
func needTool(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s is not installed; skipping its compatibility tests", name)
	}
	return path
}

// cleanEnv is the parent environment minus any ambient AWS, rclone, or
// restic configuration, so the user's real credentials and config can never
// leak into a test run — or be touched by one.
func cleanEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "AWS_") ||
			strings.HasPrefix(kv, "RCLONE_") ||
			strings.HasPrefix(kv, "RESTIC_") {
			continue
		}
		env = append(env, kv)
	}
	return env
}

// runTool runs one CLI invocation and fails the test, with the tool's full
// output, on a non-zero exit.
func runTool(t *testing.T, env []string, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", filepath.Base(name), strings.Join(args, " "), err, out)
	}
	return string(out)
}

// writeRandomFile writes n bytes of seeded pseudo-random data at path,
// creating parent directories, and returns the data.
func writeRandomFile(t *testing.T, path string, seed uint64, n int) []byte {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 0))
	data := make([]byte, n)
	i := 0
	for ; i+8 <= n; i += 8 {
		binary.LittleEndian.PutUint64(data[i:], rng.Uint64())
	}
	for ; i < n; i++ {
		data[i] = byte(rng.Uint32())
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return data
}

// mustEqualFile fails unless the file at path holds exactly want.
func mustEqualFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: %d bytes, want %d — content differs from the original", path, len(got), len(want))
	}
}

// findFile walks root for a file named name and returns its path; tools like
// restic restore under the snapshot's absolute source path, so the exact
// location below the target is theirs to choose.
func findFile(t *testing.T, root, name string) string {
	t.Helper()
	var found string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == name {
			found = p
		}
		return err
	})
	if err != nil || found == "" {
		t.Fatalf("no file named %q under %s (walk err: %v)", name, root, err)
	}
	return found
}
