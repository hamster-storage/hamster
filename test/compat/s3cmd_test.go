//go:build compat

package compat

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// s3cmd configures through an INI file, not environment variables; setting
// host_bucket to the bare endpoint (no %(bucket)s template) is its idiom for
// path-style addressing.
func (s *server) s3cmdConfig(t *testing.T) string {
	t.Helper()
	cfg := filepath.Join(t.TempDir(), "s3cfg")
	content := fmt.Sprintf(`[default]
access_key = %s
secret_key = %s
host_base = %s
host_bucket = %s
use_https = False
signature_v2 = False
bucket_location = %s
`, accessKey, secretKey, s.Host, s.Host, region)
	if err := os.WriteFile(cfg, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestS3cmd(t *testing.T) {
	bin := needTool(t, "s3cmd")
	s := startServer(t)
	cfg := s.s3cmdConfig(t)
	env := cleanEnv()
	run := func(t *testing.T, args ...string) string {
		return runTool(t, env, bin, append([]string{"--config", cfg}, args...)...)
	}

	dir := t.TempDir()
	small := writeRandomFile(t, filepath.Join(dir, "small.bin"), 8, 32<<10)
	big := writeRandomFile(t, filepath.Join(dir, "big.bin"), 9, 12<<20)

	t.Run("upload", func(t *testing.T) {
		run(t, "mb", "s3://s3cmd-bkt")
		run(t, "put", filepath.Join(dir, "small.bin"), "s3://s3cmd-bkt/small.bin")
		// Forced down from the 15 MiB default so the 12 MiB file goes
		// through s3cmd's own multipart implementation.
		run(t, "--multipart-chunk-size-mb=5", "put", filepath.Join(dir, "big.bin"), "s3://s3cmd-bkt/big.bin")
	})

	t.Run("listing", func(t *testing.T) {
		out := run(t, "ls", "s3://s3cmd-bkt")
		for _, want := range []string{"small.bin", "big.bin"} {
			if !strings.Contains(out, want) {
				t.Fatalf("ls missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("download", func(t *testing.T) {
		// s3cmd verifies the downloaded MD5 against the ETag itself on
		// single-part objects; the byte comparison below covers both.
		run(t, "get", "s3://s3cmd-bkt/small.bin", filepath.Join(dir, "small.out"))
		run(t, "get", "s3://s3cmd-bkt/big.bin", filepath.Join(dir, "big.out"))
		mustEqualFile(t, filepath.Join(dir, "small.out"), small)
		mustEqualFile(t, filepath.Join(dir, "big.out"), big)
	})

	t.Run("delete", func(t *testing.T) {
		run(t, "del", "--recursive", "--force", "s3://s3cmd-bkt")
		run(t, "rb", "s3://s3cmd-bkt")
		if out := run(t, "ls"); strings.TrimSpace(out) != "" {
			t.Fatalf("buckets remain after rb:\n%s", out)
		}
	})
}
