//go:build compat

package compat

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// rcloneEnv defines the remote entirely through environment variables, with
// RCLONE_CONFIG pointed at an empty temp path so the user's real remotes are
// invisible. Upload cutoff and chunk size are forced down from rclone's
// 200 MiB default so a 12 MiB file exercises rclone's own multipart path —
// a third independent UploadPart implementation after the aws CLI and the
// gateway's test signer.
func (s *server) rcloneEnv(t *testing.T) []string {
	// An existing empty file, not a missing one: rclone prints a NOTICE for
	// a missing config, and output-emptiness assertions would read it.
	cfg := filepath.Join(t.TempDir(), "rclone.conf")
	if err := os.WriteFile(cfg, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	return append(cleanEnv(),
		"RCLONE_CONFIG="+cfg,
		"RCLONE_CONFIG_HAMSTER_TYPE=s3",
		"RCLONE_CONFIG_HAMSTER_PROVIDER=Other",
		"RCLONE_CONFIG_HAMSTER_ENDPOINT="+s.URL,
		"RCLONE_CONFIG_HAMSTER_ACCESS_KEY_ID="+accessKey,
		"RCLONE_CONFIG_HAMSTER_SECRET_ACCESS_KEY="+secretKey,
		"RCLONE_CONFIG_HAMSTER_REGION="+region,
		"RCLONE_CONFIG_HAMSTER_UPLOAD_CUTOFF=5Mi",
		"RCLONE_CONFIG_HAMSTER_CHUNK_SIZE=5Mi",
	)
}

func TestRclone(t *testing.T) {
	bin := needTool(t, "rclone")
	s := startServer(t)
	env := s.rcloneEnv(t)
	run := func(t *testing.T, args ...string) string { return runTool(t, env, bin, args...) }

	src := t.TempDir()
	a := writeRandomFile(t, filepath.Join(src, "a.txt"), 3, 1024)
	b := writeRandomFile(t, filepath.Join(src, "nested", "b.bin"), 4, 100<<10)
	big := writeRandomFile(t, filepath.Join(src, "big.bin"), 5, 12<<20)

	t.Run("copy_and_check", func(t *testing.T) {
		run(t, "mkdir", "hamster:rcl")
		run(t, "copy", src, "hamster:rcl")
		// check verifies sizes and MD5 hashes against ETags — the test that
		// breaks first if ETag semantics drift from what sync tools expect.
		run(t, "check", src, "hamster:rcl")
	})

	t.Run("listing", func(t *testing.T) {
		out := run(t, "lsf", "-R", "--files-only", "hamster:rcl")
		for _, want := range []string{"a.txt", "nested/b.bin", "big.bin"} {
			if !strings.Contains(out, want) {
				t.Fatalf("lsf missing %q:\n%s", want, out)
			}
		}
	})

	t.Run("download", func(t *testing.T) {
		dst := t.TempDir()
		run(t, "copy", "hamster:rcl", dst)
		mustEqualFile(t, filepath.Join(dst, "a.txt"), a)
		mustEqualFile(t, filepath.Join(dst, "nested", "b.bin"), b)
		mustEqualFile(t, filepath.Join(dst, "big.bin"), big)
	})

	t.Run("sync_deletion", func(t *testing.T) {
		if err := os.Remove(filepath.Join(src, "a.txt")); err != nil {
			t.Fatal(err)
		}
		run(t, "sync", src, "hamster:rcl")
		if out := run(t, "lsf", "-R", "--files-only", "hamster:rcl"); strings.Contains(out, "a.txt") {
			t.Fatalf("a.txt survived sync deletion:\n%s", out)
		}
	})

	t.Run("purge", func(t *testing.T) {
		run(t, "purge", "hamster:rcl")
		if out := run(t, "lsd", "hamster:"); strings.TrimSpace(out) != "" {
			t.Fatalf("buckets remain after purge:\n%s", out)
		}
	})
}
