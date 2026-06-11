//go:build compat

package compat

import (
	"path/filepath"
	"testing"
)

// restic is built on the minio-go SDK — a different SigV4 signer and S3
// client than the aws CLI or rclone use — and its check --read-data pass
// re-downloads and verifies every pack it wrote.
func TestRestic(t *testing.T) {
	bin := needTool(t, "restic")
	s := startServer(t)
	env := append(cleanEnv(),
		"AWS_ACCESS_KEY_ID="+accessKey,
		"AWS_SECRET_ACCESS_KEY="+secretKey,
		"RESTIC_REPOSITORY=s3:"+s.URL+"/restic",
		"RESTIC_PASSWORD=compat-suite",
		"RESTIC_CACHE_DIR="+t.TempDir(),
	)
	run := func(t *testing.T, args ...string) string { return runTool(t, env, bin, args...) }

	data := t.TempDir()
	doc := writeRandomFile(t, filepath.Join(data, "doc.txt"), 6, 4096)
	blob := writeRandomFile(t, filepath.Join(data, "blob.bin"), 7, 6<<20)

	t.Run("init", func(t *testing.T) {
		run(t, "init") // creates the bucket itself via minio-go
	})

	t.Run("backup", func(t *testing.T) {
		run(t, "backup", data)
	})

	t.Run("check_read_data", func(t *testing.T) {
		run(t, "check", "--read-data")
	})

	t.Run("restore", func(t *testing.T) {
		target := t.TempDir()
		run(t, "restore", "latest", "--target", target)
		// restic restores under the snapshot's absolute source path; find
		// the files wherever it put them.
		mustEqualFile(t, findFile(t, target, "doc.txt"), doc)
		mustEqualFile(t, findFile(t, target, "blob.bin"), blob)
	})
}
