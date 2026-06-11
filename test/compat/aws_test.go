//go:build compat

package compat

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

// awsEnv configures the aws CLI against s with nothing inherited: explicit
// credentials, no config files, no instance metadata, no retries (everything
// is local — a failure is a failure, not weather).
//
// Checksum behavior is pinned to when_required: recent CLI versions default
// to when_supported, which uploads with x-amz-checksum-* trailers that
// Hamster does not implement yet (S3-API.md schedules the checksum family
// as a later, additive arrival). When it lands, drop these two lines and the
// suite tests the default path again.
func (s *server) awsEnv(t *testing.T) []string {
	none := filepath.Join(t.TempDir(), "nonexistent")
	return append(cleanEnv(),
		"AWS_ACCESS_KEY_ID="+accessKey,
		"AWS_SECRET_ACCESS_KEY="+secretKey,
		"AWS_DEFAULT_REGION="+region,
		"AWS_REGION="+region,
		"AWS_CONFIG_FILE="+none,
		"AWS_SHARED_CREDENTIALS_FILE="+none,
		"AWS_EC2_METADATA_DISABLED=true",
		"AWS_PAGER=",
		"AWS_MAX_ATTEMPTS=1",
		"AWS_REQUEST_CHECKSUM_CALCULATION=when_required",
		"AWS_RESPONSE_CHECKSUM_VALIDATION=when_required",
	)
}

func TestAWS(t *testing.T) {
	bin := needTool(t, "aws")
	s := startServer(t)
	env := s.awsEnv(t)
	run := func(t *testing.T, args ...string) string {
		return runTool(t, env, bin, append([]string{"--endpoint-url", s.URL}, args...)...)
	}

	dir := t.TempDir()
	small := writeRandomFile(t, filepath.Join(dir, "small.bin"), 1, 64<<10)
	big := writeRandomFile(t, filepath.Join(dir, "big.bin"), 2, 16<<20)

	t.Run("upload", func(t *testing.T) {
		run(t, "s3", "mb", "s3://compat")
		// The CLI signs these as aws-chunked streaming uploads on plain
		// HTTP, and splits big.bin into multipart automatically above its
		// 8 MiB threshold — both paths exercised with zero flags.
		run(t, "s3", "cp", filepath.Join(dir, "small.bin"), "s3://compat/small.bin")
		run(t, "s3", "cp", filepath.Join(dir, "big.bin"), "s3://compat/big.bin")
	})

	t.Run("etag_shapes", func(t *testing.T) {
		head := run(t, "s3api", "head-object", "--bucket", "compat", "--key", "small.bin")
		sum := md5.Sum(small)
		if !strings.Contains(head, hex.EncodeToString(sum[:])) {
			t.Fatalf("single-PUT ETag is not the body MD5:\n%s", head)
		}
		head = run(t, "s3api", "head-object", "--bucket", "compat", "--key", "big.bin")
		if !strings.Contains(head, "-2\\\"") {
			t.Fatalf("16 MiB upload should be a 2-part composite ETag:\n%s", head)
		}
	})

	t.Run("download", func(t *testing.T) {
		run(t, "s3", "cp", "s3://compat/small.bin", filepath.Join(dir, "small.out"))
		run(t, "s3", "cp", "s3://compat/big.bin", filepath.Join(dir, "big.out"))
		mustEqualFile(t, filepath.Join(dir, "small.out"), small)
		mustEqualFile(t, filepath.Join(dir, "big.out"), big)
	})

	t.Run("server_side_copy", func(t *testing.T) {
		run(t, "s3", "cp", "s3://compat/small.bin", "s3://compat/copied.bin")
		run(t, "s3", "cp", "s3://compat/copied.bin", filepath.Join(dir, "copied.out"))
		mustEqualFile(t, filepath.Join(dir, "copied.out"), small)
	})

	t.Run("presign", func(t *testing.T) {
		url := strings.TrimSpace(run(t, "s3", "presign", "s3://compat/small.bin"))
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("GET presigned URL: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("presigned GET: %d\n%s", resp.StatusCode, body)
		}
		sum, want := md5.Sum(body), md5.Sum(small)
		if sum != want {
			t.Fatalf("presigned GET returned wrong content (%d bytes)", len(body))
		}
	})

	t.Run("recursive_delete", func(t *testing.T) {
		run(t, "s3", "rm", "s3://compat", "--recursive") // batches through DeleteObjects
		run(t, "s3", "rb", "s3://compat")
		if out := run(t, "s3", "ls"); strings.TrimSpace(out) != "" {
			t.Fatalf("buckets remain after rb:\n%s", out)
		}
	})
}
