//go:build compat

package compat

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

// jsonField pulls a string field out of an aws CLI JSON response (s3api emits
// JSON by default).
func jsonField(t *testing.T, out, field string) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("parsing aws JSON output: %v\n%s", err, out)
	}
	s, _ := m[field].(string)
	return s
}

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

// TestAWSVersioning drives the v0.5 versioning surface through the real aws CLI:
// put/get bucket versioning, version IDs on put-object, list-object-versions,
// get-object --version-id, and delete-object with and without a version id.
func TestAWSVersioning(t *testing.T) {
	bin := needTool(t, "aws")
	s := startServer(t)
	env := s.awsEnv(t)
	run := func(t *testing.T, args ...string) string {
		return runTool(t, env, bin, append([]string{"--endpoint-url", s.URL}, args...)...)
	}
	dir := t.TempDir()
	body1 := writeRandomFile(t, filepath.Join(dir, "v1.bin"), 11, 32<<10)
	body2 := writeRandomFile(t, filepath.Join(dir, "v2.bin"), 12, 48<<10)

	run(t, "s3", "mb", "s3://vbucket")
	run(t, "s3api", "put-bucket-versioning", "--bucket", "vbucket",
		"--versioning-configuration", "Status=Enabled")
	if got := run(t, "s3api", "get-bucket-versioning", "--bucket", "vbucket"); !strings.Contains(got, "Enabled") {
		t.Fatalf("get-bucket-versioning did not report Enabled:\n%s", got)
	}

	// Two versions of one key; the CLI surfaces each VersionId from the
	// x-amz-version-id response header.
	vid1 := jsonField(t, run(t, "s3api", "put-object", "--bucket", "vbucket", "--key", "obj", "--body", filepath.Join(dir, "v1.bin")), "VersionId")
	vid2 := jsonField(t, run(t, "s3api", "put-object", "--bucket", "vbucket", "--key", "obj", "--body", filepath.Join(dir, "v2.bin")), "VersionId")
	if vid1 == "" || vid2 == "" || vid1 == vid2 {
		t.Fatalf("put-object version ids: %q, %q", vid1, vid2)
	}

	if lov := run(t, "s3api", "list-object-versions", "--bucket", "vbucket"); !strings.Contains(lov, vid1) || !strings.Contains(lov, vid2) {
		t.Fatalf("list-object-versions missing a version:\n%s", lov)
	}

	// Each version is fetchable by id.
	run(t, "s3api", "get-object", "--bucket", "vbucket", "--key", "obj", "--version-id", vid1, filepath.Join(dir, "v1.out"))
	mustEqualFile(t, filepath.Join(dir, "v1.out"), body1)
	run(t, "s3api", "get-object", "--bucket", "vbucket", "--key", "obj", "--version-id", vid2, filepath.Join(dir, "v2.out"))
	mustEqualFile(t, filepath.Join(dir, "v2.out"), body2)

	// Delete with no version id drops a marker.
	markerVID := jsonField(t, run(t, "s3api", "delete-object", "--bucket", "vbucket", "--key", "obj"), "VersionId")
	if markerVID == "" {
		t.Fatal("delete-object did not report a delete-marker version id")
	}

	// Permanently delete every version and the marker, leaving the bucket empty.
	for _, vid := range []string{vid1, vid2, markerVID} {
		run(t, "s3api", "delete-object", "--bucket", "vbucket", "--key", "obj", "--version-id", vid)
	}
	run(t, "s3", "rb", "s3://vbucket")
}

// TestAWSObjectLock drives the v0.6 object-lock surface through the real aws CLI:
// creating a lock-enabled bucket, the bucket lock configuration, per-object
// retention, and legal hold. The COMPLIANCE object is left in place — it cannot
// be deleted, which is the point — and the per-test server is discarded with it.
func TestAWSObjectLock(t *testing.T) {
	bin := needTool(t, "aws")
	s := startServer(t)
	env := s.awsEnv(t)
	run := func(t *testing.T, args ...string) string {
		return runTool(t, env, bin, append([]string{"--endpoint-url", s.URL}, args...)...)
	}
	dir := t.TempDir()
	writeRandomFile(t, filepath.Join(dir, "doc.bin"), 21, 16<<10)

	run(t, "s3api", "create-bucket", "--bucket", "locked", "--object-lock-enabled-for-bucket")

	// Bucket default retention round-trips.
	run(t, "s3api", "put-object-lock-configuration", "--bucket", "locked",
		"--object-lock-configuration", `{"ObjectLockEnabled":"Enabled","Rule":{"DefaultRetention":{"Mode":"GOVERNANCE","Days":1}}}`)
	if got := run(t, "s3api", "get-object-lock-configuration", "--bucket", "locked"); !strings.Contains(got, "GOVERNANCE") {
		t.Fatalf("get-object-lock-configuration:\n%s", got)
	}

	// An object under explicit COMPLIANCE retention.
	run(t, "s3api", "put-object", "--bucket", "locked", "--key", "doc", "--body", filepath.Join(dir, "doc.bin"),
		"--object-lock-mode", "COMPLIANCE", "--object-lock-retain-until-date", "2099-01-01T00:00:00Z")
	if got := run(t, "s3api", "get-object-retention", "--bucket", "locked", "--key", "doc"); !strings.Contains(got, "COMPLIANCE") {
		t.Fatalf("get-object-retention:\n%s", got)
	}

	// Legal hold toggles and reads back.
	run(t, "s3api", "put-object-legal-hold", "--bucket", "locked", "--key", "doc", "--legal-hold", "Status=ON")
	if got := run(t, "s3api", "get-object-legal-hold", "--bucket", "locked", "--key", "doc"); !strings.Contains(got, "ON") {
		t.Fatalf("get-object-legal-hold:\n%s", got)
	}
}
