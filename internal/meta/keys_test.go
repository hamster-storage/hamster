package meta

import (
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
)

func TestVersionRowsSortNewestFirst(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 0))
	older := mintAt(1_000, rng)
	newer := mintAt(2_000, rng)
	if !(versionRowKey("b", "k", newer) < versionRowKey("b", "k", older)) {
		t.Fatal("newer version does not sort first under the key prefix")
	}
}

func TestVersionRowRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(2, 0))
	vid := mintAt(42_000, rng)
	for _, key := range []string{"k", "a/b/c", "日本語のキー", strings.Repeat("x", 1024)} {
		row := versionRowKey("bucket", key, vid)
		gotKey, gotVid := keyAndVersionFromVersionRow(row, "bucket")
		if gotKey != key || gotVid != vid {
			t.Fatalf("round trip of %q: got (%q, %v)", key, gotKey, gotVid)
		}
	}
	row := currentRowKey("bucket", "a/b")
	if got := objectKeyFromCurrentRow(row, "bucket"); got != "a/b" {
		t.Fatalf("current row round trip: got %q", got)
	}
}

func TestUploadRowRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 0))
	uid := mintAt(7_000, rng)
	for _, key := range []string{"k", "a/b/c", "日本語のキー", strings.Repeat("x", 1024)} {
		gotKey, gotUID, _, isPart := uploadFromRow(uploadRowKey("bkt", key, uid), "bkt")
		if gotKey != key || gotUID != uid || isPart {
			t.Fatalf("upload row round trip of %q: got (%q, %v, part=%v)", key, gotKey, gotUID, isPart)
		}
		gotKey, gotUID, part, isPart := uploadFromRow(partRowKey("bkt", key, uid, 7), "bkt")
		if gotKey != key || gotUID != uid || part != 7 || !isPart {
			t.Fatalf("part row round trip of %q: got (%q, %v, %d, part=%v)", key, gotKey, gotUID, part, isPart)
		}
	}

	// IDs containing NUL bytes parse fine: the key/ID split uses the first
	// NUL, and object keys cannot contain one.
	hostile := VersionID{0x00, 0xFF, 0x00, 0xFF}
	k, id, n, isPart := uploadFromRow(partRowKey("bkt", "k", hostile, 1), "bkt")
	if k != "k" || id != hostile || n != 1 || !isPart {
		t.Fatalf("NUL-bearing ID round trip: (%q, %v, %d, part=%v)", k, id, n, isPart)
	}

	// Part rows sort directly after their upload row, numerically.
	if !(uploadRowKey("bkt", "k", uid) < partRowKey("bkt", "k", uid, 1)) {
		t.Fatal("part row does not sort after its upload row")
	}
	if !(partRowKey("bkt", "k", uid, 2) < partRowKey("bkt", "k", uid, 10)) {
		t.Fatal("part numbers do not sort numerically")
	}
}

func TestValidateObjectKey(t *testing.T) {
	for _, bad := range []string{"", "a\x00b", "\x00", strings.Repeat("x", 1025)} {
		if err := validateObjectKey(bad); !errors.Is(err, ErrInvalidObjectKey) {
			t.Errorf("key %q accepted", bad)
		}
	}
	for _, good := range []string{"a", "a/b c.txt", "日本語", strings.Repeat("x", 1024)} {
		if err := validateObjectKey(good); err != nil {
			t.Errorf("key %q rejected: %v", good, err)
		}
	}
}

func TestValidateBucketName(t *testing.T) {
	for _, bad := range []string{"ab", "-abc", "abc-", "Abc", "a_b_c", strings.Repeat("x", 64)} {
		if err := validateBucketName(bad); !errors.Is(err, ErrInvalidBucketName) {
			t.Errorf("bucket name %q accepted", bad)
		}
	}
	for _, good := range []string{"abc", "my-bucket.backups", "0numbers9", strings.Repeat("x", 63)} {
		if err := validateBucketName(good); err != nil {
			t.Errorf("bucket name %q rejected: %v", good, err)
		}
	}
}
