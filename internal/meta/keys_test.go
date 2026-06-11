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
