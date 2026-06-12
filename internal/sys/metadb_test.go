package sys

import (
	"testing"

	"github.com/hamster-storage/hamster/internal/meta"
)

func TestMetaDBRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenMetaDB(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Keys carry raw NUL and high bytes (the keyspace encoding does);
	// values are opaque. Overwrites and deletes in later transactions.
	if err := db.Commit([]meta.Row{
		{Key: "b/bkt", Value: []byte{1, 2}},
		{Key: "v/bkt\x00k\x00\xff\x00\xfe", Value: []byte{3}},
		{Key: "c/bkt\x00k", Value: []byte{4}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Commit([]meta.Row{
		{Key: "c/bkt\x00k", Value: []byte{5, 6}}, // overwrite
		{Key: "v/bkt\x00k\x00\xff\x00\xfe"},      // delete (nil value)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: exactly the surviving rows, byte for byte.
	db, err = OpenMetaDB(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got := make(map[string]string)
	if err := db.Load(func(k string, v []byte) error {
		got[k] = string(v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"b/bkt":      "\x01\x02",
		"c/bkt\x00k": "\x05\x06",
	}
	if len(got) != len(want) {
		t.Fatalf("loaded %d rows, want %d: %q", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("row %q: %q, want %q", k, got[k], v)
		}
	}
}
