package wal_test

import (
	"bytes"
	"testing"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
	"github.com/hamster-storage/hamster/internal/sys"
	"github.com/hamster-storage/hamster/internal/wal"
)

func newDisk(t *testing.T) seam.Disk {
	t.Helper()
	d, err := sys.NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestLogRoundTrip(t *testing.T) {
	disk := newDisk(t)

	l, records, err := wal.Open(disk, "log")
	if err != nil || len(records) != 0 {
		t.Fatalf("open empty: %d records, %v", len(records), err)
	}
	want := [][]byte{[]byte("one"), {}, []byte("three, somewhat longer")}
	for _, r := range want {
		if err := l.Append(r); err != nil {
			t.Fatal(err)
		}
	}

	_, records, err = wal.Open(disk, "log")
	if err != nil || len(records) != len(want) {
		t.Fatalf("reopen: %d records, %v", len(records), err)
	}
	for i := range want {
		if !bytes.Equal(records[i], want[i]) {
			t.Fatalf("record %d: %q, want %q", i, records[i], want[i])
		}
	}
}

// TestTornTailRecovery plants every shape of torn tail a crash can leave —
// a cut-off length prefix, a frame missing payload bytes, a checksum that
// does not match — and requires Open to recover the intact records,
// truncate the damage, and append cleanly afterwards.
func TestTornTailRecovery(t *testing.T) {
	intact := [][]byte{[]byte("alpha"), []byte("beta")}
	tails := map[string][]byte{
		"cut_length_prefix": {0x96}, // first byte of a multi-byte uvarint
		"missing_payload":   {0x09, 0x00, 0x00, 0x00, 0x00, 'p', 'a', 'r'},
		"checksum_mismatch": {0x03, 0xFF, 0xFF, 0xFF, 0xFF, 'b', 'a', 'd'},
	}
	for name, tail := range tails {
		t.Run(name, func(t *testing.T) {
			disk := newDisk(t)
			l, _, err := wal.Open(disk, "log")
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range intact {
				if err := l.Append(r); err != nil {
					t.Fatal(err)
				}
			}
			// The crash: a torn suffix of an unsynced append, durable on
			// the next boot's disk.
			if err := disk.Append("log", tail); err != nil {
				t.Fatal(err)
			}
			if err := disk.Sync("log"); err != nil {
				t.Fatal(err)
			}

			l, records, err := wal.Open(disk, "log")
			if err != nil || len(records) != len(intact) {
				t.Fatalf("open over torn tail: %d records, %v", len(records), err)
			}
			if err := l.Append([]byte("gamma")); err != nil {
				t.Fatal(err)
			}
			_, records, err = wal.Open(disk, "log")
			if err != nil || len(records) != 3 || !bytes.Equal(records[2], []byte("gamma")) {
				t.Fatalf("append after truncation: %d records, %v", len(records), err)
			}
		})
	}
}

func TestRowLogReplay(t *testing.T) {
	disk := newDisk(t)

	l, rows, err := wal.OpenRows(disk, "meta/log")
	if err != nil || len(rows) != 0 {
		t.Fatalf("open empty: %v rows, %v", rows, err)
	}
	batches := [][]meta.Row{
		{{Key: "b/docs", Value: []byte{1}}, {Key: "v/docs\x00k", Value: []byte{2}}},
		{{Key: "v/docs\x00k", Value: []byte{3}}, {Key: "c/docs\x00k", Value: []byte{4}}},
		{{Key: "c/docs\x00k", Value: nil}, {Key: "v/docs\x00k", Value: nil}},
	}
	for _, b := range batches {
		if err := l.Commit(b); err != nil {
			t.Fatal(err)
		}
	}

	_, rows, err = wal.OpenRows(disk, "meta/log")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !bytes.Equal(rows["b/docs"], []byte{1}) {
		t.Fatalf("replayed rows: %v", rows)
	}
}

// TestRowLogRestoresStore closes the loop with the real store: a store
// persisting through a RowLog, reopened, must be the same store.
func TestRowLogRestoresStore(t *testing.T) {
	disk := newDisk(t)
	l, _, err := wal.OpenRows(disk, "meta/log")
	if err != nil {
		t.Fatal(err)
	}
	s := meta.NewStore()
	s.SetPersister(l)
	if err := s.ApplyCreateBucket(meta.CreateBucket{ProposedAtUnixMS: 1, Bucket: "docs"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyPutObject(meta.PutObject{ProposedAtUnixMS: 1, Bucket: "docs", Key: "k",
		VersionID: meta.VersionID{1}, Size: 3, ETag: []byte{0xAB}, ObjectChecksum: []byte{0xCD}}); err != nil {
		t.Fatal(err)
	}

	_, rows, err := wal.OpenRows(disk, "meta/log")
	if err != nil {
		t.Fatal(err)
	}
	restored := meta.NewStore()
	for k, v := range rows {
		if err := restored.Restore(k, v); err != nil {
			t.Fatal(err)
		}
	}
	cur, ok := restored.Current("docs", "k")
	if !ok {
		t.Fatal("restored store lost the object")
	}
	entry, ok := restored.GetVersion("docs", "k", cur.VersionID)
	if !ok || entry.Size != 3 || !bytes.Equal(entry.ETag, []byte{0xAB}) {
		t.Fatalf("restored entry: %+v, %v", entry, ok)
	}
}

// TestCorruptBatchIsAnError: malformed protobuf inside an intact frame is
// corruption, not a torn tail — surfaced, never skipped.
func TestCorruptBatchIsAnError(t *testing.T) {
	disk := newDisk(t)
	l, _, err := wal.Open(disk, "meta/log")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte{0xFF, 0xFF}); err != nil { // valid frame, garbage payload
		t.Fatal(err)
	}
	if _, _, err := wal.OpenRows(disk, "meta/log"); err == nil {
		t.Fatal("garbage batch replayed without error")
	}
}
