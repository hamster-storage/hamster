package meta

import (
	"math/rand/v2"
	"testing"
	"time"
)

func mintAt(ms int64, rng *rand.Rand) VersionID {
	return NewVersionID(time.UnixMilli(ms), rng)
}

func TestNewVersionIDLayout(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 0))
	at := int64(1_750_000_000_123)
	id := mintAt(at, rng)

	if got := id[6] >> 4; got != 7 {
		t.Errorf("version nibble = %d, want 7", got)
	}
	if got := id[8] >> 6; got != 0b10 {
		t.Errorf("variant bits = %b, want 10", got)
	}
	ms := int64(id[0])<<40 | int64(id[1])<<32 | int64(id[2])<<24 |
		int64(id[3])<<16 | int64(id[4])<<8 | int64(id[5])
	if ms != at {
		t.Errorf("embedded timestamp = %d, want %d", ms, at)
	}
}

func TestVersionIDTimeOrdering(t *testing.T) {
	rng := rand.New(rand.NewPCG(2, 0))
	older := mintAt(1_000, rng)
	newer := mintAt(2_000, rng)
	if older.Compare(newer) >= 0 {
		t.Fatal("an earlier mint did not sort before a later one")
	}
}

func TestVersionIDDeterminism(t *testing.T) {
	a := mintAt(5_000, rand.New(rand.NewPCG(3, 0)))
	b := mintAt(5_000, rand.New(rand.NewPCG(3, 0)))
	if a != b {
		t.Fatal("same time and seed minted different IDs")
	}
}

func TestVersionIDNext(t *testing.T) {
	id := VersionID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0x01, 0xff}
	next := id.Next()
	if next.Compare(id) <= 0 {
		t.Fatal("Next did not sort after the original")
	}
	want := VersionID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0x02, 0x00}
	if next != want {
		t.Fatalf("carry: got %v, want %v", next, want)
	}
	if (VersionID{}).IsZero() != true || next.IsZero() {
		t.Fatal("IsZero misbehaves")
	}
}
