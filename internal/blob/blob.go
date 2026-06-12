// Package blob stores object data as files on a seam.Disk, addressed by
// data IDs (meta.VersionEntry.DataID). It is the v0.1 single-node data
// path; the erasure-coded distributed path replaces it behind the gateway's
// blob interface without changing the metadata it feeds.
//
// Put streams through the write buffer: a fixed-size buffer reads from the
// source and appends to disk, so memory stays bounded no matter the object
// size. Data is durable before Put returns — appended, then synced —
// because the metadata commit that follows is the linearization point and
// must never reference bytes a crash could lose (docs/ARCHITECTURE.md).
package blob

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
)

// writeBufferSize is how much of an incoming object is in memory at once —
// the v0.1 write buffer. It matches the nominal frame chunk size in
// docs/DATA-STREAM.md, the unit the erasure-coded path will also work in.
const writeBufferSize = 1 << 20

// Store is a blob store over one disk.
type Store struct {
	disk seam.Disk
}

// NewStore returns a Store backed by disk.
func NewStore(disk seam.Disk) *Store {
	return &Store{disk: disk}
}

func name(id meta.VersionID) string {
	return "o/" + hex.EncodeToString(id[:])
}

// Put streams r's content under id and syncs it: durable when Put returns,
// with the reported size the byte count stored. On any error — the
// reader's or the disk's — the staged bytes are removed best-effort and
// nothing is left behind. A reader error is returned wrapped, so callers
// can still classify it with errors.Is or errors.As.
func (s *Store) Put(id meta.VersionID, r io.Reader) (int64, error) {
	n := name(id)
	// Stage the file empty first: a zero-length object is a real object,
	// and must exist even if r yields no bytes.
	if err := s.disk.WriteFile(n, nil); err != nil {
		return 0, fmt.Errorf("blob create %s: %w", n, err)
	}
	buf := make([]byte, writeBufferSize)
	var size int64
	for {
		nr, readErr := r.Read(buf)
		if nr > 0 {
			if err := s.disk.Append(n, buf[:nr]); err != nil {
				s.discard(n)
				return size, fmt.Errorf("blob append %s: %w", n, err)
			}
			size += int64(nr)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			s.discard(n)
			return size, fmt.Errorf("blob put %s: reading source: %w", n, readErr)
		}
	}
	if err := s.disk.Sync(n); err != nil {
		s.discard(n)
		return size, fmt.Errorf("blob sync %s: %w", n, err)
	}
	return size, nil
}

// discard removes a failed Put's staging, best effort: if the remove fails
// the bytes are an orphan for GC, never a visible object — only the
// metadata commit makes data visible.
func (s *Store) discard(n string) {
	if s.disk.Remove(n) == nil {
		_ = s.disk.Sync(n)
	}
}

// Get returns the data stored under id. A missing blob satisfies
// errors.Is(err, fs.ErrNotExist).
func (s *Store) Get(id meta.VersionID) ([]byte, error) {
	data, err := s.disk.ReadFile(name(id))
	if err != nil {
		return nil, fmt.Errorf("blob get %s: %w", name(id), err)
	}
	return data, nil
}

// Remove deletes the blob under id and syncs the removal. Removing a blob
// that does not exist is an error, as on seam.Disk.
func (s *Store) Remove(id meta.VersionID) error {
	n := name(id)
	if err := s.disk.Remove(n); err != nil {
		return fmt.Errorf("blob remove %s: %w", n, err)
	}
	if err := s.disk.Sync(n); err != nil {
		return fmt.Errorf("blob remove sync %s: %w", n, err)
	}
	return nil
}
