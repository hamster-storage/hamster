// Package blob stores object data as whole files on a seam.Disk, addressed
// by data IDs (meta.VersionEntry.DataID). It is the v0.1 single-node data
// path; the erasure-coded distributed path replaces it behind the gateway's
// blob interface without changing the metadata it feeds.
//
// Put makes data durable before it returns — write then sync — because the
// metadata commit that follows is the linearization point and must never
// reference bytes a crash could lose (docs/ARCHITECTURE.md).
package blob

import (
	"encoding/hex"
	"fmt"

	"github.com/hamster-storage/hamster/internal/meta"
	"github.com/hamster-storage/hamster/internal/seam"
)

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

// Put writes data under id and syncs it: durable when Put returns.
func (s *Store) Put(id meta.VersionID, data []byte) error {
	n := name(id)
	if err := s.disk.WriteFile(n, data); err != nil {
		return fmt.Errorf("blob put %s: %w", n, err)
	}
	if err := s.disk.Sync(n); err != nil {
		return fmt.Errorf("blob sync %s: %w", n, err)
	}
	return nil
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
