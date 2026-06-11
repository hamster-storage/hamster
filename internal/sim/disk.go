package sim

import (
	"io/fs"
	"maps"
	"math/rand/v2"
	"slices"
)

// disk implements seam.Disk with the crash semantics the design demands:
// every change is staged until Sync makes it durable, and a crash resolves
// each staged change adversarially — reverted, torn, or kept, by the PRNG.
type disk struct {
	durable map[string][]byte
	staged  map[string]stagedChange
}

// stagedChange is one un-synced mutation: either new content or a removal.
type stagedChange struct {
	data    []byte
	removed bool
}

func newDisk() *disk {
	return &disk{
		durable: make(map[string][]byte),
		staged:  make(map[string]stagedChange),
	}
}

func (d *disk) WriteFile(name string, data []byte) error {
	if !fs.ValidPath(name) {
		return &fs.PathError{Op: "write", Path: name, Err: fs.ErrInvalid}
	}
	d.staged[name] = stagedChange{data: slices.Clone(data)}
	return nil
}

func (d *disk) Sync(name string) error {
	if !fs.ValidPath(name) {
		return &fs.PathError{Op: "sync", Path: name, Err: fs.ErrInvalid}
	}
	st, ok := d.staged[name]
	if !ok {
		return nil
	}
	if st.removed {
		delete(d.durable, name)
	} else {
		d.durable[name] = st.data
	}
	delete(d.staged, name)
	return nil
}

func (d *disk) ReadFile(name string) ([]byte, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrInvalid}
	}
	if st, ok := d.staged[name]; ok {
		if st.removed {
			return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
		}
		return slices.Clone(st.data), nil
	}
	if data, ok := d.durable[name]; ok {
		return slices.Clone(data), nil
	}
	return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
}

func (d *disk) Remove(name string) error {
	if !fs.ValidPath(name) {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrInvalid}
	}
	if st, ok := d.staged[name]; ok {
		if st.removed {
			return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
		}
		d.staged[name] = stagedChange{removed: true}
		return nil
	}
	if _, ok := d.durable[name]; !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	d.staged[name] = stagedChange{removed: true}
	return nil
}

func (d *disk) List() ([]string, error) {
	names := make([]string, 0, len(d.durable)+len(d.staged))
	for name := range d.durable {
		if st, ok := d.staged[name]; ok && st.removed {
			continue
		}
		names = append(names, name)
	}
	for name, st := range d.staged {
		if st.removed {
			continue
		}
		if _, ok := d.durable[name]; ok {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

// crash resolves every staged change the way real storage does: maybe the
// write never reached the platter, maybe it landed partially, maybe it
// completed. Staged removes are simply lost — the file comes back. Iteration
// is in sorted name order so PRNG consumption stays deterministic.
func (d *disk) crash(rng *rand.Rand) {
	for _, name := range slices.Sorted(maps.Keys(d.staged)) {
		st := d.staged[name]
		if st.removed {
			continue // the unlink never became durable
		}
		if rng.IntN(2) == 0 {
			continue // the write never became durable
		}
		// Torn write: a prefix landed, possibly all of it, replacing
		// whatever was durable before.
		d.durable[name] = st.data[:rng.IntN(len(st.data)+1)]
	}
	clear(d.staged)
}
