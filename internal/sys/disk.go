package sys

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Disk implements seam.Disk on a directory of real files.
//
// WriteFile writes in place, so a crash between WriteFile and Sync can
// leave the previous content, a torn prefix, or the full new data — exactly
// the outcomes the simulated disk models. Sync fsyncs the file and every
// directory from its parent up to the root, which is what makes creations
// and removals durable on POSIX filesystems.
type Disk struct {
	root string
}

// NewDisk creates (if needed) the root directory and returns a Disk over it.
func NewDisk(root string) (*Disk, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolving disk root %q: %w", root, err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("creating disk root: %w", err)
	}
	return &Disk{root: abs}, nil
}

// WriteFile implements seam.Disk.
func (d *Disk) WriteFile(name string, data []byte) error {
	path, err := d.path("write", name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating parent of %q: %w", name, err)
	}
	return os.WriteFile(path, data, 0o644)
}

// Append implements seam.Disk: open with O_APPEND (creating if needed),
// write, close. Durability still comes from Sync, exactly as for WriteFile.
func (d *Disk) Append(name string, data []byte) error {
	path, err := d.path("append", name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating parent of %q: %w", name, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("opening %q for append: %w", name, err)
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil {
		return fmt.Errorf("appending to %q: %w", name, writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("closing %q after append: %w", name, closeErr)
	}
	return nil
}

// Sync implements seam.Disk. A missing file is not an error: the staged
// change being made durable may be a removal, in which case only the
// directory fsyncs matter.
func (d *Disk) Sync(name string) error {
	path, err := d.path("sync", name)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	switch {
	case err == nil:
		syncErr := f.Sync()
		closeErr := f.Close()
		if syncErr != nil {
			return fmt.Errorf("syncing %q: %w", name, syncErr)
		}
		if closeErr != nil {
			return fmt.Errorf("closing %q after sync: %w", name, closeErr)
		}
	case !os.IsNotExist(err):
		return fmt.Errorf("opening %q for sync: %w", name, err)
	}
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("syncing directory for %q: %w", name, err)
		}
		if dir == d.root {
			return nil
		}
	}
}

// ReadFile implements seam.Disk.
func (d *Disk) ReadFile(name string) ([]byte, error) {
	path, err := d.path("read", name)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

// ReadFileAt implements seam.Disk: a positioned read via os.File.ReadAt,
// short (not an error) at end of file.
func (d *Disk) ReadFileAt(name string, offset int64, length int) ([]byte, error) {
	path, err := d.path("readat", name)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length < 0 {
		return nil, &fs.PathError{Op: "readat", Path: name, Err: fs.ErrInvalid}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	closeErr := f.Close()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("reading %q at %d: %w", name, offset, err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("closing %q after read: %w", name, closeErr)
	}
	return buf[:n], nil
}

// Remove implements seam.Disk.
func (d *Disk) Remove(name string) error {
	path, err := d.path("remove", name)
	if err != nil {
		return err
	}
	return os.Remove(path)
}

// List implements seam.Disk. fs.WalkDir visits in lexical order, so the
// result is already sorted.
func (d *Disk) List() ([]string, error) {
	var names []string
	err := fs.WalkDir(os.DirFS(d.root), ".", func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			names = append(names, p)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing disk: %w", err)
	}
	return names, nil
}

func (d *Disk) path(op, name string) (string, error) {
	if !fs.ValidPath(name) {
		return "", &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	return filepath.Join(d.root, filepath.FromSlash(name)), nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
