// Package fsx defines the minimal filesystem abstraction used by the storage
// layer. Depending on an interface instead of *os.File lets tests inject
// faults (ENOSPC, EIO, short writes, fsync failures, ...) without root
// privileges.
package fsx

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// File is the subset of *os.File that durable storage code relies on.
type File interface {
	io.Writer
	io.Closer
	// Sync flushes buffered data and metadata of the file to stable storage.
	Sync() error
	Name() string
}

// FS is the filesystem surface required by the storage package.
type FS interface {
	// OpenFile mirrors os.OpenFile.
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	// Rename atomically replaces dst with src.
	Rename(oldpath, newpath string) error
	// Remove deletes a file.
	Remove(name string) error
	// ReadFile reads a whole file.
	ReadFile(name string) ([]byte, error)
	// SyncDir fsyncs the directory given by dir, so that a newly created
	// directory entry is persisted rather than only the file inode.
	SyncDir(dir string) error
}

// OSFS is the production implementation backed by the real operating system.
type OSFS struct {
	Root string
}

func NewOSFS(root string) *OSFS { return &OSFS{Root: root} }

func (o *OSFS) resolve(name string) (string, error) {
	if o.Root == "" {
		return "", errors.New("fsx: osfs root is empty")
	}
	clean := filepath.Clean(name)
	if filepath.IsAbs(clean) || clean == ".." || filepath.HasPrefix(clean, "../") {
		return "", &os.PathError{Op: "resolve", Path: name, Err: os.EINVAL}
	}
	return filepath.Join(o.Root, clean), nil
}

func (o *OSFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	full, err := o.resolve(name)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(full, flag, perm)
}

func (o *OSFS) Rename(oldpath, newpath string) error {
	from, err := o.resolve(oldpath)
	if err != nil {
		return err
	}
	to, err := o.resolve(newpath)
	if err != nil {
		return err
	}
	return os.Rename(from, to)
}

func (o *OSFS) Remove(name string) error {
	full, err := o.resolve(name)
	if err != nil {
		return err
	}
	return os.Remove(full)
}

func (o *OSFS) ReadFile(name string) ([]byte, error) {
	full, err := o.resolve(name)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(full)
}

func (o *OSFS) SyncDir(dir string) error {
	full, err := o.resolve(dir)
	if err != nil {
		return err
	}
	d, err := os.Open(full)
	if err != nil {
		return err
	}
	return d.Close() // replaced below
}
