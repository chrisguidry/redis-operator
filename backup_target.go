package main

// Where a snapshot goes: an S3-compatible object store, or a directory
// on a volume the instance mounts. Both take the same keys, so a Backup
// names its snapshot the same way wherever it lands.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type backupTarget interface {
	put(ctx context.Context, key string, body io.Reader, size int64) error
	get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	delete(ctx context.Context, key string) error
	relativeKey(key string) string
}

// dirStore keeps snapshots as files under root.
type dirStore struct {
	root string
}

func (d dirStore) path(key string) (string, error) {
	path := filepath.Join(d.root, filepath.FromSlash(key))
	if !strings.HasPrefix(path, filepath.Clean(d.root)+string(filepath.Separator)) {
		return "", fmt.Errorf("key %q leaves the backup volume", key)
	}
	return path, nil
}

// put writes the file under a temporary name and renames it, so a
// snapshot that stops halfway never looks whole.
func (d dirStore) put(_ context.Context, key string, body io.Reader, size int64) error {
	path, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	partial, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(partial.Name())
	written, err := io.Copy(partial, body)
	if syncErr := partial.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := partial.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if written != size {
		return fmt.Errorf("wrote %d of %d bytes to %s", written, size, key)
	}
	return os.Rename(partial.Name(), path)
}

func (d dirStore) get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	path, err := d.path(key)
	if err != nil {
		return nil, 0, err
	}
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, 0, fmt.Errorf("%w: %s", errNoSuchKey, key)
	}
	if err != nil {
		return nil, 0, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, err
	}
	return file, stat.Size(), nil
}

func (d dirStore) delete(_ context.Context, key string) error {
	path, err := d.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (d dirStore) relativeKey(key string) string { return key }
