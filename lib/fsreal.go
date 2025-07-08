//nolint:wrapcheck,forbidigo
package lib

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

type RealFS struct {
	BasePath string
}

func NewRealFS(basePath string) *RealFS {
	return &RealFS{BasePath: basePath}
}

func (f *RealFS) OpenWrite(name string) (io.WriteCloser, error) {
	return os.OpenFile(filepath.Join(f.BasePath, name), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
}

func (f *RealFS) OpenWriteExcl(name string) (io.WriteCloser, error) {
	return os.OpenFile(filepath.Join(f.BasePath, name), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
}

func (f *RealFS) OpenRead(name string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(f.BasePath, name))
}

func (f *RealFS) Chmod(name string, mode fs.FileMode) error {
	return os.Chmod(filepath.Join(f.BasePath, name), mode)
}

func (f *RealFS) Chmtime(name string, mtime time.Time) error {
	return os.Chtimes(filepath.Join(f.BasePath, name), time.Time{}, mtime)
}

func (f *RealFS) Chown(name string, uid int, gid int) error {
	return os.Chown(filepath.Join(f.BasePath, name), uid, gid)
}

func (f *RealFS) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(filepath.Join(f.BasePath, name))
}

func (f *RealFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(filepath.Join(f.BasePath, name))
}

func (f *RealFS) Mkdir(name string) error {
	return os.Mkdir(filepath.Join(f.BasePath, name), 0o700)
}

func (f *RealFS) MkdirAll(path string) error {
	return os.MkdirAll(filepath.Join(f.BasePath, path), 0o700)
}

func (f *RealFS) Remove(name string) error {
	return os.Remove(filepath.Join(f.BasePath, name))
}

func (f *RealFS) RemoveAll(path string) error {
	return os.RemoveAll(filepath.Join(f.BasePath, path))
}

func (f *RealFS) Rename(oldpath, newpath string) error {
	return os.Rename(filepath.Join(f.BasePath, oldpath), filepath.Join(f.BasePath, newpath))
}

func (f *RealFS) MkSub(path string) (FS, error) {
	if err := os.MkdirAll(filepath.Join(f.BasePath, path), 0o700); err != nil {
		return nil, WrapErrorf(err, "failed to create directory %s", path)
	}
	return &RealFS{BasePath: filepath.Join(f.BasePath, path)}, nil
}

func (f *RealFS) WalkDir(path string, fn fs.WalkDirFunc) error {
	return filepath.WalkDir(filepath.Join(f.BasePath, path), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(f.BasePath, path)
		if err != nil {
			return err
		}
		return fn(relPath, d, err)
	})
}

func (f *RealFS) String() string {
	return "RealFS(" + f.BasePath + ")"
}
