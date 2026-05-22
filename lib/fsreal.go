//go:build !wasm

//nolint:wrapcheck,forbidigo
package lib

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type RealFS struct {
	BasePath string
}

func NewRealFS(basePath string) *RealFS {
	return &RealFS{BasePath: basePath}
}

func (f *RealFS) OpenWrite(name string) (io.WriteCloser, error) {
	file, err := os.OpenFile(
		filepath.Join(f.BasePath, name),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC|syscall.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, translateErrIsSymlink("open", name, err)
	}
	return file, nil
}

func (f *RealFS) OpenWriteExcl(name string) (io.WriteCloser, error) {
	// macOS reports `EEXIST` before `ELOOP` when both `O_EXCL` and a symlink
	// would trigger. Check ourselves first so both platforms surface
	// `ErrIsSymlink`.
	info, err := os.Lstat(filepath.Join(f.BasePath, name))
	if err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: ErrIsSymlink}
	}
	file, err := os.OpenFile(
		filepath.Join(f.BasePath, name),
		os.O_CREATE|os.O_WRONLY|os.O_EXCL|syscall.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, translateErrIsSymlink("open", name, err)
	}
	return file, nil
}

func (f *RealFS) FSync(file io.WriteCloser) error {
	fsFile, ok := file.(*os.File)
	if !ok {
		return Errorf("invalid file type %T", file)
	}
	return fsFile.Sync()
}

func (f *RealFS) FSyncDir(path string) error {
	path = filepath.Join(f.BasePath, path)
	dir, err := os.Open(path)
	if err != nil {
		return WrapErrorf(err, "failed to open directory %s", path)
	}
	defer dir.Close() //nolint:errcheck
	if err := dir.Sync(); err != nil {
		return WrapErrorf(err, "failed to sync directory %s", path)
	}
	return nil
}

func (f *RealFS) OpenRead(name string) (io.ReadCloser, error) {
	file, err := os.OpenFile(filepath.Join(f.BasePath, name), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, translateErrIsSymlink("open", name, err)
	}
	return file, nil
}

func (f *RealFS) Chmod(name string, mode fs.FileMode) error {
	if err := f.refuseSymlink("chmod", name); err != nil {
		return err
	}
	return os.Chmod(filepath.Join(f.BasePath, name), mode)
}

func (f *RealFS) Chmtime(name string, mtime time.Time) error {
	ts := []unix.Timespec{
		unix.NsecToTimespec(mtime.UnixNano()),
		unix.NsecToTimespec(mtime.UnixNano()),
	}
	return unix.UtimesNanoAt(unix.AT_FDCWD, filepath.Join(f.BasePath, name), ts, unix.AT_SYMLINK_NOFOLLOW)
}

func (f *RealFS) Chown(name string, uid int, gid int) error {
	if err := f.refuseSymlink("chown", name); err != nil {
		return err
	}
	return os.Chown(filepath.Join(f.BasePath, name), uid, gid)
}

func (f *RealFS) Stat(name string) (fs.FileInfo, error) {
	return os.Lstat(filepath.Join(f.BasePath, name))
}

func (f *RealFS) Symlink(target string, name string) error {
	return os.Symlink(target, filepath.Join(f.BasePath, name))
}

func (f *RealFS) ReadLink(name string) (string, error) {
	return os.Readlink(filepath.Join(f.BasePath, name))
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

func (f *RealFS) Sub(path string) (FS, error) {
	_, err := os.Stat(filepath.Join(f.BasePath, path))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fs.ErrNotExist
	}
	if err != nil {
		return nil, WrapErrorf(err, "failed to stat directory %s", path)
	}
	return &RealFS{BasePath: filepath.Join(f.BasePath, path)}, nil
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

func (f *RealFS) Lock(ctx context.Context, path string) (unlock func() error, err error) {
	lock := NewLock(filepath.Join(f.BasePath, path))
	if err := lock.Lock(ctx); err != nil {
		return nil, err
	}
	return lock.Unlock, nil
}

func translateErrIsSymlink(op, name string, err error) error {
	if errors.Is(err, syscall.ELOOP) {
		return &fs.PathError{Op: op, Path: name, Err: ErrIsSymlink}
	}
	return err
}

func (f *RealFS) refuseSymlink(op, name string) error {
	info, err := os.Lstat(filepath.Join(f.BasePath, name))
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return &fs.PathError{Op: op, Path: name, Err: ErrIsSymlink}
	}
	return nil
}
