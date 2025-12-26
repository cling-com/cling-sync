//nolint:wrapcheck,forbidigo
package lib

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const PathSeparator = string(os.PathSeparator)

// A file system abstraction that only provides what is actually needed.
//
// About file modes:
// `OpenWrite*` and `Mkdir*` don't take a `fs.FileMode` argument, because the effective
// mode is determined by the umask. So we are better off setting the mode explicitly.
// `RealFS` uses sensible defaults (0o600 and 0o700) in `OpenWrite*` and `Mkdir*`.
type FS interface {
	// The file is always fully overwritten.
	OpenWrite(name string) (io.WriteCloser, error)
	// Return `fs.ErrExist` if the file already exists.
	OpenWriteExcl(name string) (io.WriteCloser, error)
	FSync(file io.WriteCloser) error
	FSyncDir(path string) error
	OpenRead(name string) (io.ReadCloser, error)
	Chmod(name string, mode fs.FileMode) error
	Chmtime(name string, mtime time.Time) error
	Chown(name string, uid int, gid int) error
	Stat(name string) (fs.FileInfo, error)
	ReadDir(name string) ([]fs.DirEntry, error)
	Mkdir(name string) error
	MkdirAll(path string) error
	Remove(name string) error
	RemoveAll(path string) error
	Rename(oldpath, newpath string) error
	// Create a sub directory and return a `FS` for it. Return `fs.ErrExist` if the directory already exists.
	MkSub(path string) (FS, error)
	// Return a `FS` for the sub directory. Return `fs.ErrNotExist` if the directory does not exist.
	Sub(path string) (FS, error)
	// `fn` is called with a path relative to the root of the FS.
	WalkDir(path string, fn fs.WalkDirFunc) error
	String() string
	// Create the lock file if it does not exist and places a lock on it.
	// Use this to synchronize inter-process access to any resource.
	// The file is not deleted when the lock is released.
	Lock(ctx context.Context, path string) (unlock func() error, err error)
}

type memoryFileWriter struct {
	*bytes.Buffer
	fs     *MemoryFS
	closed bool
}

func (w *memoryFileWriter) Write(p []byte) (n int, err error) {
	if int64(w.Len()+len(p)) > w.fs.maxMemory {
		return 0, WrapErrorf(io.ErrShortWrite, "memory limit of %d bytes exceeded", w.fs.maxMemory)
	}
	return w.Buffer.Write(p)
}

func (w *memoryFileWriter) Sync() error {
	return nil
}

func (w *memoryFileWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.fs.usedMemory += int64(w.Len())
	return nil
}

// Always returns an error on `Read`.
type errorReader struct {
	err error
}

func (r errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

type MemoryFileInfo struct {
	name        string
	mode        fs.FileMode
	gid         uint32
	uid         uint32
	modTimeSec  int64
	modTimeNSec int32
	content     bytes.Buffer
}

func (f *MemoryFileInfo) Name() string {
	return f.name
}

func (f *MemoryFileInfo) Size() int64 {
	return int64(f.content.Len())
}

func (f *MemoryFileInfo) Mode() fs.FileMode {
	return f.mode
}

func (f *MemoryFileInfo) ModTime() time.Time {
	return time.Unix(f.modTimeSec, int64(f.modTimeNSec))
}

func (f *MemoryFileInfo) IsDir() bool {
	return f.mode.IsDir()
}

func (f *MemoryFileInfo) Sys() any {
	return &syscall.Stat_t{ //nolint:exhaustruct
		Uid: f.uid,
		Gid: f.gid,
	}
}

// This returns itself to make it compatible with `fs.DirEntry`.
func (f *MemoryFileInfo) Info() (fs.FileInfo, error) {
	return f, nil
}

func (f *MemoryFileInfo) Type() fs.FileMode {
	return f.mode.Type()
}

type MemoryFS struct {
	files      map[string]*MemoryFileInfo
	locks      map[string]*sync.Mutex
	locksMutex sync.Mutex
	maxMemory  int64
	usedMemory int64
}

func NewMemoryFS(maxMemory int64) *MemoryFS {
	f := &MemoryFS{make(map[string]*MemoryFileInfo), make(map[string]*sync.Mutex), sync.Mutex{}, maxMemory, 0}
	f.create(".", 0o700|os.ModeDir)
	return f
}

func (f *MemoryFS) OpenWrite(name string) (io.WriteCloser, error) {
	if file, ok := f.files[name]; ok && file.mode.IsDir() {
		return nil, &fs.PathError{Op: "open", Path: name, Err: syscall.EISDIR}
	}
	file := f.create(name, 0o600)
	return &memoryFileWriter{&file.content, f, false}, nil
}

func (f *MemoryFS) OpenWriteExcl(name string) (io.WriteCloser, error) {
	if _, ok := f.files[name]; ok {
		return nil, fs.ErrExist
	}
	return f.OpenWrite(name)
}

func (f *MemoryFS) FSync(file io.WriteCloser) error {
	return nil
}

func (f *MemoryFS) FSyncDir(path string) error {
	return nil
}

func (f *MemoryFS) OpenRead(name string) (io.ReadCloser, error) {
	file, ok := f.files[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	if file.mode.IsDir() {
		return io.NopCloser(errorReader{&fs.PathError{Op: "read", Path: name, Err: syscall.EISDIR}}), nil
	}
	return io.NopCloser(bytes.NewReader(file.content.Bytes())), nil
}

func (f *MemoryFS) Chmod(name string, mode fs.FileMode) error {
	file, ok := f.files[name]
	if !ok {
		return fs.ErrNotExist
	}
	// Only update the permission bits.
	file.mode &= ^fs.ModePerm
	file.mode |= mode & fs.ModePerm
	return nil
}

func (f *MemoryFS) Chmtime(name string, mtime time.Time) error {
	file, ok := f.files[name]
	if !ok {
		return fs.ErrNotExist
	}
	file.modTimeSec = mtime.Unix()
	file.modTimeNSec = int32(mtime.Nanosecond()) //nolint:gosec
	return nil
}

func (f *MemoryFS) Chown(name string, uid int, gid int) error {
	file, ok := f.files[name]
	if !ok {
		return fs.ErrNotExist
	}
	file.uid = uint32(uid) //nolint:gosec
	file.gid = uint32(gid) //nolint:gosec
	return nil
}

func (f *MemoryFS) Stat(name string) (fs.FileInfo, error) {
	file, ok := f.files[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return file, nil
}

func (f *MemoryFS) ReadDir(path string) ([]fs.DirEntry, error) {
	file, ok := f.files[path]
	if !ok {
		return nil, fs.ErrNotExist
	}
	if !file.mode.IsDir() {
		return nil, fs.ErrInvalid
	}
	entries := []fs.DirEntry{}
	for name, file := range f.files {
		if filepath.Dir(name) == path && name != path {
			entry := file
			entry.name = strings.TrimPrefix(name, path+"/")
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func (f *MemoryFS) Mkdir(path string) error {
	if _, ok := f.files[path]; ok {
		return fs.ErrExist
	}
	for {
		f.create(path, 0o700|os.ModeDir)
		path = filepath.Dir(path)
		if path == "." {
			break
		}
		if _, ok := f.files[path]; !ok {
			return fs.ErrNotExist
		}
	}
	f.create(path, 0o700|os.ModeDir)
	return nil
}

func (f *MemoryFS) MkdirAll(path string) error {
	for path != "." {
		if file, ok := f.files[path]; ok && file.mode.IsDir() {
			file.mode |= 0o700
			return nil
		}
		f.create(path, 0o700|os.ModeDir)
		path = filepath.Dir(path)
	}
	return nil
}

func (f *MemoryFS) Remove(name string) error {
	file, ok := f.files[name]
	if !ok {
		return fs.ErrNotExist
	}
	if file.mode.IsDir() {
		isEmpty := true
		for _, file := range f.files {
			if strings.HasPrefix(file.name, name+"/") {
				isEmpty = false
				break
			}
		}
		if !isEmpty {
			return &fs.PathError{Op: "remove", Path: name, Err: syscall.ENOTEMPTY}
		}
	}
	f.usedMemory -= int64(file.content.Len())
	delete(f.files, name)
	return nil
}

func (f *MemoryFS) RemoveAll(path string) error {
	toDelete := []string{}
	for _, file := range f.files {
		if strings.HasPrefix(file.name, path) {
			toDelete = append(toDelete, file.name)
		}
	}
	for _, name := range toDelete {
		f.usedMemory -= int64(f.files[name].content.Len())
		delete(f.files, name)
	}
	return nil
}

func (f *MemoryFS) Rename(oldpath, newpath string) error {
	oldFile, ok := f.files[oldpath]
	if !ok {
		return fs.ErrNotExist
	}
	newFile, ok := f.files[newpath]
	if ok {
		if newFile.mode.IsDir() {
			return fs.ErrExist
		}
		f.usedMemory -= int64(newFile.content.Len())
	}
	delete(f.files, newpath)
	delete(f.files, oldpath)
	oldFile.name = newpath
	f.files[newpath] = oldFile
	return nil
}

func (f *MemoryFS) Sub(path string) (FS, error) {
	if _, ok := f.files[path]; !ok {
		return nil, fs.ErrNotExist
	}
	return &subMemoryFS{f, path}, nil
}

func (f *MemoryFS) MkSub(path string) (FS, error) {
	if _, ok := f.files[path]; ok {
		return nil, fs.ErrExist
	}
	f.create(path, 0o700|os.ModeDir)
	return &subMemoryFS{f, path}, nil
}

func (f *MemoryFS) WalkDir(path string, fn fs.WalkDirFunc) error {
	if path == "." {
		path = ""
	}
	var names []string //nolint:prealloc
	for name := range f.files {
		if !strings.HasPrefix(name, path) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	skipDir := ""
	for _, name := range names {
		d := f.files[name]
		if skipDir != "" {
			if strings.HasPrefix(name, skipDir) {
				continue
			}
		}
		if err := fn(name, d, nil); err != nil {
			if errors.Is(err, fs.SkipDir) {
				skipDir = name + "/"
			} else {
				return err
			}
		}
	}
	return nil
}

func (f *MemoryFS) String() string {
	return "MemoryFS"
}

// This uses a sync.Mutex because MemoryFS can only be used in a single process.
func (f *MemoryFS) Lock(ctx context.Context, path string) (func() error, error) {
	if _, ok := f.locks[path]; !ok {
		f, err := f.OpenWriteExcl(path)
		if err != nil {
			return nil, WrapErrorf(err, "failed to create lock file %s", path)
		}
		if _, err := f.Write([]byte(time.Now().Format(time.RFC3339Nano) + "\n")); err != nil {
			return nil, WrapErrorf(err, "failed to write lock file %s", path)
		}
		if err := f.Close(); err != nil {
			return nil, WrapErrorf(err, "failed to close lock file %s", path)
		}
	}
	f.locksMutex.Lock()
	m := f.locks[path]
	if m == nil {
		m = new(sync.Mutex)
		f.locks[path] = m
	}
	f.locksMutex.Unlock()

	locked := make(chan struct{}, 1)
	go func() {
		m.Lock()
		locked <- struct{}{}
	}()

	select {
	case <-locked:
		unlocked := false
		return func() error {
			if unlocked {
				return nil
			}
			unlocked = true
			m.Unlock()
			return nil
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *MemoryFS) Debug() string {
	var sb strings.Builder
	sb.WriteString("MemoryFS(")
	for _, file := range f.files {
		sb.WriteString(file.name + "\n")
	}
	sb.WriteString(")")
	return sb.String()
}

type subMemoryFS struct {
	parent *MemoryFS
	path   string
}

func (f *subMemoryFS) OpenWrite(name string) (io.WriteCloser, error) {
	return f.parent.OpenWrite(filepath.Join(f.path, name))
}

func (f *subMemoryFS) OpenWriteExcl(name string) (io.WriteCloser, error) {
	return f.parent.OpenWriteExcl(filepath.Join(f.path, name))
}

func (f *subMemoryFS) FSync(file io.WriteCloser) error {
	return f.parent.FSync(file)
}

func (f *subMemoryFS) FSyncDir(path string) error {
	return f.parent.FSyncDir(path)
}

func (f *subMemoryFS) OpenRead(name string) (io.ReadCloser, error) {
	return f.parent.OpenRead(filepath.Join(f.path, name))
}

func (f *subMemoryFS) Chmod(name string, mode fs.FileMode) error {
	return f.parent.Chmod(filepath.Join(f.path, name), mode)
}

func (f *subMemoryFS) Chmtime(name string, mtime time.Time) error {
	return f.parent.Chmtime(filepath.Join(f.path, name), mtime)
}

func (f *subMemoryFS) Chown(name string, uid int, gid int) error {
	return f.parent.Chown(filepath.Join(f.path, name), uid, gid)
}

func (f *subMemoryFS) Stat(name string) (fs.FileInfo, error) {
	return f.parent.Stat(filepath.Join(f.path, name))
}

func (f *subMemoryFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return f.parent.ReadDir(filepath.Join(f.path, name))
}

func (f *subMemoryFS) Mkdir(name string) error {
	return f.parent.Mkdir(filepath.Join(f.path, name))
}

func (f *subMemoryFS) MkdirAll(path string) error {
	return f.parent.MkdirAll(filepath.Join(f.path, path))
}

func (f *subMemoryFS) Remove(name string) error {
	return f.parent.Remove(filepath.Join(f.path, name))
}

func (f *subMemoryFS) RemoveAll(path string) error {
	return f.parent.RemoveAll(filepath.Join(f.path, path))
}

func (f *subMemoryFS) Rename(oldpath, newpath string) error {
	return f.parent.Rename(filepath.Join(f.path, oldpath), filepath.Join(f.path, newpath))
}

func (f *subMemoryFS) MkSub(path string) (FS, error) {
	return f.parent.MkSub(filepath.Join(f.path, path))
}

func (f *subMemoryFS) Sub(path string) (FS, error) {
	return f.parent.Sub(filepath.Join(f.path, path))
}

func (f *subMemoryFS) WalkDir(path string, fn fs.WalkDirFunc) error {
	return f.parent.WalkDir(filepath.Join(f.path, path), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(f.path, path)
		if err != nil {
			return err
		}
		return fn(relPath, d, err)
	})
}

func (f *subMemoryFS) String() string {
	return "MemoryFS(" + f.path + ")"
}

func (f *subMemoryFS) Lock(ctx context.Context, path string) (unlock func() error, err error) {
	return f.parent.Lock(ctx, filepath.Join(f.path, path))
}

func ReadFile(fs FS, name string) ([]byte, error) {
	f, err := fs.OpenRead(name)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck
	return io.ReadAll(f)
}

func WriteFile(fs FS, name string, data []byte) error {
	f, err := fs.OpenWrite(name)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	defer f.Close() //nolint:errcheck
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return err
}

func AtomicWriteTempFilename(name string) string {
	return filepath.Join(
		filepath.Dir(name),
		".cling_sync_tmp_"+filepath.Base(name)+"."+strconv.FormatInt(time.Now().UnixNano(), 16),
	)
}

func IsAtomicWriteTempFile(name string) bool {
	return strings.HasPrefix(filepath.Base(name), ".cling_sync_tmp_")
}

// Try to write the data to the target file in the most safe way possible:
//   - Write the data to a temporary file.
//   - fsync the temporary file.
//   - Set the permissions of the temporary file.
//   - Rename the temporary file to the target file.
//   - fsync the parent directory of the target file.
//
// In case of an error, the temporary file is deleted.
func AtomicWriteFile(fs FS, name string, perm fs.FileMode, data ...[]byte) error { //nolint:funlen
	tmpPath := AtomicWriteTempFilename(name)
	f, err := fs.OpenWrite(tmpPath)
	if err != nil {
		return err
	}
	for _, d := range data {
		if _, err := f.Write(d); err != nil {
			_ = f.Close()
			if err := fs.Remove(tmpPath); err != nil {
				return WrapErrorf(
					err,
					"failed to write data and failed to remove temporary file %s (it is garbage now)",
					tmpPath,
				)
			}
			return err
		}
	}
	if err := fs.FSync(f); err != nil {
		_ = f.Close()
		if err := fs.Remove(tmpPath); err != nil {
			return WrapErrorf(
				err,
				"failed to fsync temporary file %s and failed to remove it (it is garbage now)",
				tmpPath,
			)
		}
		return WrapErrorf(err, "failed to fsync temporary file %s", tmpPath)
	}
	if err := f.Close(); err != nil {
		if err := fs.Remove(tmpPath); err != nil {
			return WrapErrorf(
				err,
				"failed to close temporary file %s and failed to remove it (it is garbage now)",
				tmpPath,
			)
		}
		return WrapErrorf(err, "failed to close temporary file %s", tmpPath)
	}
	// Set the permissions.
	if err := fs.Chmod(tmpPath, perm); err != nil {
		return WrapErrorf(err, "failed to change permissions of %s", tmpPath)
	}
	// Rename the temporary file to the target file.
	if err := fs.Rename(tmpPath, name); err != nil {
		if err := fs.Remove(tmpPath); err != nil {
			return WrapErrorf(
				err,
				"failed to rename temporary file %s to %s and failed to remove temporary file (it is garbage now)",
				tmpPath,
				name,
			)
		}
		return WrapErrorf(err, "failed to rename temporary file %s to %s", tmpPath, name)
	}
	// fsync the parent directory to make sure the rename is durable.
	if err := fs.FSyncDir(filepath.Dir(name)); err != nil {
		_ = f.Close()
		if err := fs.Remove(tmpPath); err != nil {
			return WrapErrorf(
				err,
				"failed to fsync parent directory of temporary file %s and failed to remove it (it is garbage now)",
				tmpPath,
			)
		}
		return WrapErrorf(err, "failed to fsync parent directory of temporary file %s", tmpPath)
	}
	return nil
}

func (f *MemoryFS) create(path string, mode fs.FileMode) *MemoryFileInfo {
	mtime := time.Now()
	file := &MemoryFileInfo{
		path,
		mode,
		1000,
		1001,
		mtime.Unix(),
		int32(mtime.Nanosecond()), //nolint:gosec
		bytes.Buffer{},
	}
	f.files[path] = file
	return file
}
