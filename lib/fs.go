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
	"sync/atomic"
	"syscall"
	"time"
)

const PathSeparator = string(os.PathSeparator)

var ErrIsSymlink = errors.New("path is a symlink")

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
	Symlink(target string, name string) error
	ReadLink(name string) (string, error)
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
	w.fs.mu.Lock()
	defer w.fs.mu.Unlock()
	if int64(w.Len()+len(p)) > w.fs.maxMemory {
		return 0, WrapErrorf(io.ErrShortWrite, "memory limit of %d bytes exceeded", w.fs.maxMemory)
	}
	return w.Buffer.Write(p)
}

func (w *memoryFileWriter) Sync() error {
	return nil
}

func (w *memoryFileWriter) Close() error {
	w.fs.mu.Lock()
	defer w.fs.mu.Unlock()
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
	linkTarget  string
}

func (f *MemoryFileInfo) Name() string {
	return f.name
}

func (f *MemoryFileInfo) Size() int64 {
	if f.mode&fs.ModeSymlink != 0 {
		return int64(len(f.linkTarget))
	}
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

// MemoryFS matches RealFS thread-safety guarantees: every method is safe to
// call concurrently from multiple goroutines. A single mutex serializes all
// access to `files` and `usedMemory`; the per-path Lock map has its own
// mutex so locking doesn't contend with FS operations.
type MemoryFS struct {
	mu         sync.Mutex
	files      map[string]*MemoryFileInfo
	locks      map[string]chan struct{}
	locksMutex sync.Mutex
	maxMemory  int64
	usedMemory int64
}

func NewMemoryFS(maxMemory int64) *MemoryFS {
	f := &MemoryFS{
		mu:         sync.Mutex{},
		files:      make(map[string]*MemoryFileInfo),
		locks:      make(map[string]chan struct{}),
		locksMutex: sync.Mutex{},
		maxMemory:  maxMemory,
		usedMemory: 0,
	}
	f.create(".", 0o700|os.ModeDir)
	return f
}

func (f *MemoryFS) OpenWrite(name string) (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if file, ok := f.files[name]; ok && file.mode&fs.ModeSymlink != 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: ErrIsSymlink}
	}
	return f.openWriteLocked(name)
}

func (f *MemoryFS) OpenWriteExcl(name string) (io.WriteCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if file, ok := f.files[name]; ok {
		if file.mode&fs.ModeSymlink != 0 {
			return nil, &fs.PathError{Op: "open", Path: name, Err: ErrIsSymlink}
		}
		return nil, fs.ErrExist
	}
	return f.openWriteLocked(name)
}

func (f *MemoryFS) FSync(file io.WriteCloser) error {
	return nil
}

func (f *MemoryFS) FSyncDir(path string) error {
	return nil
}

func (f *MemoryFS) OpenRead(name string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	if file.mode&fs.ModeSymlink != 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: ErrIsSymlink}
	}
	if file.mode.IsDir() {
		return io.NopCloser(errorReader{&fs.PathError{Op: "read", Path: name, Err: syscall.EISDIR}}), nil
	}
	src := file.content.Bytes()
	data := make([]byte, len(src))
	copy(data, src)
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *MemoryFS) Chmod(name string, mode fs.FileMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[name]
	if !ok {
		return fs.ErrNotExist
	}
	if file.mode&fs.ModeSymlink != 0 {
		return &fs.PathError{Op: "chmod", Path: name, Err: ErrIsSymlink}
	}
	file.mode &= ^fs.ModePerm
	file.mode |= mode & fs.ModePerm
	return nil
}

// `Chmtime` operates on the path's final component without following.
// Setting a symlink's own mtime is allowed (matches `lutimes` semantics).
func (f *MemoryFS) Chmtime(name string, mtime time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[name]
	if !ok {
		return fs.ErrNotExist
	}
	file.modTimeSec = mtime.Unix()
	file.modTimeNSec = int32(mtime.Nanosecond()) //nolint:gosec
	return nil
}

func (f *MemoryFS) Chown(name string, uid int, gid int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[name]
	if !ok {
		return fs.ErrNotExist
	}
	if file.mode&fs.ModeSymlink != 0 {
		return &fs.PathError{Op: "chown", Path: name, Err: ErrIsSymlink}
	}
	file.uid = uint32(uid) //nolint:gosec
	file.gid = uint32(gid) //nolint:gosec
	return nil
}

func (f *MemoryFS) Stat(name string) (fs.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[name]
	if !ok {
		return nil, fs.ErrNotExist
	}
	// Return a value-copy so concurrent mutation of the live struct does
	// not race with the caller's reads.
	snapshot := *file
	return &snapshot, nil
}

func (f *MemoryFS) Symlink(target string, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.files[name]; ok {
		return fs.ErrExist
	}
	file := f.create(name, 0o777|fs.ModeSymlink)
	file.linkTarget = target
	return nil
}

func (f *MemoryFS) ReadLink(name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	file, ok := f.files[name]
	if !ok {
		return "", fs.ErrNotExist
	}
	if file.mode&fs.ModeSymlink == 0 {
		return "", fs.ErrInvalid
	}
	return file.linkTarget, nil
}

func (f *MemoryFS) ReadDir(path string) ([]fs.DirEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
			entry := *file
			entry.name = strings.TrimPrefix(name, path+"/")
			entries = append(entries, &entry)
		}
	}
	return entries, nil
}

func (f *MemoryFS) Mkdir(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.files[path]; ok {
		return fs.ErrExist
	}
	for {
		parent := filepath.Dir(path)
		if parent == "." {
			break
		}
		if _, ok := f.files[parent]; !ok {
			return fs.ErrNotExist
		}
		f.create(path, 0o700|os.ModeDir)
		path = parent
	}
	f.create(path, 0o700|os.ModeDir)
	return nil
}

func (f *MemoryFS) MkdirAll(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
	toDelete := []string{}
	for _, file := range f.files {
		if strings.HasPrefix(file.name, path+"/") || file.name == path {
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
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.files[path]; !ok {
		return nil, fs.ErrNotExist
	}
	return &subMemoryFS{f, path}, nil
}

func (f *MemoryFS) MkSub(path string) (FS, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	// Snapshot the matching entries under the lock so `fn` can safely call
	// back into the FS without deadlocking.
	type walkEntry struct {
		name string
		info MemoryFileInfo
	}
	f.mu.Lock()
	entries := make([]walkEntry, 0, len(f.files))
	for name, file := range f.files {
		if !strings.HasPrefix(name, path) {
			continue
		}
		entries = append(entries, walkEntry{name: name, info: *file})
	}
	f.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
	skipDir := ""
	for i := range entries {
		e := &entries[i]
		if skipDir != "" {
			if strings.HasPrefix(e.name, skipDir) {
				continue
			}
		}
		if err := fn(e.name, &e.info, nil); err != nil {
			switch {
			case errors.Is(err, fs.SkipAll):
				return nil
			case errors.Is(err, fs.SkipDir):
				skipDir = e.name + "/"
			default:
				return err
			}
		}
	}
	return nil
}

func (f *MemoryFS) String() string {
	return "MemoryFS"
}

// A buffered channel of size 1 acts as the per-path mutex: sending claims it,
// receiving releases it. Selecting the send against ctx.Done() makes the
// acquisition itself cancel-aware, so a bailed-out caller can't leave the
// lock held by a stranded goroutine.
func (f *MemoryFS) Lock(ctx context.Context, path string) (func() error, error) {
	f.locksMutex.Lock()
	ch, existed := f.locks[path]
	if !existed {
		ch = make(chan struct{}, 1)
		f.locks[path] = ch
	}
	f.locksMutex.Unlock()

	if !existed {
		lf, err := f.OpenWriteExcl(path)
		if err != nil {
			return nil, WrapErrorf(err, "failed to create lock file %s", path)
		}
		if _, err := lf.Write([]byte(time.Now().Format(time.RFC3339Nano) + "\n")); err != nil {
			return nil, WrapErrorf(err, "failed to write lock file %s", path)
		}
		if err := lf.Close(); err != nil {
			return nil, WrapErrorf(err, "failed to close lock file %s", path)
		}
	}

	// Honor an already-cancelled context before attempting the send: with
	// both cases ready, the select below would pick at random.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case ch <- struct{}{}:
		unlocked := false
		return func() error {
			if unlocked {
				return nil
			}
			unlocked = true
			<-ch
			return nil
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *MemoryFS) Debug() string {
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *subMemoryFS) Symlink(target string, name string) error {
	return f.parent.Symlink(target, filepath.Join(f.path, name))
}

func (f *subMemoryFS) ReadLink(name string) (string, error) {
	return f.parent.ReadLink(filepath.Join(f.path, name))
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

// atomicWriteSeq makes temp filenames unique. Seeding it with the startup time
// keeps separate processes writing the same target from colliding; the atomic
// increment keeps concurrent goroutines within one process from colliding too.
var atomicWriteSeq = time.Now().UnixNano() //nolint:gochecknoglobals

func AtomicWriteTempFilename(name string) string {
	seq := atomic.AddInt64(&atomicWriteSeq, 1)
	return filepath.Join(
		filepath.Dir(name),
		".cling_sync_tmp_"+filepath.Base(name)+"."+strconv.FormatInt(seq, 16),
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
	file := &MemoryFileInfo{ //nolint:exhaustruct
		name:        path,
		mode:        mode,
		gid:         1000,
		uid:         1001,
		modTimeSec:  mtime.Unix(),
		modTimeNSec: int32(mtime.Nanosecond()), //nolint:gosec
	}
	f.files[path] = file
	return file
}

// Caller must hold f.mu.
func (f *MemoryFS) openWriteLocked(name string) (io.WriteCloser, error) {
	if file, ok := f.files[name]; ok && file.mode.IsDir() {
		return nil, &fs.PathError{Op: "open", Path: name, Err: syscall.EISDIR}
	}
	file := f.create(name, 0o600)
	return &memoryFileWriter{&file.content, f, false}, nil
}
