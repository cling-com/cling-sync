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
	// Create a sub directory, including any missing parents, and return a `FS` for it.
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

// MemoryFS is a complete in-memory file system modelled as a tree: each
// directory node holds its children by base name, so a node and its subtree
// list, move, and delete as a unit.

// memNode is a single tree node: a regular file, a directory, or a symlink.
type memNode struct {
	mode        fs.FileMode
	uid         uint32
	gid         uint32
	modTimeSec  int64
	modTimeNSec int32
	content     bytes.Buffer        // regular file data
	linkTarget  string              // symlink target
	children    map[string]*memNode // non-nil iff this is a directory
}

func newNode(mode fs.FileMode) *memNode {
	now := time.Now()
	n := &memNode{ //nolint:exhaustruct
		mode:        mode,
		uid:         1001,
		gid:         1000,
		modTimeSec:  now.Unix(),
		modTimeNSec: int32(now.Nanosecond()), //nolint:gosec
	}
	if mode.IsDir() {
		n.children = map[string]*memNode{}
	}
	return n
}

func (n *memNode) isDir() bool {
	return n.mode&fs.ModeDir != 0
}

func (n *memNode) isSymlink() bool {
	return n.mode&fs.ModeSymlink != 0
}

func (n *memNode) touch() {
	now := time.Now()
	n.modTimeSec = now.Unix()
	n.modTimeNSec = int32(now.Nanosecond()) //nolint:gosec
}

// info snapshots the node so the caller's reads can't race a later mutation.
func (n *memNode) info(name string) memFileInfo {
	size := int64(n.content.Len())
	if n.isSymlink() {
		size = int64(len(n.linkTarget))
	}
	return memFileInfo{
		name:        name,
		mode:        n.mode,
		size:        size,
		uid:         n.uid,
		gid:         n.gid,
		modTimeSec:  n.modTimeSec,
		modTimeNSec: n.modTimeNSec,
	}
}

type memFileInfo struct {
	name        string
	mode        fs.FileMode
	size        int64
	uid         uint32
	gid         uint32
	modTimeSec  int64
	modTimeNSec int32
}

func (i *memFileInfo) Name() string       { return i.name }
func (i *memFileInfo) Size() int64        { return i.size }
func (i *memFileInfo) Mode() fs.FileMode  { return i.mode }
func (i *memFileInfo) ModTime() time.Time { return time.Unix(i.modTimeSec, int64(i.modTimeNSec)) }
func (i *memFileInfo) IsDir() bool        { return i.mode.IsDir() }
func (i *memFileInfo) Type() fs.FileMode  { return i.mode.Type() }

func (i *memFileInfo) Sys() any {
	return &syscall.Stat_t{ //nolint:exhaustruct
		Uid: i.uid,
		Gid: i.gid,
	}
}

// This returns itself to make it compatible with `fs.DirEntry`.
func (i *memFileInfo) Info() (fs.FileInfo, error) {
	return i, nil
}

// Always returns an error on `Read`.
type errorReader struct {
	err error
}

func (r errorReader) Read(p []byte) (n int, err error) {
	return 0, r.err
}

type memoryFileWriter struct {
	*bytes.Buffer
	shared *memShared
	closed bool
}

func (w *memoryFileWriter) Write(p []byte) (n int, err error) {
	w.shared.mu.Lock()
	defer w.shared.mu.Unlock()
	if int64(w.Len()+len(p)) > w.shared.maxMemory {
		return 0, WrapErrorf(io.ErrShortWrite, "memory limit of %d bytes exceeded", w.shared.maxMemory)
	}
	return w.Buffer.Write(p)
}

func (w *memoryFileWriter) Sync() error {
	return nil
}

func (w *memoryFileWriter) Close() error {
	w.shared.mu.Lock()
	defer w.shared.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	w.shared.usedMemory += int64(w.Len())
	return nil
}

// memShared is the state shared by a MemoryFS and every view Sub/MkSub returns.
// The views differ only in their `base` prefix.
type memShared struct {
	mu         sync.Mutex
	root       *memNode
	locks      map[string]chan struct{}
	locksMutex sync.Mutex
	maxMemory  int64
	usedMemory int64
}

type MemoryFS struct {
	shared *memShared
	base   string
}

func NewMemoryFS(maxMemory int64) *MemoryFS {
	return &MemoryFS{
		shared: &memShared{ //nolint:exhaustruct
			root:      newNode(0o700 | fs.ModeDir),
			locks:     map[string]chan struct{}{},
			maxMemory: maxMemory,
		},
		base: ".",
	}
}

func (f *MemoryFS) OpenWrite(name string) (io.WriteCloser, error) {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	parent, leaf, err := f.shared.resolveParent(f.abs(name))
	if err != nil {
		return nil, err
	}
	if node, ok := parent.children[leaf]; ok {
		if node.isSymlink() {
			return nil, &fs.PathError{Op: "open", Path: name, Err: ErrIsSymlink}
		}
		if node.isDir() {
			return nil, &fs.PathError{Op: "open", Path: name, Err: syscall.EISDIR}
		}
		// Truncate in place so mode and ownership survive, like O_TRUNC.
		f.shared.usedMemory -= int64(node.content.Len())
		node.content.Reset()
		node.touch()
		return &memoryFileWriter{&node.content, f.shared, false}, nil
	}
	node := newNode(0o600)
	parent.children[leaf] = node
	return &memoryFileWriter{&node.content, f.shared, false}, nil
}

func (f *MemoryFS) OpenWriteExcl(name string) (io.WriteCloser, error) {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	parent, leaf, err := f.shared.resolveParent(f.abs(name))
	if err != nil {
		return nil, err
	}
	if node, ok := parent.children[leaf]; ok {
		if node.isSymlink() {
			return nil, &fs.PathError{Op: "open", Path: name, Err: ErrIsSymlink}
		}
		return nil, fs.ErrExist
	}
	node := newNode(0o600)
	parent.children[leaf] = node
	return &memoryFileWriter{&node.content, f.shared, false}, nil
}

func (f *MemoryFS) FSync(file io.WriteCloser) error {
	return nil
}

func (f *MemoryFS) FSyncDir(path string) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	_, err := f.shared.resolve(f.abs(path))
	return err
}

func (f *MemoryFS) OpenRead(name string) (io.ReadCloser, error) {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	node, err := f.shared.resolve(f.abs(name))
	if err != nil {
		return nil, err
	}
	if node.isSymlink() {
		return nil, &fs.PathError{Op: "open", Path: name, Err: ErrIsSymlink}
	}
	if node.isDir() {
		return io.NopCloser(errorReader{&fs.PathError{Op: "read", Path: name, Err: syscall.EISDIR}}), nil
	}
	data := make([]byte, node.content.Len())
	copy(data, node.content.Bytes())
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *MemoryFS) Chmod(name string, mode fs.FileMode) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	node, err := f.shared.resolve(f.abs(name))
	if err != nil {
		return err
	}
	if node.isSymlink() {
		return &fs.PathError{Op: "chmod", Path: name, Err: ErrIsSymlink}
	}
	node.mode = node.mode&^fs.ModePerm | mode&fs.ModePerm
	return nil
}

// `Chmtime` operates on the path's final component without following.
// Setting a symlink's own mtime is allowed (matches `lutimes` semantics).
func (f *MemoryFS) Chmtime(name string, mtime time.Time) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	node, err := f.shared.resolve(f.abs(name))
	if err != nil {
		return err
	}
	node.modTimeSec = mtime.Unix()
	node.modTimeNSec = int32(mtime.Nanosecond()) //nolint:gosec
	return nil
}

func (f *MemoryFS) Chown(name string, uid int, gid int) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	node, err := f.shared.resolve(f.abs(name))
	if err != nil {
		return err
	}
	if node.isSymlink() {
		return &fs.PathError{Op: "chown", Path: name, Err: ErrIsSymlink}
	}
	node.uid = uint32(uid) //nolint:gosec
	node.gid = uint32(gid) //nolint:gosec
	return nil
}

func (f *MemoryFS) Stat(name string) (fs.FileInfo, error) {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	abs := f.abs(name)
	node, err := f.shared.resolve(abs)
	if err != nil {
		return nil, err
	}
	info := node.info(abs)
	return &info, nil
}

func (f *MemoryFS) Symlink(target string, name string) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	parent, leaf, err := f.shared.resolveParent(f.abs(name))
	if err != nil {
		return err
	}
	if _, ok := parent.children[leaf]; ok {
		return fs.ErrExist
	}
	node := newNode(0o777 | fs.ModeSymlink)
	node.linkTarget = target
	parent.children[leaf] = node
	return nil
}

func (f *MemoryFS) ReadLink(name string) (string, error) {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	node, err := f.shared.resolve(f.abs(name))
	if err != nil {
		return "", err
	}
	if !node.isSymlink() {
		return "", fs.ErrInvalid
	}
	return node.linkTarget, nil
}

func (f *MemoryFS) ReadDir(name string) ([]fs.DirEntry, error) {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	node, err := f.shared.resolve(f.abs(name))
	if err != nil {
		return nil, err
	}
	if !node.isDir() {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: syscall.ENOTDIR}
	}
	entries := make([]fs.DirEntry, 0, len(node.children))
	for _, key := range sortedKeys(node.children) {
		info := node.children[key].info(key)
		entries = append(entries, &info)
	}
	return entries, nil
}

func (f *MemoryFS) Mkdir(name string) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	parent, leaf, err := f.shared.resolveParent(f.abs(name))
	if err != nil {
		return err
	}
	if _, ok := parent.children[leaf]; ok {
		return fs.ErrExist
	}
	parent.children[leaf] = newNode(0o700 | fs.ModeDir)
	return nil
}

func (f *MemoryFS) MkdirAll(path string) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	return f.shared.mkdirAllLocked(f.abs(path))
}

func (f *MemoryFS) Remove(name string) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	parent, leaf, err := f.shared.resolveParent(f.abs(name))
	if err != nil {
		return err
	}
	node, ok := parent.children[leaf]
	if !ok {
		return fs.ErrNotExist
	}
	if node.isDir() && len(node.children) > 0 {
		return &fs.PathError{Op: "remove", Path: name, Err: syscall.ENOTEMPTY}
	}
	f.shared.usedMemory -= int64(node.content.Len())
	delete(parent.children, leaf)
	return nil
}

func (f *MemoryFS) RemoveAll(path string) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	parent, leaf, err := f.shared.resolveParent(f.abs(path))
	if err != nil {
		// A missing or blocked parent means there is nothing to remove.
		return nil
	}
	node, ok := parent.children[leaf]
	if !ok {
		return nil
	}
	f.shared.usedMemory -= subtreeContentSize(node)
	delete(parent.children, leaf)
	return nil
}

func (f *MemoryFS) Rename(oldpath, newpath string) error {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	oldParent, oldLeaf, err := f.shared.resolveParent(f.abs(oldpath))
	if err != nil {
		return err
	}
	node, ok := oldParent.children[oldLeaf]
	if !ok {
		return fs.ErrNotExist
	}
	newParent, newLeaf, err := f.shared.resolveParent(f.abs(newpath))
	if err != nil {
		return err
	}
	if existing, ok := newParent.children[newLeaf]; ok {
		if existing.isDir() {
			return fs.ErrExist
		}
		f.shared.usedMemory -= int64(existing.content.Len())
	}
	delete(oldParent.children, oldLeaf)
	newParent.children[newLeaf] = node
	return nil
}

func (f *MemoryFS) Sub(path string) (FS, error) {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	abs := f.abs(path)
	if _, err := f.shared.resolve(abs); err != nil {
		return nil, err
	}
	return &MemoryFS{shared: f.shared, base: abs}, nil
}

func (f *MemoryFS) MkSub(path string) (FS, error) {
	f.shared.mu.Lock()
	defer f.shared.mu.Unlock()
	abs := f.abs(path)
	if err := f.shared.mkdirAllLocked(abs); err != nil {
		return nil, err
	}
	return &MemoryFS{shared: f.shared, base: abs}, nil
}

func (f *MemoryFS) WalkDir(path string, fn fs.WalkDirFunc) error {
	// Snapshot the matching entries under the lock so `fn` can safely call
	// back into the FS without deadlocking.
	type walkEntry struct {
		name string
		info memFileInfo
	}
	f.shared.mu.Lock()
	start, err := f.shared.resolve(f.abs(path))
	if err != nil {
		f.shared.mu.Unlock()
		return err
	}
	entries := []walkEntry{}
	var rec func(node *memNode, viewPath string)
	rec = func(node *memNode, viewPath string) {
		entries = append(entries, walkEntry{name: viewPath, info: node.info(viewPath)})
		if node.isDir() {
			for _, key := range sortedKeys(node.children) {
				rec(node.children[key], filepath.Join(viewPath, key))
			}
		}
	}
	rec(start, path)
	f.shared.mu.Unlock()
	skipDir := ""
	for i := range entries {
		e := &entries[i]
		if skipDir != "" && strings.HasPrefix(e.name, skipDir) {
			continue
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
	if f.base == "." {
		return "MemoryFS"
	}
	return "MemoryFS(" + f.base + ")"
}

// Lock is cancel-aware: a size-1 channel per path is the mutex (send to
// acquire, receive to release), so a blocked acquire can lose to ctx.Done().
func (f *MemoryFS) Lock(ctx context.Context, path string) (func() error, error) {
	abs := f.abs(path)
	f.shared.locksMutex.Lock()
	ch, existed := f.shared.locks[abs]
	if !existed {
		ch = make(chan struct{}, 1)
		f.shared.locks[abs] = ch
	}
	f.shared.locksMutex.Unlock()

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

	// A cancelled ctx must win even when the lock is free. The select below
	// would otherwise pick a ready case at random.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case ch <- struct{}{}:
		unlocked := false
		return func() error {
			if !unlocked {
				unlocked = true
				<-ch
			}
			return nil
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *MemoryFS) abs(name string) string {
	return filepath.Join(f.base, name)
}

// resolve returns the node at `abs`. A missing component yields fs.ErrNotExist,
// descending through a non-directory yields ENOTDIR. Caller must hold s.mu.
func (s *memShared) resolve(abs string) (*memNode, error) {
	node := s.root
	for _, seg := range splitPath(abs) {
		if !node.isDir() {
			return nil, syscall.ENOTDIR
		}
		child, ok := node.children[seg]
		if !ok {
			return nil, fs.ErrNotExist
		}
		node = child
	}
	return node, nil
}

// resolveParent returns the parent directory node of `abs` and the final path
// component. Caller must hold s.mu.
func (s *memShared) resolveParent(abs string) (*memNode, string, error) {
	parent, err := s.resolve(filepath.Dir(abs))
	if err != nil {
		return nil, "", err
	}
	if !parent.isDir() {
		return nil, "", syscall.ENOTDIR
	}
	return parent, filepath.Base(abs), nil
}

// mkdirAllLocked mirrors os.MkdirAll: create every missing component, no error
// if the directory already exists, ENOTDIR if a component is a file. Caller
// must hold s.mu.
func (s *memShared) mkdirAllLocked(abs string) error {
	node := s.root
	for _, seg := range splitPath(abs) {
		child, ok := node.children[seg]
		switch {
		case !ok:
			child = newNode(0o700 | fs.ModeDir)
			node.children[seg] = child
		case !child.isDir():
			return &fs.PathError{Op: "mkdir", Path: seg, Err: syscall.ENOTDIR}
		}
		node = child
	}
	return nil
}

// splitPath turns a tree-relative path into its components. "." and "" are the
// root and yield no components.
func splitPath(abs string) []string {
	if abs == "." || abs == "" {
		return nil
	}
	return strings.Split(abs, "/")
}

func sortedKeys(m map[string]*memNode) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func subtreeContentSize(node *memNode) int64 {
	total := int64(node.content.Len())
	for _, child := range node.children {
		total += subtreeContentSize(child)
	}
	return total
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
