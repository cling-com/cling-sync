package lib

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestRealFS(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		// Verify that the real underlying file system is used.
		t.Parallel()
		assert := NewAssert(t)
		sut := td.NewRealFS(t)
		f, err := sut.OpenWrite("a.txt")
		assert.NoError(err)
		_, err = f.Write([]byte("abcd"))
		assert.NoError(err)
		err = f.Close()
		assert.NoError(err)
		actual, _ := os.ReadFile(filepath.Join(sut.BasePath, "a.txt")) //nolint:forbidigo
		assert.Equal("abcd", string(actual))

		stat, err := os.Stat(filepath.Join(sut.BasePath, "a.txt")) //nolint:forbidigo
		assert.NoError(err)
		assert.Equal(int64(4), stat.Size())
		assert.Equal(fs.FileMode(0o600), stat.Mode().Perm())
	})

	t.Run("MkSub should create a real sub directory in the underlying file system", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := td.NewRealFS(t)

		sub, err := sut.MkSub("a")
		assert.NoError(err)
		subRealFS, ok := sub.(*RealFS)
		assert.Equal(true, ok)
		assert.Equal(filepath.Join(sut.BasePath, "a"), subRealFS.BasePath)
		stat, err := os.Stat(filepath.Join(sut.BasePath, "a")) //nolint:forbidigo
		assert.NoError(err)
		assert.Equal(true, stat.IsDir())
	})

	checkConsistency(t, func() FS {
		return td.NewRealFS(t)
	})
}

func TestMemoryFS(t *testing.T) {
	t.Parallel()

	t.Run("Memory limit is enforced", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewMemoryFS(15)

		w, err := sut.OpenWrite("a.txt")
		assert.NoError(err)
		written, err := w.Write([]byte("1234567890"))
		assert.NoError(err)
		assert.Equal(10, written)
		_, err = w.Write([]byte("1234567890"))
		assert.ErrorIs(err, io.ErrShortWrite)
		assert.Error(err, "memory limit of 15 bytes exceeded")
	})

	t.Run("Remove frees memory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewMemoryFS(20)

		writeFile(t, sut, "a.txt", "1234567890")
		assert.Equal(int64(10), sut.usedMemory)

		writeFile(t, sut, "b.txt", "12345")
		assert.Equal(int64(15), sut.usedMemory)

		err := sut.Remove("a.txt")
		assert.NoError(err)
		assert.Equal(int64(5), sut.usedMemory)
	})

	t.Run("RemoveAll frees memory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewMemoryFS(20)

		assert.NoError(sut.Mkdir("a"))
		writeFile(t, sut, "a/b.txt", "1234567890")
		writeFile(t, sut, "a/c.txt", "12345")
		writeFile(t, sut, "d.txt", "123")
		assert.Equal(int64(18), sut.usedMemory)

		err := sut.RemoveAll("a")
		assert.NoError(err)
		assert.Equal(int64(3), sut.usedMemory)
	})

	t.Run("RemoveAll should not delete entries that merely share a prefix", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewMemoryFS(10000000)

		assert.NoError(sut.Mkdir("foo"))
		writeFile(t, sut, "foo/a.txt", "a")
		assert.NoError(sut.Mkdir("foobar"))
		writeFile(t, sut, "foobar/b.txt", "b")

		err := sut.RemoveAll("foo")
		assert.NoError(err)

		// "foobar" and its contents should not have been deleted.
		_, err = sut.Stat("foobar")
		assert.NoError(err)
		data, err := ReadFile(sut, "foobar/b.txt")
		assert.NoError(err)
		assert.Equal("b", string(data))
	})

	t.Run("Mkdir should not create orphaned entries on failure", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewMemoryFS(10000000)

		// "a" exists but "a/b" does not, so Mkdir("a/b/c") should fail.
		assert.NoError(sut.Mkdir("a"))
		err := sut.Mkdir("a/b/c")
		assert.ErrorIs(err, fs.ErrNotExist)

		// "a/b/c" should not exist.
		_, err = sut.Stat("a/b/c")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("ReadDir must not corrupt internal file names", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewMemoryFS(10000000)

		assert.NoError(sut.Mkdir("a"))
		writeFile(t, sut, "a/b.txt", "data")

		// ReadDir returns entries with basenames, but must not mutate the
		// internal full-path names stored in the FS.
		_, err := sut.ReadDir("a")
		assert.NoError(err)

		// After ReadDir, the file should still be accessible by its full path.
		stat, err := sut.Stat("a/b.txt")
		assert.NoError(err)
		assert.Equal("a/b.txt", stat.Name())
	})

	t.Run("Rename frees memory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewMemoryFS(20)

		writeFile(t, sut, "a.txt", "1234567890")
		assert.NoError(sut.Rename("a.txt", "b.txt"))
		assert.Equal(int64(10), sut.usedMemory)

		writeFile(t, sut, "c.txt", "12345")
		assert.Equal(int64(15), sut.usedMemory)
		assert.NoError(sut.Rename("b.txt", "c.txt"))
		assert.Equal(int64(10), sut.usedMemory)
	})

	t.Run("Concurrent FS operations are race-free", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewMemoryFS(10000000)
		assert.NoError(sut.Mkdir("d"))

		// Hammer the FS from many goroutines doing every kind of operation
		// against an overlapping set of files. We don't care which call
		// happens to win a given race — only that the FS does not corrupt
		// itself. `go test -race` flags any unsynchronized access.
		const workers = 16
		const iters = 200
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := range workers {
			go func(w int) {
				defer wg.Done()
				for i := range iters {
					name := fmt.Sprintf("d/f-%d.txt", (w+i)%4)
					switch i % 7 {
					case 0:
						_ = WriteFile(sut, name, []byte("data"))
					case 1:
						_, _ = ReadFile(sut, name)
					case 2:
						_, _ = sut.Stat(name)
					case 3:
						_, _ = sut.ReadDir("d")
					case 4:
						_ = sut.Remove(name)
					case 5:
						_ = sut.Rename(name, name+".renamed")
					case 6:
						_ = sut.WalkDir(".", func(string, fs.DirEntry, error) error { return nil })
					}
				}
			}(w)
		}
		wg.Wait()
	})

	t.Run("Lock with a cancelled context does not leak the lock", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewMemoryFS(10000000)

		// Repeatedly call Lock with an already-cancelled context. Each call
		// must release whatever internal state it temporarily acquired, so a
		// subsequent clean Lock still succeeds. (Without the fix, the
		// internal goroutine racing toward the per-path mutex could acquire
		// it after the caller bailed and leak it.)
		for range 100 {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err := sut.Lock(ctx, "lock")
			assert.ErrorIs(err, context.Canceled)
		}

		// Bound the wait so we surface DeadlineExceeded instead of hanging
		// the whole suite if the regression returns.
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		unlock, err := sut.Lock(ctx, "lock")
		assert.NoError(err)
		assert.NoError(unlock())
	})

	checkConsistency(t, func() FS {
		return NewMemoryFS(10000000)
	})
}

// Verify that the FS is consistent in itself.
func checkConsistency(t *testing.T, newSut func() FS) {
	t.Helper()
	t.Run("OpenWrite", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		w, err := sut.OpenWrite("a.txt")
		assert.NoError(err)
		written, err := w.Write([]byte("abcd"))
		assert.Equal(4, written)
		assert.NoError(err)
		assert.NoError(w.Close())

		assert.Equal("abcd", readFile(t, sut, "a.txt"))
	})

	t.Run("OpenWrite on existing file should overwrite it", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		w, err := sut.OpenWrite("a.txt")
		assert.NoError(err)
		written, err := w.Write([]byte("abcd"))
		assert.Equal(4, written)
		assert.NoError(err)
		assert.NoError(w.Close())
		assert.Equal("abcd", readFile(t, sut, "a.txt"))

		w, err = sut.OpenWrite("a.txt")
		assert.NoError(err)
		written, err = w.Write([]byte("efgh"))
		assert.Equal(4, written)
		assert.NoError(err)
		assert.NoError(w.Close())
		assert.Equal("efgh", readFile(t, sut, "a.txt"))
	})

	t.Run("OpenWrite on a directory should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Mkdir("mydir"))
		_, err := sut.OpenWrite("mydir")
		pathError, ok := err.(*fs.PathError) //nolint:errorlint
		assert.Equal(true, ok)
		assert.Equal("open", pathError.Op)
		assert.Equal(true, strings.HasSuffix(pathError.Path, "mydir"), pathError.Path)
		assert.Equal(syscall.EISDIR, pathError.Err)
	})

	t.Run("OpenWriteExcl should not overwrite existing file", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		w, err := sut.OpenWrite("a.txt")
		assert.NoError(err)
		written, err := w.Write([]byte("abcd"))
		assert.Equal(4, written)
		assert.NoError(err)
		assert.NoError(w.Close())

		_, err = sut.OpenWriteExcl("a.txt")
		assert.ErrorIs(err, fs.ErrExist)
	})

	t.Run("OpenWriteExcl on a directory should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Mkdir("a"))
		_, err := sut.OpenWriteExcl("a")
		assert.ErrorIs(err, fs.ErrExist)
	})

	t.Run("FSync", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		f, err := sut.OpenWrite("a.txt")
		assert.NoError(err)
		assert.NoError(sut.FSync(f))
		assert.NoError(f.Close())
	})

	t.Run("FSyncDir", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Mkdir("a"))
		writeFile(t, sut, "a/b.txt", "abcd")
		assert.NoError(sut.FSyncDir("a"))
	})

	t.Run("OpenRead", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		f, err := sut.OpenRead("a.txt")
		assert.NoError(err)
		data, err := io.ReadAll(f)
		assert.NoError(err)
		assert.Equal("abcd", string(data))

		// Read again.
		f, err = sut.OpenRead("a.txt")
		assert.NoError(err)
		data, err = io.ReadAll(f)
		assert.NoError(err)
		assert.Equal("abcd", string(data))
	})

	t.Run("OpenRead on non-existing file should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		_, err := sut.OpenRead("a.txt")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("OpenRead on a directory should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Mkdir("mydir"))
		r, err := sut.OpenRead("mydir")
		assert.NoError(err)
		defer r.Close() //nolint:errcheck
		_, err = io.ReadAll(r)
		pathError, ok := err.(*fs.PathError) //nolint:errorlint
		assert.Equal(true, ok)
		assert.Equal("read", pathError.Op)
		assert.Equal(true, strings.HasSuffix(pathError.Path, "mydir"), pathError.Path)
		assert.Equal(syscall.EISDIR, pathError.Err)
	})

	t.Run("Mkdir", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.Mkdir("a")
		assert.NoError(err)
		stat, err := sut.Stat("a")
		assert.NoError(err)
		assert.Equal(fs.FileMode(0o700), stat.Mode().Perm())
		assert.Equal(true, stat.Mode().IsDir())
	})

	t.Run("Mkdir on existing directory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.Mkdir("a")
		assert.NoError(err)
		err = sut.Mkdir("a")
		assert.ErrorIs(err, fs.ErrExist)

		err = sut.Mkdir("a/b")
		assert.NoError(err)
	})

	t.Run("Mkdir with not existing deep path should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.Mkdir("a/b/c")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("MkdirAll", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.MkdirAll("a/b/c")
		assert.NoError(err)
		stat, err := sut.Stat("a/b/c")
		assert.NoError(err)
		assert.Equal(fs.FileMode(0o700), stat.Mode().Perm())
	})

	t.Run("Stat", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		stat, err := sut.Stat("a.txt")
		assert.NoError(err)
		assert.Equal(int64(4), stat.Size())
		assert.Equal(fs.FileMode(0o600), stat.Mode().Perm())
		assert.Equal(true, stat.Mode().IsRegular())
	})

	t.Run("Stat on `.`", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		stat, err := sut.Stat(".")
		assert.NoError(err)
		assert.Equal(true, stat.Mode().IsDir())
	})

	t.Run("ReadDir", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Mkdir("a"))
		writeFile(t, sut, "a/b.txt", "b")
		writeFile(t, sut, "a/c.txt", "c")
		entries, err := sut.ReadDir("a")
		assert.NoError(err)
		assert.Equal(2, len(entries))
		names := make([]string, len(entries))
		for i, entry := range entries {
			names[i] = entry.Name()
		}
		slices.Sort(names)
		assert.Equal([]string{"b.txt", "c.txt"}, names)
	})

	t.Run("Chmod", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "a")
		err := sut.Chmod("a.txt", 0o400)
		assert.NoError(err)
		stat, err := sut.Stat("a.txt")
		assert.NoError(err)
		assert.Equal(fs.FileMode(0o400), stat.Mode().Perm())
	})

	t.Run("Chmod should fail if the file does not exist", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.Chmod("a.txt", 0o777)
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("Chmtime", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "a")
		mtime := time.Now()
		err := sut.Chmtime("a.txt", mtime)
		assert.NoError(err)
		stat, err := sut.Stat("a.txt")
		assert.NoError(err)
		assert.Equal(mtime.UnixNano(), stat.ModTime().UnixNano())
	})

	t.Run("Chmtime should fail if the file does not exist", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.Chmtime("a.txt", time.Now())
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("Chown", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "a")
		uid, gid := os.Getuid(), os.Getgid() //nolint:forbidigo
		err := sut.Chown("a.txt", uid, gid)
		assert.NoError(err)
	})

	t.Run("Chown should fail if the file does not exist", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.Chown("a.txt", 0, 0)
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("Remove", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "a")
		err := sut.Remove("a.txt")
		assert.NoError(err)
		_, err = sut.Stat("a.txt")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("Remove should fail if the file does not exist", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.Remove("a.txt")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("Remove an empty directory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Mkdir("mydir"))
		err := sut.Remove("mydir")
		assert.NoError(err)
		_, err = sut.Stat("mydir")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("Remove on a non-empty directory should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Mkdir("mydir"))
		writeFile(t, sut, "mydir/b.txt", "b")
		err := sut.Remove("mydir")
		pathError, ok := err.(*fs.PathError) //nolint:errorlint
		assert.Equal(true, ok)
		assert.Equal("remove", pathError.Op)
		assert.Equal(true, strings.HasSuffix(pathError.Path, "mydir"), pathError.Path)
		assert.Equal(syscall.ENOTEMPTY, pathError.Err)
	})

	t.Run("RemoveAll", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Mkdir("a"))
		writeFile(t, sut, "a/b.txt", "b")
		writeFile(t, sut, "a/c.txt", "c")
		err := sut.RemoveAll("a")
		assert.NoError(err)
		_, err = sut.Stat("a")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("RemoveAll should not fail if the directory does not exist", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.RemoveAll("a")
		assert.NoError(err)
	})

	t.Run("Rename", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "a")
		err := sut.Rename("a.txt", "b.txt")
		assert.NoError(err)
		_, err = sut.Stat("a.txt")
		assert.ErrorIs(err, fs.ErrNotExist)
		assert.Equal("a", readFile(t, sut, "b.txt"))
	})

	t.Run("Rename should fail if the source does not exist", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		err := sut.Rename("a.txt", "b.txt")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("Sub", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		_, err := sut.Sub("a")
		assert.ErrorIs(err, fs.ErrNotExist)

		assert.NoError(sut.Mkdir("a"))
		sub, err := sut.Sub("a")
		assert.NoError(err)
		stat, err := sub.Stat(".")
		assert.NoError(err)
		assert.Equal(true, stat.IsDir())
	})

	t.Run("MkSub", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		sub, err := sut.MkSub("a")
		assert.NoError(err)
		stat, err := sut.Stat("a")
		assert.NoError(err)
		assert.Equal(true, stat.IsDir())

		assert.NoError(sub.Mkdir("b"))
		stat, err = sub.Stat("b")
		assert.NoError(err)
		assert.Equal(true, stat.IsDir())
		stat, err = sut.Stat("a/b")
		assert.NoError(err)
		assert.Equal(true, stat.IsDir())
	})

	t.Run("WalkDir", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()
		assert.NoError(sut.MkdirAll("a/b/c"))
		writeFile(t, sut, "a/b/c/d.txt", "d")
		writeFile(t, sut, "a/b/c/e.txt", "e")
		writeFile(t, sut, "a/b/f.txt", "f")
		actual := []string{}
		err := sut.WalkDir("a/b", func(path string, d fs.DirEntry, err error) error {
			assert.NoError(err)
			actual = append(actual, path)
			return nil
		})
		assert.NoError(err)
		assert.Equal([]string{"a/b", "a/b/c", "a/b/c/d.txt", "a/b/c/e.txt", "a/b/f.txt"}, actual)
		actual = []string{}
		err = sut.WalkDir("a/b/c", func(path string, d fs.DirEntry, err error) error {
			assert.NoError(err)
			actual = append(actual, path)
			return nil
		})
		assert.NoError(err)
		assert.Equal([]string{"a/b/c", "a/b/c/d.txt", "a/b/c/e.txt"}, actual)
		// Walk the root directory.
		actual = []string{}
		err = sut.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
			assert.NoError(err)
			actual = append(actual, path)
			return nil
		})
		assert.NoError(err)
		assert.Equal([]string{".", "a", "a/b", "a/b/c", "a/b/c/d.txt", "a/b/c/e.txt", "a/b/f.txt"}, actual)
	})

	t.Run("WalkDir an empty directory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()
		actual := []string{}
		err := sut.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
			assert.NoError(err)
			actual = append(actual, path)
			return nil
		})
		assert.NoError(err)
		assert.Equal([]string{"."}, actual)
	})

	t.Run("WalkDir with SkipDir everything", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()
		writeFile(t, sut, "a.txt", "a")
		// In an earlier version, MemoryFS.WalkDir would just return the last error regardless
		// if it was a `SkipDir` error or not.
		err := sut.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
			return filepath.SkipDir
		})
		assert.NoError(err)
	})

	t.Run("WalkDir with SkipDir some directory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()
		assert.NoError(sut.MkdirAll("dir1"))
		assert.NoError(sut.MkdirAll("dir1b"))
		writeFile(t, sut, "dir1/a.txt", "a")
		writeFile(t, sut, "dir1/b.txt", "a")
		writeFile(t, sut, "dir1a.txt", "a")
		writeFile(t, sut, "dir1b/a.txt", "a")
		actual := []string{}
		err := sut.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
			if path == "dir1" && d.IsDir() {
				return filepath.SkipDir
			}
			assert.NoError(err)
			actual = append(actual, path)
			return nil
		})
		assert.NoError(err)
		assert.Equal([]string{".", "dir1a.txt", "dir1b", "dir1b/a.txt"}, actual)
	})

	t.Run("WalkDir with last error", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()
		writeFile(t, sut, "a.txt", "a")
		err := sut.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
			return Errorf("Boom")
		})
		assert.Error(err, "Boom")
	})

	t.Run("WalkDir on a MkSub", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()
		sub, err := sut.MkSub("a")
		assert.NoError(sut.MkdirAll("a/b/c"))
		writeFile(t, sut, "a/b/c/d.txt", "d")
		writeFile(t, sut, "a/b/c/e.txt", "e")
		writeFile(t, sut, "a/b/f.txt", "f")
		assert.NoError(err)
		actual := []string{}
		err = sub.WalkDir("b", func(path string, d fs.DirEntry, err error) error {
			assert.NoError(err)
			actual = append(actual, path)
			return nil
		})
		assert.NoError(err)
		assert.Equal([]string{"b", "b/c", "b/c/d.txt", "b/c/e.txt", "b/f.txt"}, actual)
	})

	t.Run("AtomicWriteFile", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()
		err := AtomicWriteFile(sut, "a.txt", 0o644, []byte("a"), []byte("b"))
		assert.NoError(err)
		assert.Equal("ab", readFile(t, sut, "a.txt"))
		stat, err := sut.Stat("a.txt")
		assert.NoError(err)
		assert.Equal(fs.FileMode(0o644), stat.Mode().Perm())
	})

	t.Run("Lock", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		unlock, err := sut.Lock(t.Context(), "lock")
		assert.NoError(err)

		ctx2, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		_, err = sut.Lock(ctx2, "lock")
		assert.ErrorIs(err, context.DeadlineExceeded)

		err = unlock()
		assert.NoError(err)

		unlock2, err := sut.Lock(t.Context(), "lock")
		assert.NoError(err)
		err = unlock2()
		assert.NoError(err)
	})

	t.Run("Calling unlock multiple times", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		unlock, err := sut.Lock(t.Context(), "lock")
		assert.NoError(err)

		err = unlock()
		assert.NoError(err)

		err = unlock()
		assert.NoError(err)
	})

	t.Run("Lock file is created", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		unlock, err := sut.Lock(t.Context(), "lock")
		assert.NoError(err)

		_, err = sut.Stat("lock")
		assert.NoError(err)

		err = unlock()
		assert.NoError(err)

		// Lock file should still exist.
		_, err = sut.Stat("lock")
		assert.NoError(err)
	})

	t.Run("Symlink and ReadLink round-trip", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		assert.NoError(sut.Symlink("a.txt", "link"))

		target, err := sut.ReadLink("link")
		assert.NoError(err)
		assert.Equal("a.txt", target)
	})

	t.Run("Symlink at existing path errors", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		err := sut.Symlink("other.txt", "a.txt")
		assert.ErrorIs(err, fs.ErrExist)
	})

	t.Run("ReadLink on a non-symlink errors", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		_, err := sut.ReadLink("a.txt")
		assert.Error(err, "")
	})

	t.Run("ReadLink on a missing path errors", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		_, err := sut.ReadLink("missing")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("Stat on a symlink does not follow", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		assert.NoError(sut.Symlink("a.txt", "link"))

		info, err := sut.Stat("link")
		assert.NoError(err)
		assert.Equal(true, info.Mode()&fs.ModeSymlink != 0)
		assert.Equal(int64(len("a.txt")), info.Size())
	})

	t.Run("Stat on a symlink with missing target still succeeds", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Symlink("does-not-exist", "link"))
		info, err := sut.Stat("link")
		assert.NoError(err)
		assert.Equal(true, info.Mode()&fs.ModeSymlink != 0)
	})

	t.Run("OpenRead on a symlink refuses to follow", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		assert.NoError(sut.Symlink("a.txt", "link"))

		_, err := sut.OpenRead("link")
		assert.ErrorIs(err, ErrIsSymlink)
	})

	t.Run("OpenWrite on a symlink refuses to follow", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		assert.NoError(sut.Symlink("a.txt", "link"))

		_, err := sut.OpenWrite("link")
		assert.ErrorIs(err, ErrIsSymlink)
		assert.Equal("abcd", readFile(t, sut, "a.txt"))
	})

	t.Run("OpenWriteExcl on a symlink refuses to follow", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		assert.NoError(sut.Symlink("a.txt", "link"))

		_, err := sut.OpenWriteExcl("link")
		assert.ErrorIs(err, ErrIsSymlink)
	})

	t.Run("Chmod on a symlink refuses to follow", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		assert.NoError(sut.Chmod("a.txt", 0o644))
		assert.NoError(sut.Symlink("a.txt", "link"))

		err := sut.Chmod("link", 0o600)
		assert.ErrorIs(err, ErrIsSymlink)

		info, err := sut.Stat("a.txt")
		assert.NoError(err)
		assert.Equal(fs.FileMode(0o644), info.Mode().Perm())
	})

	t.Run("Chmtime on a symlink sets the link's own mtime, not the target's", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		targetMtime := time.Unix(500_000, 0)
		assert.NoError(sut.Chmtime("a.txt", targetMtime))
		assert.NoError(sut.Symlink("a.txt", "link"))

		linkMtime := time.Unix(1_000_000, 0)
		assert.NoError(sut.Chmtime("link", linkMtime))

		linkInfo, err := sut.Stat("link")
		assert.NoError(err)
		assert.Equal(linkMtime.Unix(), linkInfo.ModTime().Unix())

		targetInfo, err := sut.Stat("a.txt")
		assert.NoError(err)
		assert.Equal(targetMtime.Unix(), targetInfo.ModTime().Unix())
	})

	t.Run("Stat on a symlink reports the link's own mtime", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		assert.NoError(sut.Chmtime("a.txt", time.Unix(500_000, 0)))
		assert.NoError(sut.Symlink("a.txt", "link"))
		assert.NoError(sut.Chmtime("link", time.Unix(1_000_000, 0)))

		linkInfo, err := sut.Stat("link")
		assert.NoError(err)
		assert.Equal(int64(1_000_000), linkInfo.ModTime().Unix())
	})

	t.Run("Chown on a symlink refuses to follow", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		assert.NoError(sut.Symlink("a.txt", "link"))

		err := sut.Chown("link", os.Getuid(), os.Getgid()) //nolint:forbidigo
		assert.ErrorIs(err, ErrIsSymlink)
	})

	t.Run("Remove on a symlink removes the link only", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		writeFile(t, sut, "a.txt", "abcd")
		assert.NoError(sut.Symlink("a.txt", "link"))

		assert.NoError(sut.Remove("link"))

		_, err := sut.Stat("link")
		assert.ErrorIs(err, fs.ErrNotExist)
		assert.Equal("abcd", readFile(t, sut, "a.txt"))
	})

	t.Run("WalkDir surfaces a symlink with ModeSymlink and does not recurse", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := newSut()

		assert.NoError(sut.Mkdir("dir"))
		writeFile(t, sut, "dir/file.txt", "x")
		assert.NoError(sut.Symlink("dir", "link"))

		seen := map[string]fs.FileMode{}
		err := sut.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			seen[path] = d.Type()
			return nil
		})
		assert.NoError(err)
		linkType, ok := seen["link"]
		assert.Equal(true, ok)
		assert.Equal(true, linkType&fs.ModeSymlink != 0)
		_, recursed := seen["link/file.txt"]
		assert.Equal(false, recursed)
	})
}

func readFile(t *testing.T, sut FS, name string) string {
	t.Helper()
	assert := NewAssert(t)
	data, err := ReadFile(sut, name)
	assert.NoError(err)
	return string(data)
}

func writeFile(t *testing.T, sut FS, name string, data string) {
	t.Helper()
	assert := NewAssert(t)
	err := WriteFile(sut, name, []byte(data))
	assert.NoError(err)
}
