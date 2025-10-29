package lib

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
		names := []string{}
		for _, entry := range entries {
			names = append(names, entry.Name())
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
