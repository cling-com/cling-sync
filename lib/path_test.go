package lib

import (
	"strings"
	"testing"
)

func TestPath(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		p, err := NewPath("a/b/c")
		assert.NoError(err)
		assert.Equal(Path{"a/b/c"}, p)
	})

	t.Run("Path can be empty", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		p, err := NewPath("")
		assert.NoError(err)
		assert.Equal(Path{""}, p)
	})

	t.Run("Paths must not be absolute", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewPath("/a/b/c")
		assert.Error(err, "invalid path")
	})

	t.Run("Paths must not contain volume name", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewPath("C:/a/b/c")
		assert.Error(err, "invalid path")
		// A volume name without a trailing slash should not panic.
		_, err = NewPath("C:")
		assert.Error(err, "invalid path")
	})

	t.Run("Paths must not contain `//`", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewPath("a//b/c")
		assert.Error(err, "invalid path")
	})

	t.Run("Paths must not be relative", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewPath("./a")
		assert.Error(err, "must not be relative")
		_, err = NewPath(".")
		assert.Error(err, "must not be relative")
		_, err = NewPath("..")
		assert.Error(err, "must not be relative")
		_, err = NewPath(".a")
		assert.NoError(err)
	})

	t.Run("Paths must not contain `.` or `..`", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewPath("a/./b")
		assert.Error(err, "must not contain `.`")
		_, err = NewPath("a/../b")
		assert.Error(err, "must not contain `.` or `..`")
	})

	t.Run("Path must not end with `/`", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewPath("a/b/")
		assert.Error(err, "must not end with `/`")
	})

	t.Run("Path must not exceed MaxPathLen bytes", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewPath(strings.Repeat("a", MaxPathLen))
		assert.NoError(err)
		_, err = NewPath(strings.Repeat("a", MaxPathLen+1))
		assert.Error(err, "must not exceed")
	})

	t.Run("IsRelativeTo", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		assert.Equal(false, Path{"a/b"}.IsRelativeTo(Path{"a/b"}))
		assert.Equal(true, Path{"a/b/c"}.IsRelativeTo(Path{"a/b"}))
		assert.Equal(true, Path{"a/b/c"}.IsRelativeTo(Path{"a/b/"}))
		assert.Equal(false, Path{"a/b/c"}.IsRelativeTo(Path{"a/bc"}))
		assert.Equal(false, Path{"dir1/dir2/dir3"}.IsRelativeTo(Path{"dir1/dir"}))
		assert.Equal(true, Path{"dir1/dir2/dir3"}.IsRelativeTo(Path{"dir1/dir2"}))
	})

	t.Run("TrimBase", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)

		res, ok := Path{"a/b/c"}.TrimBase(Path{"a/b"})
		assert.Equal(true, ok)
		assert.Equal(Path{"c"}, res)

		// If the base does not match, the original path is returned.
		res, ok = Path{"a/b/c"}.TrimBase(Path{"a/b/d"})
		assert.Equal(false, ok)
		assert.Equal(Path{"a/b/c"}, res)

		// Base is treated as a directory not just a string prefix.
		res, ok = Path{"dir1/dir2/dir3"}.TrimBase(Path{"dir1/dir"})
		assert.Equal(false, ok)
		assert.Equal(Path{"dir1/dir2/dir3"}, res)

		// Empty base returns the original path.
		res, ok = Path{"a/b/c"}.TrimBase(Path{""})
		assert.Equal(true, ok)
		assert.Equal(Path{"a/b/c"}, res)
	})

	t.Run("Dir", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)

		assert.Equal("a/b", Path{"a/b/c"}.Dir().String())
		assert.Equal("a", Path{"a/b"}.Dir().String())
		assert.Equal("", Path{"a"}.Dir().String())
		assert.Equal("", Path{""}.Dir().String())
	})
}

func TestPathExclusionFilter(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewPathExclusionFilter([]string{"etc", "**/*.txt", "!etc/host.conf", "!opt/test.txt"})
		assert.Equal(true, sut.Include(Path{"etc/host.conf"}, false))
		assert.Equal(false, sut.Include(Path{"etc/passwd"}, false))
		assert.Equal(false, sut.Include(Path{"home/user/file.txt"}, false))
		assert.Equal(true, sut.Include(Path{"opt/test.txt"}, false))
	})

	t.Run("A pattern matching a directory also matches everything below it", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewPathExclusionFilter([]string{"build"})
		assert.Equal(false, sut.Include(Path{"build"}, true))
		assert.Equal(false, sut.Include(Path{"build/x.o"}, false))
		assert.Equal(true, sut.Include(Path{"buildings/x.o"}, false))
	})
}

func TestPathInclusionFilter(t *testing.T) {
	t.Parallel()

	t.Run("A pattern matches inside a directory but not the directory itself", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewPathInclusionFilter([]string{"src/**"})
		assert.Equal(false, sut.Include(Path{"src"}, true))
		assert.Equal(true, sut.Include(Path{"src/a.go"}, false))
		assert.Equal(true, sut.Include(Path{"src/sub"}, true))
	})
}
