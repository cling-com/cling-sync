package lib

import (
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
	assert := NewAssert(t)
	sut := NewPathExclusionFilter([]string{"etc", "**/*.txt", "!etc/host.conf", "!opt/test.txt"})
	assert.Equal(true, sut.Include(Path{"etc/host.conf"}, false))
	assert.Equal(false, sut.Include(Path{"etc/passwd"}, false))
	assert.Equal(false, sut.Include(Path{"home/user/file.txt"}, false))
	assert.Equal(true, sut.Include(Path{"opt/test.txt"}, false))
}

func TestAllPathFilter(t *testing.T) {
	t.Parallel()
	assert := NewAssert(t)
	exclude1 := NewPathExclusionFilter([]string{"etc", "!etc/host.conf"})
	// Even though exclude2 allows "etc", exclude1 should still exclude it.
	// Filters are evaluated separately.
	exclude2 := NewPathExclusionFilter([]string{"**/*.txt", "!opt/test.txt", "!etc"})
	sut := AllPathFilter{Filters: []PathFilter{exclude1, exclude2}}
	assert.Equal(true, sut.Include(Path{"etc/host.conf"}, false))
	assert.Equal(false, sut.Include(Path{"etc/passwd"}, false))
	assert.Equal(false, sut.Include(Path{"home/user/file.txt"}, false))
	assert.Equal(true, sut.Include(Path{"opt/test.txt"}, false))
}
