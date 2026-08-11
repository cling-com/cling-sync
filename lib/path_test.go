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

	t.Run("Base", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)

		assert.Equal("c", Path{"a/b/c"}.Base().String())
		assert.Equal("b", Path{"a/b"}.Base().String())
		assert.Equal("a", Path{"a"}.Base().String())
		assert.Equal("", Path{""}.Base().String())
	})

	t.Run("Dir", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)

		assert.Equal("a/b", Path{"a/b/c"}.Dir().String())
		assert.Equal("a", Path{"a/b"}.Dir().String())
		assert.Equal("", Path{"a"}.Dir().String())
		assert.Equal("", Path{""}.Dir().String())
	})

	t.Run("Depth", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)

		assert.Equal(0, Path{""}.Depth())
		assert.Equal(1, Path{"a"}.Depth())
		assert.Equal(2, Path{"a/b"}.Depth())
		assert.Equal(3, Path{"a/b/c"}.Depth())
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

func TestNilPathFilter(t *testing.T) {
	t.Parallel()
	assert := NewAssert(t)

	// A nil filter means no filter, so it keeps every path. An empty one is a
	// different thing and both are checked here so they cannot be confused.
	var include *PathInclusionFilter
	var exclude *PathExclusionFilter
	assert.Equal(true, include.Include(Path{"a.txt"}, false), "a nil include should keep every path")
	assert.Equal(true, exclude.Include(Path{"a.txt"}, false), "a nil exclude should keep every path")
	assert.Equal(false, NewPathInclusionFilter(nil).Include(Path{"a.txt"}, false),
		"an empty include matches nothing, so it drops every path")
	assert.Equal(true, NewPathExclusionFilter(nil).Include(Path{"a.txt"}, false),
		"an empty exclude drops nothing")
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

func TestPathCompare(t *testing.T) {
	t.Parallel()
	const (
		isFile = false
		isDir  = true
	)
	cases := []struct {
		name   string
		a      string
		aIsDir bool
		b      string
		bIsDir bool
		want   int
	}{
		{"Equal files", "a.txt", isFile, "a.txt", isFile, 0},
		{"Equal directories", "a", isDir, "a", isDir, 0},
		{"Equal nested files", "a/b.txt", isFile, "a/b.txt", isFile, 0},

		{"Files by name", "a.txt", isFile, "b.txt", isFile, -1},
		{"Directories by name", "a", isDir, "b", isDir, -1},
		{"Nested files by name", "a/b.txt", isFile, "a/c.txt", isFile, -1},
		{"Nested directories by name", "a/b", isDir, "a/c", isDir, -1},
		{"Deep files by last segment", "a/b/c.txt", isFile, "a/b/d.txt", isFile, -1},

		{"File before directory of the same name", "a", isFile, "a", isDir, -1},
		{"Nested file before nested directory of the same path", "a/b", isFile, "a/b", isDir, -1},
		{"Only the last segment decides file or directory", "a/b/c.txt", isFile, "a/b/c.txt", isDir, -1},

		{"Files before directories in the same parent", "a/c.txt", isFile, "a/b", isDir, -1},
		{"Files before directories at the root", "c.txt", isFile, "b", isDir, -1},
		// ...but not before a directory sorting below `0`, which is the bug
		// `PathCompare` reproduces. A root file is ordered as `0` plus its name,
		// so it lands among the directories rather than ahead of them.
		{"Dot-directory before every root file", ".a", isDir, "b.txt", isFile, -1},
		{"Root file after a directory starting with `0`", "b", isFile, "0a", isDir, 1},
		{"Root file before a directory starting with `0`", "b", isFile, "0c", isDir, -1},
		{"File before a sibling directory's contents", "a/c.txt", isFile, "a/b/c.txt", isFile, -1},
		{"Root file before a directory's contents", "c.txt", isFile, "a/b.txt", isFile, -1},
		{"File before a directory its name extends", "a/b.txt", isFile, "a/b", isDir, -1},
		{"Root file before a directory its name extends", "ab", isFile, "a", isDir, -1},
		{"Root file before a nested path under a shared prefix", "ab.txt", isFile, "a/b.txt", isFile, -1},

		{"Directory before its contents", "a", isDir, "a/b.txt", isFile, -1},
		{"Nested directory before its contents", "a/b", isDir, "a/b/c.txt", isFile, -1},
		{"Directory before a later directory's contents", "a", isDir, "b/c", isDir, -1},
		{"Directory after an earlier directory's contents", "b", isDir, "a/c.txt", isFile, 1},
		{"Contents of a later directory after an earlier directory", "b/a.txt", isFile, "a", isDir, 1},
		{"Contents of a directory before a longer sibling name", "a/b/c.txt", isFile, "ab/c.txt", isFile, -1},

		{"Prefix name first", "a", isDir, "ab", isDir, -1},
		// The root is always a directory, so there is no case for a file at
		// the empty path.
		{"Empty path before anything", "", isDir, "a", isDir, -1},
		{"Empty path before a root file", "", isDir, "a.txt", isFile, -1},
		//
		// The stored order cannot tell a root file from a directory spelling
		// the same name, because the file is ordered as `0` plus its name. The
		// file sorts first, so the two never tie and a commit can hold both.
		{"A file and a directory never compare equal in sub-paths", "a/b", isFile, "a/0b", isDir, -1},
		{"A file and a directory never compare equal at the root", "b", isFile, "0b", isDir, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			assert := NewAssert(t)
			a, b := Path{c.a}, Path{c.b}
			assert.Equal(c.want, PathCompare(a, c.aIsDir, b, c.bIsDir))
			assert.Equal(-c.want, PathCompare(b, c.bIsDir, a, c.aIsDir), "must be antisymmetric")
		})
	}
}

// `PathCompare` defines a stored order, so it has to be a strict total order
// over every path, and two different entries must never tie. A tie would make
// `TempWriter` reject the second one as a duplicate.
func FuzzPathCompare(f *testing.F) {
	f.Add("a/b", true, "a/b", false, "a", true)
	f.Add("b", false, "0b", true, "0a", true)
	f.Add("", true, "a.txt", false, ".git", true)
	f.Add("a/b/c", false, "a/bc", true, "ab/c", false)
	f.Fuzz(func(t *testing.T, a string, aIsDir bool, b string, bIsDir bool, c string, cIsDir bool) {
		if len(a) > 64 || len(b) > 64 || len(c) > 64 {
			return
		}
		assert := NewAssert(t)
		pa, pb, pc := Path{a}, Path{b}, Path{c}
		ab := PathCompare(pa, aIsDir, pb, bIsDir)
		bc := PathCompare(pb, bIsDir, pc, cIsDir)
		ac := PathCompare(pa, aIsDir, pc, cIsDir)

		assert.Equal(0, PathCompare(pa, aIsDir, pa, aIsDir), "must be reflexive")
		assert.Equal(-ab, PathCompare(pb, bIsDir, pa, aIsDir), "must be antisymmetric")
		if ab == 0 {
			assert.Equal(a, b, "only the same entry may tie")
			assert.Equal(aIsDir, bIsDir, "only the same entry may tie")
		}
		if ab <= 0 && bc <= 0 {
			assert.Equal(true, ac <= 0, "must be transitive")
		}
		if ab >= 0 && bc >= 0 {
			assert.Equal(true, ac >= 0, "must be transitive")
		}
	})
}

func BenchmarkPathCompare(b *testing.B) {
	x, y := Path{"d012/sub/f003456.txt"}, Path{"d012/sub/f003457.txt"}
	b.Run("Compare", func(b *testing.B) {
		for b.Loop() {
			_ = PathCompare(x, false, y, false)
		}
	})
	b.Run("CompareRootFile", func(b *testing.B) {
		f, d := Path{"a.txt"}, Path{"a"}
		for b.Loop() {
			_ = PathCompare(f, false, d, true)
		}
	})
	b.Run("CompareDirs", func(b *testing.B) {
		for b.Loop() {
			_ = PathCompare(x, true, y, true)
		}
	})
}
