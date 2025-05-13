package lib

import (
	"strings"
	"testing"
)

func TestPath(t *testing.T) {
	t.Parallel()
	assert := NewAssert(t)
	assert.Equal("a/b/c", NewPath("a", "b", "c").FSString())
	assert.Equal("a%2f/b%/c", NewPath("a/", "b%", "c").FSString())
}

func TestPathPattern(t *testing.T) {
	t.Parallel()
	t.Run("Compile", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		compileErr := func(pattern string) error {
			_, err := NewPathPattern(pattern)
			return err
		}
		assert.Error(compileErr(""), "empty")
		assert.Error(compileErr("**a"), "** must be followed by / or placed at the end")
		assert.Error(compileErr("a**a"), "** must be preceded by / or placed at the start")
		assert.Error(compileErr("a**b"), "** must be preceded by / or placed at the start")
		assert.Error(compileErr("a/**b"), "** must be followed by / or placed at the end")
	})

	t.Run("BasicPatterns", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		match := func(pattern string, path string) bool {
			t.Helper()
			p, err := NewPathPattern(pattern)
			assert.NoError(err)
			return p.Match(path)
		}

		// Literal matches
		assert.Equal(true, match("a.txt", "a.txt"), "exact match")
		assert.Equal(false, match("a.txt", "b.txt"), "exact non-match")
		assert.Equal(true, match("a/b.txt", "a/b.txt"), "path exact match")
		assert.Equal(false, match("a/b.txt", "a/c.txt"), "path exact non-match")
		assert.Equal(false, match("a/b.txt", "/a/b.txt"), "leading slash mismatch")

		// Question mark patterns
		assert.Equal(true, match("?.txt", "a.txt"), "? matches one character")
		assert.Equal(false, match("?.txt", "ab.txt"), "? doesn't match multiple characters")
		assert.Equal(true, match("a?c.txt", "abc.txt"), "? in middle")
		assert.Equal(false, match("a?b.txt", "a/b.txt"), "? doesn't match path delimiter")
		assert.Equal(false, match("a.txt?", "a.txt"), "? must match a character")
		assert.Equal(true, match("?.?.txt", "a.b.txt"), "multiple ? in pattern")

		// Single asterisk patterns
		assert.Equal(true, match("*.txt", "a.txt"), "* matches any within segment")
		assert.Equal(true, match("a*c.txt", "abbbbbbc.txt"), "* matches multiple characters")
		assert.Equal(true, match("*", "a.txt"), "* matches entire segment")
		assert.Equal(false, match("*c.txt", "ab.txt"), "* with suffix non-match")
		assert.Equal(false, match("*.txt", "a/b.txt"), "* doesn't match path delimiter")
		assert.Equal(false, match("*", "/ab.txt"), "* doesn't match leading slash")
		assert.Equal(true, match("a/*/c.txt", "a/b/c.txt"), "* matches directory name")
		assert.Equal(false, match("a/*/c.txt", "a/b/d/c.txt"), "* doesn't match multiple dirs")

		// Mixed patterns
		assert.Equal(true, match("a?/*/*.txt", "ab/c/d.txt"), "mixed ? and *")
		assert.Equal(true, match("*/*/*.???", "a/b/c.txt"), "* and ? together")
		assert.Equal(false, match("*/*/*.??", "a/b/c.txt"), "* and ? insufficient")
	})

	t.Run("DoubleAsteriskPatterns", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		match := func(pattern string, path string) bool {
			t.Helper()
			p, err := NewPathPattern(pattern)
			assert.NoError(err)
			return p.Match(path)
		}

		// Basic ** patterns
		assert.Equal(true, match("**/a.txt", "a.txt"), "** matches zero directories")
		assert.Equal(true, match("**/a.txt", "/a.txt"), "** matches zero dirs with leading slash")
		assert.Equal(true, match("**/a.txt", "dir/a.txt"), "** matches one directory")
		assert.Equal(true, match("**/a.txt", "dir1/dir2/a.txt"), "** matches multiple directories")
		assert.Equal(false, match("**/a.txt", "a.text"), "** with exact filename non-match")

		// ** in different positions
		assert.Equal(true, match("a/**/b.txt", "a/b.txt"), "** matches zero directories in middle")
		assert.Equal(true, match("a/**/b.txt", "a/x/b.txt"), "** matches one directory in middle")
		assert.Equal(true, match("a/**/b.txt", "a/x/y/z/b.txt"), "** matches multiple dirs in middle")
		assert.Equal(false, match("a/**/b.txt", "b.txt"), "** with prefix non-match")
		assert.Equal(false, match("a/**/b.txt", "a/b/c.txt"), "** with exact suffix non-match")

		// Trailing **
		assert.Equal(true, match("a/**", "a"), "trailing ** matches empty path")
		assert.Equal(true, match("a/**", "a/b"), "trailing ** matches one subdirectory")
		assert.Equal(true, match("a/**", "a/b/c/d"), "trailing ** matches any subdirectory depth")
		assert.Equal(false, match("a/**", "ab"), "trailing ** with prefix non-match")

		// Leading **
		assert.Equal(true, match("**/end", "end"), "leading ** matches zero prefix")
		assert.Equal(true, match("**/end", "start/end"), "leading ** matches one directory prefix")
		assert.Equal(true, match("**/end", "start/middle/end"), "leading ** matches multi-dir prefix")
		assert.Equal(false, match("**/end", "ending"), "leading ** with exact suffix non-match")

		// Multiple ** patterns
		assert.Equal(true, match("**/**/*.txt", "a.txt"), "multiple ** match zero dirs")
		assert.Equal(true, match("**/**/*.txt", "a/b/c.txt"), "multiple ** match nested paths")
		assert.Equal(true, match("a/**/**/b", "a/b"), "consecutive ** match zero dirs")
		assert.Equal(true, match("a/**/**/b", "a/x/y/z/b"), "consecutive ** match multiple dirs")

		// Complex ** patterns
		assert.Equal(true, match("**/a/**/b.txt", "a/b.txt"), "complex ** pattern minimal match")
		assert.Equal(true, match("**/a/**/b.txt", "a/x/y/z/b.txt"), "complex ** pattern deep nesting")
		assert.Equal(false, match("**/a/**/b.txt", "a/b/c.txt"), "complex ** pattern non-match")

		// Very complex pattern
		complexPattern := "**/a/**/b/**/c/**/d.txt"
		assert.Equal(true, match(complexPattern, "a/b/c/d.txt"), "minimal match for complex pattern")
		assert.Equal(true, match(complexPattern, "x/a/y/b/z/c/w/d.txt"), "complex alternating pattern")
		assert.Equal(false, match(complexPattern, "a/b/c/e.txt"), "complex pattern suffix non-match")
	})

	t.Run("EdgeCasesAndMaliciousInput", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		match := func(pattern string, path string) bool {
			t.Helper()
			p, err := NewPathPattern(pattern)
			assert.NoError(err)
			return p.Match(path)
		}

		// Deeply nested paths
		deepPath := strings.Repeat("a/", 500) + "file.txt"
		assert.Equal(true, match("**/file.txt", deepPath), "should handle very deep paths")
		assert.Equal(false, match("*/file.txt", deepPath), "* doesn't match deep paths")

		// Long paths with many separators
		manySlashesPath := strings.Repeat("/", 1000) + "file.txt"
		assert.Equal(false, match("*", manySlashesPath), "* shouldn't cross directory boundaries")
		assert.Equal(true, match("**/file.txt", manySlashesPath), "** should match paths with many slashes")

		// Very long pattern segments
		longSegment := strings.Repeat("a", 1000)
		longSegmentPath := longSegment + ".txt"
		assert.Equal(true, match(longSegment+".txt", longSegmentPath), "should match very long segments")
		assert.Equal(true, match(longSegment[:500]+"*"+".txt", longSegmentPath), "* should work with long segments")

		// Many pattern segments
		manySegmentsPattern := strings.Repeat("?/", 100) + "file.txt"
		manySegmentsPath := strings.Repeat("a/", 100) + "file.txt"
		assert.Equal(true, match(manySegmentsPattern, manySegmentsPath), "should handle patterns with many segments")

		// Nested pattern with many asterisks
		nestedPattern := strings.Repeat("*/", 50) + "file.txt"
		nestedPath := strings.Repeat("a/", 50) + "file.txt"
		assert.Equal(true, match(nestedPattern, nestedPath), "should handle deeply nested * patterns")

		// Mixed deep nesting
		mixedPattern := "**/" + strings.Repeat("a/*/", 20) + "file.txt"
		mixedPath := strings.Repeat("a/b/", 20) + "file.txt"
		assert.Equal(true, match(mixedPattern, mixedPath), "should handle mixed deep patterns")

		// Paths with unusual characters
		assert.Equal(true, match("**/file[1].txt", "a/b/file[1].txt"), "should handle paths with brackets")
		assert.Equal(true, match("**/*.{jpg,png}", "a/b/image.{jpg,png}"), "should handle paths with braces")
		assert.Equal(true, match("**/?hidden", "a/b/.hidden"), "should handle hidden files")

		// Empty segments in path
		assert.Equal(false, match("a/b", "a//b"), "should handle empty segments")
		assert.Equal(true, match("**/b", "a//b"), "** should handle empty segments")
	})
}
