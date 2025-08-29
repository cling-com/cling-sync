// The comments in quotes are from the Git documentation at https://git-scm.com/docs/gitignore
//
//nolint:forbidigo,paralleltest,gosec
package lib

import (
	iofs "io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

var paths = []string{ //nolint:gochecknoglobals
	"README.md",
	".env",
	"extra/trailing-space ",
	"extra/ leading-space",
	"extra/middle space",
	"extra/with-tab\t",
	"extra/#",
	"extra/#hash",
	"extra/!",
	"extra/!important.txt",
	"extra/*",
	"extra/star*.txt",
	"extra/?",
	"extra/question?.txt",
	"extra/^.txt",
	"extra/\\.txt",
	"extra/[",
	"extra/bracket[.txt",
	"extra/bracket].txt",
	"extra/über.txt",
	"extra/emoji😀.txt",
	"extra/backslash\\",
	"extra/with-dash-in-name.txt",
	"extra/.env",
	"extra/123.txt",
	"lib/README.md",
	"lib/path.go",
	"lib/path_test.go",
	"lib/extra/README.md",
	"lib/extra/lib/README.md",
	"lib/extra/lib/more/lib/extra/README.md",
	"library/README.md",
	"library/info.go",
	"cli/extra",
}

func TestPrepareGlobPattern(t *testing.T) {
	t.Skip("Skipping because it takes too long")
	assert := NewAssert(t)
	assert.Equal("", string(PrepareGlobPattern((" "))))

	assert.Equal("a", string(PrepareGlobPattern("a")))
	assert.Equal("a", string(PrepareGlobPattern("a ")))
	assert.Equal("a", string(PrepareGlobPattern("a  ")))
	assert.Equal("a", string(PrepareGlobPattern("a   ")))

	assert.Equal(" a", string(PrepareGlobPattern(" a")))
	assert.Equal(" a", string(PrepareGlobPattern(" a ")))

	assert.Equal("a\\ ", string(PrepareGlobPattern("a\\ ")))
	assert.Equal("\\ ", string(PrepareGlobPattern("\\ ")))

	assert.Equal("a\\\\", string(PrepareGlobPattern("a\\\\ ")))
}

func TestGlobMatch(t *testing.T) {
	t.Skip("Skipping because it takes too long")
	var g globTester
	dir := t.TempDir()
	if os.Getenv("GLOB_TEST_IMPL") == "git" {
		t.Log("Using git glob tester")
		g = newGitGlobTester(t, dir)
	} else {
		t.Log("Using lib glob tester")
		g = newLibGlobTester(t, dir)
	}

	// "A blank line matches no files, so it can serve as a separator for readability."
	t.Run("Blank lines", func(t *testing.T) {
		g.non("", "README.md")
	})

	t.Run("Empty input", func(t *testing.T) {
		if os.Getenv("GLOB_TEST_IMPL") == "git" {
			t.Skip("Git does not support empty input")
		}
		g.non("", "")
		g.non("*.md", "")
	})

	// "A line starting with # serves as a comment. Put a backslash ("\") in front of
	// the first hash for patterns that begin with a hash."
	t.Run("Comments", func(t *testing.T) {
		g.non("# This is a comment", "README.md")
		g.non("#hash", "extra/#hash")
		g.yes("\\#hash", "extra/#hash")
		g.yes("\\#", "extra/#")
	})

	t.Run("Literals", func(t *testing.T) {
		g.yes("README.md", "README.md", "lib/README.md")

		// Match a directory and thus all files under it.
		g.yes("lib", "lib", "lib/README.md", "lib/extra/README.md")
	})

	// "Trailing spaces are ignored unless they are quoted with backslash ("\")."
	t.Run("Trailing spaces", func(t *testing.T) {
		g.yes("README.md   ", "README.md")
		g.non("trailing-space ", "extra/trailing-space ")
		g.yes("trailing-space\\ ", "extra/trailing-space ")
		// This is not escaping the last space.
		g.non("trailing-space\\\\ ", "extra/trailing-space ")

		// Leading and intermediate spaces are matched as-is.
		g.yes(" leading-space", "extra/ leading-space")
		g.yes("middle space", "extra/middle space")
	})

	// "The slash "/" is used as the directory separator. Separators may occur at
	// the beginning, middle or end of the .gitignore search pattern."
	t.Run("Slash", func(t *testing.T) {
		// "If there is a separator at the beginning or middle (or both) of the pattern,
		// then the pattern is relative to the directory level of the particular
		// .gitignore file itself. Otherwise the pattern may also match at any level
		// below the .gitignore level."
		// In this test we only test a single .gitignore file at the root so all paths are
		// relative to the root.
		g.yes("/README.md", "README.md")
		g.non("/README.md", "lib/README.md")
		g.yes("lib/extra", "lib/extra/README.md", "lib/extra/lib/more/lib/extra/README.md")

		// "If there is a separator at the end of the pattern then the pattern will only match
		// directories, otherwise the pattern can match both files and directories.
		// For example, a pattern doc/frotz/ matches doc/frotz directory, but not a/doc/frotz
		// directory; however frotz/ matches frotz and a/frotz that is a directory (all paths are
		// relative from the .gitignore file)."
		g.yes("extra/", "lib/extra", "lib/extra/lib", "lib/extra/lib/README.md")
		g.non("extra/", "cli/extra") // `cli/extra` is a file.
		g.non("README.md/", "README.md")

		// If a pattern contains a slash, it has to match from the beginning.
		g.yes("lib/extra/README.md", "lib/extra/README.md")
		g.non("lib/extra/README.md", "lib/extra/lib/more/lib/extra/README.md")

		// Empty path components do not match.
		g.non("lib//path.go", "lib/path.go")
	})

	// "An asterisk "*" matches anything except a slash. The character "?" matches any one
	// character except "/". The range notation, e.g. [a-zA-Z], can be used to match one of the
	// characters in a range. See fnmatch(3) and the FNM_PATHNAME flag for a more detailed
	// description."
	t.Run("Asterisk", func(t *testing.T) {
		g.yes("*.md", "README.md", "lib/README.md")
		g.yes("li*", "lib", "lib/extra", "lib/extra/README.md")
		g.non("li*", "cli")
		g.yes("li*/README.md", "lib/README.md")
		g.non("li*/README.md", "lib/path.go")
		g.yes("li*/", "lib")

		g.yes("lib/*.go", "lib/path.go")
		g.non("lib/*.go", "lib/README.md")
		g.non("lib*.go", "lib/path.go") // * does not match slashes.

		// An asterisk "*" matches exactly one path component.
		g.yes("lib/*/README.md", "lib/extra/README.md")
		g.non("lib/*/README.md", "lib/README.md", "lib/extra/lib/more/lib/extra/README.md")

		// Just to make sure that .* has no special meaning.
		g.yes(".*", ".env", "extra/.env")
		g.non(".*", "library/README.md")

		// A trailing "*" matches everything.
		g.yes("lib*", "lib/README.md", "lib/extra/README.md")

		// Regression test: This caused a panic because we were reading past
		// the end of the pattern.
		g.yes("README.*d", "README.md")

		g.yes("R*D*.*", "README.md")
		g.yes("*D*", "README.md")
	})

	t.Run("Question mark", func(t *testing.T) {
		g.yes("READM?.md", "README.md")
		g.non("lib?path.go", "lib/path.go")
		g.yes("README.m?", "README.md")
		g.non("README.me?", "README.md")
	})

	t.Run("Character class", func(t *testing.T) {
		g.yes("lib/p[a]th.go", "lib/path.go")
		g.yes("lib/p[ba]th.go", "lib/path.go")
		g.yes("lib/path.g[ao]", "lib/path.go")
		g.yes("lib/pa[a-z]h.go", "lib/path.go")
		g.yes("lib/p[ba-z]th.go", "lib/path.go")
		g.non("lib/p[b-z]th.go", "lib/path.go")

		// Character class at beginning and end.
		g.yes("[al]ib/path.g[ao]", "lib/path.go")
		g.yes("[p]*[o]", "lib/path.go")

		// Empty character class.
		g.non("lib/pat[].go", "lib/path.go")
		g.non("lib/pat[!].go", "lib/path.go")
		g.non("[^].txt", "extra/^.txt")

		// Character class with only negation.
		g.non("lib/path[!].go", "lib/path.go")

		// Escaped backslash in character class.
		g.yes("extra/[\\\\].txt", "extra/\\.txt")
		g.non("extra/[\\].txt", "extra/\\.txt")

		// Multiple ranges in character class.
		g.yes("lib/p[a-ce-g]th.go", "lib/path.go")
		g.yes("lib/p[e-ga-c]th.go", "lib/path.go")

		// Backwards range (Git treats as empty).
		g.non("lib/p[z-a]th.go", "lib/path.go")

		// Negated character class.
		g.yes("[!0-9]*", "README.md")
		g.non("[0-9]*", "README.md")
		g.yes("[!a]ath.go", "lib/path.go")
		g.non("[a]ath.go", "lib/path.go")
		// Negation means that a character must not match any literal or range.
		g.non("[!ap]ath.go", "lib/path.go")
		g.non("[!a-oo-q]ath.go", "lib/path.go")
		// ^ also negates according to Git's source code at
		// https://github.com/git/git/blob/master/wildmatch.c#L177
		// I couldn't find any documentation on this though.
		g.yes("^.txt", "extra/^.txt")
		g.yes("[^a].txt", "extra/^.txt")

		// Escape inside character class.
		g.yes("[\\!]*", "extra/!important.txt")
		g.yes("with[a\\-z]dash-in-name.txt", "extra/with-dash-in-name.txt")
		g.non("with[a-z]dash-in-name.txt", "extra/with-dash-in-name.txt")
		g.yes("bracket[\\]].txt", "extra/bracket].txt")
		// Escaping is lax, i.e. a backslash can precede any character.
		g.yes("star[\\*].txt", "extra/star*.txt")
		g.yes("[\\p]ath.go", "lib/path.go")

		// Character class is not closed.
		g.non("bracket[.txt", "extra/bracket[.txt")
		g.non("bracket[\\].txt", "extra/bracket].txt")

		// Understand that this is no regex, so `*` does not mean
		// `match the previous expression 0 or more times`.
		g.yes("lib/[p]*.go", "lib/path_test.go")
	})

	t.Run("POSIX character classes", func(t *testing.T) {
		// According to https://github.com/git/git/blob/master/wildmatch.c#L228
		// the following character classes are supported by Git:
		// "alnum" "alpha" "blank" "cntrl" "digit" "graph" "lower" "print" "punct" "space" "upper" "xdigit"

		// [:alnum:]
		g.yes("[[:alnum:]]*", "README.md")
		g.yes("extra/[[:alnum:]]*", "extra/123.txt")
		g.non("extra/[[:alnum:]]*", "extra/ leading-space")

		// [:alpha:]
		g.yes("R[[:alpha:]]ADME.md", "README.md")
		g.yes("*[[:alpha:]]", "README.md")
		g.non("extra/[[:alpha:]]*", "extra/123.txt")
		// Unicode characters don't fall into [[:alpha:]].
		g.non("[[:alpha:]]ber.txt", "extra/über.txt")
		// Just verify that we are working with bytes and not runes.
		g.yes("??ber.txt", "extra/über.txt")

		// [:blank:]
		g.yes("*[[:blank:]]", "extra/trailing-space ", "extra/with-tab\t")
		g.non("[[:blank:]]*", "README.md")

		// [:cntrl:]
		g.yes("*[[:cntrl:]]", "extra/with-tab\t")
		g.non("[[:cntrl:]]*", "README.md")
		g.non("[[:cntrl:]]*", "extra/!important.txt")

		// [:digit:]
		g.yes("extra/[[:digit:]]*", "extra/123.txt")
		g.non("[[:digit:]]*", "README.md")

		// [:graph:]
		g.yes("[[:graph:]]*", "README.md")
		g.yes("extra/[[:graph:]]*", "extra/!important.txt")
		g.yes("extra/[[:graph:]]*", "extra/123.txt")
		g.non("extra/*[[:graph:]]", "extra/with-tab\t")
		g.non("extra/*[[:graph:]]", "extra/trailing-space ")
		g.non("extra/[[:graph:]]*", "extra/über.txt") // This is not ASCII.

		// [:lower:]
		g.yes("lib/[[:lower:]]*", "lib/path.go")
		g.non("[[:lower:]]*", "README.md")

		// [:print:]
		g.yes("[[:print:]]*", "README.md")
		g.yes("extra/[[:print:]]*", "extra/ leading-space")
		g.non("extra/*[[:print:]]", "extra/with-tab\t")

		// [:punct:]
		g.yes("extra/[[:punct:]]*", "extra/!important.txt")
		g.non("extra/[[:punct:]]*", "extra/ leading-space")
		g.non("[[:punct:]]*", "README.md")

		// [:space:]
		g.yes("[[:space:]]*", "extra/ leading-space")
		g.yes("extra/*[[:space:]]", "extra/with-tab\t")

		// [:upper:]
		g.yes("[[:upper:]]*", "README.md")
		g.non("[[:upper:]]ath.go", "lib/path.go")

		// [:xdigit:]
		g.yes("R[[:xdigit:]]ADME.md", "README.md")
		g.non("READ[[:xdigit:]]E.md", "README.md")

		// Negation.
		g.non("[^[:alpha:]]*", "README.md")
		g.yes("[^[:digit:]]*", "README.md")

		// Combinations.
		g.yes("[[:upper:][:alpha:]]*", "README.md")         // Both match.
		g.yes("[[:lower:][:alpha:]]*", "README.md")         // Only the last matches.
		g.yes("[[:digit:][:lower:]R]EADME.md", "README.md") // Only the explicit "R" matches.

		// Unknown POSIX character classes.
		g.non("[[:beta:]]*", "README.md")
	})

	// "Two consecutive asterisks ("**") in patterns matched against full
	// pathname may have special meaning:"
	t.Run("Double asterisk", func(t *testing.T) {
		// "A leading "**" followed by a slash means match in all directories. For example, "**/foo"
		// matches file or directory "foo" anywhere, the same as pattern "foo". "**/foo/bar" matches
		// file or directory "bar" anywhere that is directly under directory "foo"."
		g.yes("**/README.md",
			"README.md", // `**` also matches no directory at all.
			"lib/extra/README.md",
			"lib/extra/lib/more/lib/extra/README.md")

		// "A trailing "/**" matches everything inside. For example, "abc/**" matches all files
		// inside directory "abc", relative to the location of the .gitignore file, with infinite
		// depth."
		g.yes("lib/**", "lib/README.md",
			"lib/path.go", "lib/path_test.go",
			"lib/extra/README.md",
			"lib/extra/lib/more/lib/extra/README.md")

		// "A slash followed by two consecutive asterisks then a slash matches zero or more
		// directories. For example, "a/**/b" matches "a/b", "a/x/b", "a/x/y/b" and so on."
		g.yes("lib/**/README.md", "lib/README.md", "lib/extra/README.md", "lib/extra/lib/more/lib/extra/README.md")

		// "Other consecutive asterisks are considered regular asterisks and will match according
		// to the previous rules."
		g.yes("lib/***/README.md", "lib/README.md", "lib/extra/README.md", "lib/extra/lib/more/lib/extra/README.md")
		g.yes("lib/**extra/README.md", "lib/extra/README.md")
		g.non("lib/**extra/README.md", "lib/extra/lib/more/lib/extra/README.md")
		g.yes("lib/******extra/README.md", "lib/extra/README.md")
		g.non("lib/**nextra/README.md", "lib/extra/README.md")

		// ** at the very end without trailing /
		g.yes("lib**", "lib", "library", "lib/path.go", "library/README.md")

		// Multiple ** parts.
		g.yes("lib/**/lib/**/README.md", "lib/extra/lib/more/lib/extra/README.md")
		g.non("lib/**/lib/**/README.md", "lib/extra/README.md")
	})

	t.Run("More escaping", func(t *testing.T) {
		// Escaping any regular character does not do anything.
		g.yes("\\README.md", "README.md")
		g.yes("REA\\DME.md", "README.md")

		// Backslash at the end fails.
		g.non("README.md\\", "README.md")
		g.non("backslash\\", "extra/backslash\\")
		g.yes("backslash\\\\", "extra/backslash\\")

		// Escape "*".
		g.yes("\\*", "extra/*")
		g.non("\\*", "README.md", "extra/star*.txt")
		g.yes("star\\*.txt", "extra/star*.txt")
		g.non("sta\\*.txt", "extra/star*.txt")

		// Escape "?".
		g.yes("\\?", "extra/?")
		g.non("\\?", "README.md", "extra/question?.txt")
		g.yes("question\\?.txt", "extra/question?.txt")

		// Escape "[".
		g.yes("\\[", "extra/[")
		g.non("[", "extra/[")
		g.non("\\[", "README.md", "extra/bracket[.txt")
		g.yes("bracket\\[.txt", "extra/bracket[.txt")
	})

	t.Run("Hidden files are not special", func(t *testing.T) {
		// Hidden files are not ignored by default.
		g.non("", ".env")

		// Wildcards match dot.
		g.yes("?env", ".env")
		g.yes("*env", ".env")
		g.yes("[.a]env", ".env")
		g.non("[!.]env", ".env")
	})

	t.Run("Matches are case sensitive", func(t *testing.T) {
		g.yes("README.md", "README.md")
		g.non("README.md", "readme.md")
	})

	t.Run("Unicode character handling", func(t *testing.T) {
		// Git uses fnmatch which matches based on byte patterns, not graphical characters.

		// Literal unicode matching.
		g.yes("über.txt", "extra/über.txt")
		g.yes("emoji😀.txt", "extra/emoji😀.txt")

		// "*" wildcards should be unaffected.
		g.yes("*😀.txt", "extra/emoji😀.txt")

		// "?" wildcards should not work because ü and 😀 are not ASCII.
		g.non("?ber.txt", "extra/über.txt")
		g.non("emoji?.txt", "extra/emoji😀.txt")

		// Character classes also operate on bytes.
		g.yes("[ü]*.txt", "extra/über.txt")
		g.yes("[a-ä]*.txt", "extra/über.txt")
	})
}

// This test is modeled after:
// https://github.com/git/git/blob/master/t/unit-tests/u-ctype.c
func TestPOSIXCharacterClasses(t *testing.T) {
	digit := "0123456789"
	lower := "abcdefghijklmnopqrstuvwxyz"
	upper := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	punct := "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
	cntrl := "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f" +
		"\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f" +
		"\x7f"
	space := " \n\r\t"
	run := func(t *testing.T, f func(byte) bool, matches ...string) {
		t.Helper()
		for b := range 128 {
			shouldMatch := false
			for _, match := range matches {
				if strings.IndexByte(match, byte(b)) >= 0 {
					shouldMatch = true
					break
				}
			}
			NewAssert(t).Equal(shouldMatch, f(byte(b)), "byte %d %s", b, string(rune(b)))
		}
	}

	// According to https://github.com/git/git/blob/master/wildmatch.c#L228
	// the following character classes are supported by Git:
	// "alnum" "alpha" "blank" "cntrl" "digit" "graph" "lower" "print" "punct" "space" "upper" "xdigit"

	// The expectations are directly taken from:
	// https://github.com/git/git/blob/master/t/unit-tests/u-ctype.c
	t.Run("alnum", func(t *testing.T) {
		run(t, IsAlnum, lower, upper, digit)
	})

	t.Run("alpha", func(t *testing.T) {
		run(t, IsAlpha, lower, upper)
	})

	t.Run("blank", func(t *testing.T) {
		run(t, IsBlank, " \t")
	})

	t.Run("cntrl", func(t *testing.T) {
		run(t, IsCntrl, cntrl)
	})

	t.Run("digit", func(t *testing.T) {
		run(t, IsDigit, digit)
	})

	t.Run("graph", func(t *testing.T) {
		run(t, IsGraph, lower, upper, digit, punct)
	})

	t.Run("lower", func(t *testing.T) {
		run(t, IsLower, lower)
	})

	t.Run("print", func(t *testing.T) {
		run(t, IsPrint, lower, upper, digit, punct, " ")
	})

	t.Run("punct", func(t *testing.T) {
		run(t, IsPunct, punct)
	})

	t.Run("space", func(t *testing.T) {
		run(t, IsSpace, space)
	})

	t.Run("upper", func(t *testing.T) {
		run(t, IsUpper, upper)
	})

	t.Run("xdigit", func(t *testing.T) {
		run(t, IsXDigit, digit, "abcdefABCDEF")
	})
}

func TestGitGlobMatch(t *testing.T) {
	// Test if Git is installed.
	cmd := exec.Command("git", "--version")
	err := cmd.Run()
	if err != nil {
		t.Skip("Git not installed")
	}
	t.Setenv("GLOB_TEST_IMPL", "git")
	TestGlobMatch(t)
	t.Setenv("GLOB_TEST_IMPL", "")
}

func TestGlobIgnorePatterns(t *testing.T) {
	assert := NewAssert(t)
	patterns := ExtendedGlobPatterns{
		NewExtendedGlobPattern("*.md", ""),
		NewExtendedGlobPattern("!README.md", ""),
	}
	assert.Equal(false, patterns.Match("README.md", false))
	assert.Equal(true, patterns.Match("TODO.md", false))
}

func TestWalkDirIgnore(t *testing.T) {
	var g globTester
	dir := t.TempDir()
	if os.Getenv("GLOB_TEST_IMPL") == "git" {
		t.Log("Using git glob tester")
		g = newGitGlobTester(t, dir)
	} else {
		t.Log("Using lib glob tester")
		g = newLibGlobTester(t, dir)
	}
	_ = g

	t.Run("No ignore file", func(t *testing.T) {
		g.walkDirIgnore("", paths...)
	})

	t.Run("Happy path", func(t *testing.T) {
		g.ignore(".gitignore", ".env", "extra", "*.go")
		g.walkDirIgnore("", ".gitignore", "README.md", "lib/README.md", "library/README.md")
	})

	t.Run(".gitignore in sub directories", func(t *testing.T) {
		g.ignore(".gitignore", ".env", "extra", "*.go")
		g.ignore("lib/.gitignore", "/README.md")
		g.walkDirIgnore("", ".gitignore", "lib/.gitignore", "README.md", "library/README.md")
	})

	t.Run(".gitignore in parallel directories", func(t *testing.T) {
		g.ignore(".gitignore", ".*", "extra")
		g.ignore("lib/.gitignore", "*.md")
		g.ignore("library/.gitignore", "*.go")
		g.walkDirIgnore("", "README.md", "lib/path.go", "lib/path_test.go", "library/README.md")
	})

	// "An optional prefix "!" which negates the pattern; any matching file excluded by a previous
	// pattern will become included again. It is not possible to re-include a file if a parent
	// directory of that file is excluded. Git doesn’t list excluded directories for performance
	// reasons, so any patterns on contained files have no effect, no matter where they are
	// defined. Put a backslash ("\") in front of the first "!" for patterns that begin with a
	// literal "!", for example, "\!important!.txt"."
	t.Run("Negation", func(t *testing.T) {
		g.ignore(".gitignore", ".*", "extra", "*.go") // Baseline
		g.ignore(".gitignore", "*.md", "!library/README.md")
		g.walkDirIgnore("", "library/README.md")

		// "It is not possible to re-include a file if a parent directory of that file is excluded."
		g.ignore(".gitignore", ".*", "extra", "lib", "library", "!lib/README.md")
		g.walkDirIgnore("", "README.md")

		// "Put a backslash ("\") in front of the first "!" for patterns that begin with a literal "!",
		// for example, "\!important!.txt"."
		g.ignore(".gitignore", ".gitignore", "\\!important.txt")
		expected := slices.Clone(paths)
		expected = slices.DeleteFunc(expected, func(path string) bool {
			return strings.HasSuffix(path, "!important.txt")
		})
		g.walkDirIgnore("", expected...)
	})

	t.Run(".gitignore in sub-directories can negate rules from parents", func(t *testing.T) {
		// This also tests that the ignore files are applied in the correct order.
		g.ignore(".gitignore", ".*", "extra", "library", "*.md") // Baseline
		g.ignore(".gitignore", "*.go")
		g.ignore("lib/.gitignore", "!*.go")
		g.walkDirIgnore("", "lib/path.go", "lib/path_test.go")
	})

	t.Run(".clingignore", func(t *testing.T) {
		if os.Getenv("GLOB_TEST_IMPL") == "git" {
			t.Skip("Git does not support .clingignore")
		}
		g.ignore(".clingignore", ".env", "extra", "*.go")
		g.ignore("lib/.clingignore", "/README.md")
		g.walkDirIgnore("", ".clingignore", "lib/.clingignore", "README.md", "library/README.md")
	})

	t.Run("Multiple nested .gitignore files with cascading rules", func(t *testing.T) {
		g.ignore(".gitignore", ".*", "extra", "cli") // Baseline
		g.ignore(".gitignore", "*.md")
		g.ignore("lib/.gitignore", "!README.md")
		g.ignore("lib/extra/.gitignore", "*.md") // Re-ignores in deeper level
		g.walkDirIgnore("",
			"lib/path.go",
			"lib/path_test.go",
			"lib/README.md", // Re-included by lib/.gitignore
			"library/info.go")
		// lib/extra/README.md is ignored again by deeper .gitignore
	})

	t.Run("Nothing below an excluded directory can be included again", func(t *testing.T) {
		g.ignore(".gitignore", ".*", "extra", "cli") // Baseline
		g.ignore(".gitignore", "lib/")               // Exclude entire directory
		g.ignore("lib/.gitignore", "!*.go")          // This should have no effect
		g.walkDirIgnore("", "README.md", "library/README.md", "library/info.go")
	})

	t.Run("Slash prefixes", func(t *testing.T) {
		g.ignore(".gitignore", ".*", "/extra", "cli", "*.go") // Baseline
		g.ignore(".gitignore", "/README.md")
		g.ignore("lib/.gitignore", "/README.md", "/extra/lib/README.md")
		g.walkDirIgnore("",
			"lib/extra/README.md",
			"lib/extra/lib/more/lib/extra/README.md",
			"library/README.md")
	})

	t.Run("Exclude all and selectively include directories", func(t *testing.T) {
		g.ignore(".gitignore", "/*", "!library")
		g.walkDirIgnore("", "library/README.md", "library/info.go")
	})
}

func TestGitWalkDirIgnore(t *testing.T) {
	// Test if Git is installed.
	cmd := exec.Command("git", "--version")
	err := cmd.Run()
	if err != nil {
		t.Skip("Git not installed")
	}
	t.Setenv("GLOB_TEST_IMPL", "git")
	TestWalkDirIgnore(t)
	t.Setenv("GLOB_TEST_IMPL", "")
}

func TestGlobMatchFuzz(t *testing.T) {
	t.Skip("Skipping because it takes too long")
	t.Run("Previous findings", func(t *testing.T) {
		// Backtracking the ** looped forever because we were past the end of the text.
		_ = GlobMatch(PrepareGlobPattern("a**/a"), []byte("a"), false)

		// Entering a character class with no more text to match read past the end of the text.
		_ = GlobMatch(PrepareGlobPattern("a[a]"), []byte("a"), true)

		// Ending with an escape sequence read past the end of the text.
		_ = GlobMatch(PrepareGlobPattern("a\\a"), []byte("a"), false)

		// This read past the end of the text during backtracking of the "*".
		_ = GlobMatch(PrepareGlobPattern("/*\\/a"), []byte("a/"), true)

		// Escaping after asterisk tried to read past the end of the text.
		_ = GlobMatch(PrepareGlobPattern("*\\?"), []byte("a"), true)

		// **<anything> read past the end of the pattern.
		_ = GlobMatch(PrepareGlobPattern("**b"), []byte("aa"), false)
	})

	t.Run("Fuzz", func(t *testing.T) {
		sut := func(pattern string, text string, isDir bool) {
			p := PrepareGlobPattern(pattern)
			done := make(chan bool, 1)
			go func() {
				select {
				case <-done:
				case <-time.After(time.Second):
					buf := make([]byte, 1<<20)
					n := runtime.Stack(buf, true)
					allStacks := string(buf[:n])
					t.Errorf(
						"GlobMatch is probably stuck! Add a previous finding like this:\n\t_ = GlobMatch(PrepareGlobPattern(%q), []byte(%q), %v)\n\nAll stacks:\n%s",
						pattern,
						text,
						isDir,
						allStacks,
					)
				}
			}()
			defer func() {
				done <- true
				if r := recover(); r != nil {
					// Get the stack trace
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					stackTrace := string(buf[:n])
					t.Errorf(
						"GlobMatch panicked! Add a previous finding like this:\n\t_ = GlobMatch(PrepareGlobPattern(%q), []byte(%q), %v)\n\nStack:\n%s",
						pattern,
						text,
						isDir,
						stackTrace,
					)
				}
			}()
			_ = GlobMatch(p, []byte(text), isDir)
		}

		generateRandomPattern := func() string {
			chars := []byte("abcü */\\?[]!-.*")
			length := rand.IntN(10)
			result := make([]byte, length)
			for i := range result {
				result[i] = chars[rand.IntN(len(chars))]
			}
			return string(result)
		}

		generateRandomText := func() string {
			chars := []byte("abcü /\\.-_123# ")
			length := rand.IntN(20)
			result := make([]byte, length)
			for i := range result {
				result[i] = chars[rand.IntN(len(chars))]
			}
			return string(result)
		}

		cases := 100000
		if testing.Short() {
			cases = 10000
		}

		for i := range cases {
			pattern := generateRandomPattern()
			text := generateRandomText()
			sut(pattern, text, i%2 == 0)
		}
	})
}

func BenchmarkGlobMatch(b *testing.B) {
	b.ResetTimer()
	patterns := []GlobPattern{
		PrepareGlobPattern("README.md"),
		PrepareGlobPattern("**/extra/README.md"),
		PrepareGlobPattern("/lib/extra/README.md"),
	}
	for b.Loop() {
		for _, pattern := range patterns {
			if !GlobMatch(pattern, []byte("lib/extra/README.md"), false) {
				b.Fatalf("Failed to match %s", pattern)
			}
		}
	}
}

type globTester interface {
	// Check that the given pattern matches the given path.
	yes(pattern string, paths ...string)
	// Check that the given pattern does not match the given path.
	non(pattern string, paths ...string)

	// Write the given patterns to the given ignore file path.
	ignore(path string, patterns ...string)

	// Walk the given directory and check that the expected paths are returned.
	walkDirIgnore(dir string, expected ...string)
}

type libGlobTester struct {
	dir string
	tb  testing.TB
}

func newLibGlobTester(tb testing.TB, dir string) *libGlobTester {
	tb.Helper()
	createTestPaths(tb, dir)
	return &libGlobTester{dir, tb}
}

func (g *libGlobTester) yes(pattern string, paths ...string) {
	g.tb.Helper()
	assert := NewAssert(g.tb)
	actual := []string{}
	globPattern := PrepareGlobPattern(pattern)
	for _, path := range paths {
		stat, err := os.Stat(filepath.Join(g.dir, path))
		assert.NoError(err, "Failed to stat %s", path)
		isDir := stat.IsDir()
		if GlobMatch(globPattern, []byte(path), isDir) {
			actual = append(actual, path)
		}
	}
	assert.Equal(paths, actual, "pattern %s", pattern)
}

func (g *libGlobTester) non(pattern string, paths ...string) {
	g.tb.Helper()
	assert := NewAssert(g.tb)
	actual := []string{}
	globPattern := PrepareGlobPattern(pattern)
	for _, path := range paths {
		stat, err := os.Stat(filepath.Join(g.dir, path))
		assert.NoError(err, "Failed to stat %s", path)
		isDir := stat.IsDir()
		if GlobMatch(globPattern, []byte(path), isDir) {
			actual = append(actual, path)
		}
	}
	assert.Equal([]string{}, actual, "pattern %s", pattern)
}

func (g *libGlobTester) ignore(path string, patterns ...string) {
	g.tb.Helper()
	ignore(g.tb, g.dir, path, patterns...)
}

func (g *libGlobTester) walkDirIgnore(dir string, expected ...string) {
	g.tb.Helper()
	defer cleanIgnoreFiles(g.tb, g.dir)
	assert := NewAssert(g.tb)
	actual := []string{}
	dir = filepath.Join(g.dir, dir)
	fs := NewRealFS(dir)
	err := WalkDirIgnore(fs, ".", func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		actual = append(actual, path)
		return nil
	})
	assert.NoError(err, "Failed to glob %s", dir)
	slices.Sort(actual)
	slices.Sort(expected)
	assert.Equal(expected, actual, "dir %s", dir)
}

type gitGlobTester struct {
	dir string
	tb  testing.TB
}

func newGitGlobTester(tb testing.TB, dir string) *gitGlobTester {
	tb.Helper()
	assert := NewAssert(tb)
	repo := &gitGlobTester{dir: dir, tb: tb}

	// Initialize Git repository.
	cmd := exec.Command("git", "init")
	cmd.Dir = dir
	assert.NoError(cmd.Run(), "Failed to initialize Git repository")

	// Tell Git to treat paths as case-sensitive.
	cmd = exec.Command("git", "config", "core.ignorecase", "false")
	cmd.Dir = dir
	assert.NoError(cmd.Run(), "Failed to set core.ignorecase")

	// Tell Git to print unicode characters as-is.
	cmd = exec.Command("git", "config", "core.quotepath", "false")
	cmd.Dir = dir
	assert.NoError(cmd.Run(), "Failed to set core.quotepath")

	createTestPaths(tb, dir)
	return repo
}

func (g *gitGlobTester) yes(pattern string, paths ...string) {
	g.tb.Helper()
	assert := NewAssert(g.tb)
	actual := g.check(pattern, paths...)
	assert.Equal(paths, actual, "pattern %s", pattern)
}

func (g *gitGlobTester) non(pattern string, paths ...string) {
	g.tb.Helper()
	assert := NewAssert(g.tb)
	actual := g.check(pattern, paths...)
	assert.Equal([]string{}, actual, "pattern %s", pattern)
}

func (g *gitGlobTester) ignore(path string, patterns ...string) {
	g.tb.Helper()
	ignore(g.tb, g.dir, path, patterns...)
}

func (g *gitGlobTester) walkDirIgnore(dir string, expected ...string) {
	g.tb.Helper()
	defer cleanIgnoreFiles(g.tb, g.dir)
	assert := NewAssert(g.tb)
	actual := []string{}
	cmd := exec.Command("git", "ls-files", "--others", "--exclude-standard")
	cmd.Dir = filepath.Join(g.dir, dir)
	out, err := cmd.Output()
	assert.NoError(err, "Failed to run git ls-files")
	for line := range strings.SplitSeq(string(out), "\n") {
		if line == "" {
			continue
		}
		line = strings.Trim(line, "\"")
		line = strings.ReplaceAll(line, "\\\\", "\\")
		line = strings.ReplaceAll(line, "\\t", "\t")
		actual = append(actual, line)
	}
	slices.Sort(actual)
	slices.Sort(expected)
	assert.Equal(expected, actual, "dir %s", dir)
}

func (g *gitGlobTester) check(pattern string, paths ...string) []string {
	assert := NewAssert(g.tb)
	err := os.WriteFile(filepath.Join(g.dir, ".gitignore"), []byte(pattern), 0o600)
	assert.NoError(err, "Failed to write .gitignore")

	matches := []string{}
	for _, path := range paths {
		cmd := exec.Command("git", "check-ignore", path)
		cmd.Dir = g.dir
		err = cmd.Run()
		switch cmd.ProcessState.ExitCode() {
		case 0:
			matches = append(matches, path)
		case 1:
		default:
			g.tb.Fatalf(
				"git check-ignore failed: expected no error, got exit status %d: %v",
				cmd.ProcessState.ExitCode(),
				err,
			)
		}
	}
	return matches
}

func ignore(tb testing.TB, baseDir, path string, patterns ...string) {
	tb.Helper()
	assert := NewAssert(tb)
	path = filepath.Join(baseDir, path)
	s := strings.Join(patterns, "\n")
	_, err := os.Stat(path)
	if err == nil {
		s = "\n" + s
	} else if !os.IsNotExist(err) {
		assert.NoError(err, "Failed to stat %s", path)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	assert.NoError(err, "Failed to open %s", path)
	defer f.Close() //nolint:errcheck
	_, err = f.WriteString(s)
	assert.NoError(err, "Failed to write to %s", path)
}

func createTestPaths(tb testing.TB, dir string) {
	tb.Helper()
	assert := NewAssert(tb)
	for _, path := range paths {
		fullPath := filepath.Join(dir, path)
		if strings.HasSuffix(path, "/") {
			assert.NoError(os.MkdirAll(fullPath, 0o700), "Failed to create directory %s", fullPath)
		} else {
			dirPath := filepath.Dir(fullPath)
			assert.NoError(os.MkdirAll(dirPath, 0o700), "Failed to create directory %s", dirPath)
			assert.NoError(os.WriteFile(fullPath, []byte("test content"), 0o600), "Failed to create file %s", fullPath)
		}
	}
}

func cleanIgnoreFiles(tb testing.TB, dir string) {
	tb.Helper()
	assert := NewAssert(tb)
	_ = filepath.WalkDir(dir, func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(path, ".gitignore") || strings.HasSuffix(path, ".clingignore") {
			assert.NoError(os.Remove(path), "Failed to remove %s", path)
		}
		return nil
	})
}
