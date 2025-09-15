// Git-style glob pattern matching.
package lib

import (
	"bytes"
	"errors"
	iofs "io/fs"
	"path/filepath"
	"strings"
)

var ignoreFileNames = []string{".gitignore", ".clingignore"} //nolint:gochecknoglobals

type GlobPattern []byte

// Take the given pattern and trim trailing spaces.
func PrepareGlobPattern(pattern string) GlobPattern {
	p := []byte(pattern)
	pend := len(p)
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] != ' ' {
			break
		}
		if i == 0 {
			pend = 0
			break
		}
		if p[i-1] != '\\' {
			pend = i
			continue
		}
		if i == 1 {
			pend = i + 1
			break
		}
		if p[i-2] == '\\' {
			pend = i
			continue
		}
		pend = i + 1
	}
	return p[:pend]
}

// Match Git-style glob pattern according to
// https://git-scm.com/docs/gitignore#_pattern_format
// and https://github.com/git/git/blob/master/wildmatch.c
func GlobMatch(pattern GlobPattern, text []byte, isDir bool) bool { //nolint:funlen
	if len(pattern) == 0 || len(text) == 0 {
		return false
	}
	if pattern[0] == '#' {
		return false
	}

	// Index into the pattern and text.
	p := 0
	t := 0

	// Backtracking positions for "*".
	starP := -1
	starT := -1

	// Backtracking positions for "**".
	starStarP := -1
	starStarT := -1

	// Determine the end of the pattern and text as well as whether to match a directory.
	pend := len(pattern) - 1
	tend := len(text) - 1
	mustMatchDir := false
	if pattern[len(pattern)-1] == '/' {
		// The pattern must match the path up to the end
		// but the terminal "/" is not part of the actual pattern.
		pend -= 1
		mustMatchDir = true
	}

	if pattern[0] == '/' {
		// The pattern must match the path from the beginning
		// but the initial "/" is not part of the actual pattern.
		p += 1
	} else if !bytes.ContainsAny(pattern[:len(pattern)-1], "/") {
		// A pattern that does not start with a slash is treated as
		// a pattern starting with "**/".
		starStarP = 0
		starStarT = 0
	}

	matched := true
	for {
		// Backtracking * and ** patterns.
		if !matched {
			if starP >= 0 {
				// We are currently trying to match a single asterisk.
				// Try to advance text one character and reset pattern.
				if t > tend {
					// No more text.
					return false
				}
				if text[t] == '/' {
					starP = -1
					starT = -1
				} else {
					t = starT
					t += 1
					starT = t
					p = starP
					matched = true
				}
			}
			if starP < 0 && starStarP >= 0 {
				// We are currently trying to match a double asterisk.
				// Try to advance text to the next path component and reset pattern.
				t = starStarT
				p = starStarP
				for t < tend && text[t] != '/' {
					t += 1
				}
				if t >= tend {
					// No more path components.
					return false
				}
				t += 1
				starStarT = t
				matched = true
			}
			if starP < 0 && starStarP < 0 {
				// No backtracking for you! We failed.
				return false
			}
		}

		// Test if we are done.
		if p > pend {
			if t > tend {
				// Fully consumed both pattern and text.
				return !mustMatchDir || isDir
			}
			if text[t] == '/' {
				// Pattern matched a directory component.
				return true
			}
			// Pattern is exhausted but text isn't — might need to backtrack.
			matched = false
			continue
		}

		// Advance the pattern and try to match.
		pc := pattern[p]
		switch pc {
		case '\\':
			// Escape the next character.
			// Escaping is lax, i.e. a backslash can precede any character.
			p += 1
			if p > pend || t > tend {
				// Invalid pattern with a trailing backslash.
				return false
			}
			matched = text[t] == pattern[p]
			if matched {
				p += 1
				t += 1
			}
		case '?':
			p += 1
			if t > tend {
				return false
			}
			// "?" never matches "/".
			matched = text[t] != '/'
			if matched {
				t += 1
			}
		case '[':
			p += 1
			if p > pend || t > tend {
				// Invalid pattern trailing bracket or no more text.
				return false
			}

			// Determine whether to negate the character class.
			negate := false
			pc := pattern[p]
			// Even though not mentioned in the Git documentation, "^" also negates.
			if pc == '!' || pc == '^' {
				negate = true
				p += 1
				if p <= pend && pattern[p] == ']' {
					// This is an empty character class.
					matched = false
					p += 1
					continue
				}
			}

			rangeStart := byte(0)
			tc := text[t]
			matched = false
			escaped := false

			for p <= pend {
				pc := pattern[p]
				p += 1
				if pc == '\\' && !escaped { //nolint:gocritic
					escaped = true
					continue
				} else if pc == ']' && !escaped {
					break
				} else {
					escaped = false
				}
				switch {
				case rangeStart > 0:
					// A range ("a-z") has been started. We already know the
					// lower bound. The upper bound is the current pattern character.
					rangeEnd := pc
					if tc >= rangeStart && tc <= rangeEnd {
						matched = true
					}
					rangeStart = 0
				case p <= pend && pattern[p] == '-':
					// A range ("a-z") has been detected. The current pattern character
					// is the lower bound.
					p += 1
					rangeStart = pc
				case p <= pend && pc == '[' && pattern[p] == ':':
					// This might be a POSIX character class.
					posixStartP := p
					p += 2
					for p < pend && pattern[p] != ']' {
						p += 1
					}
					if p == pend || pattern[p-1] != ':' {
						// Not a POSIX character class, treat it like a normal character.
						p = posixStartP
						if tc == pc {
							matched = true
						}
						continue
					}
					p += 1
					switch string(pattern[posixStartP+1 : p-2]) {
					case "alnum":
						if IsAlnum(tc) {
							matched = true
						}
					case "alpha":
						if IsAlpha(tc) {
							matched = true
						}
					case "blank":
						if IsBlank(tc) {
							matched = true
						}
					case "cntrl":
						if IsCntrl(tc) {
							matched = true
						}
					case "digit":
						if IsDigit(tc) {
							matched = true
						}
					case "graph":
						if IsGraph(tc) {
							matched = true
						}
					case "lower":
						if IsLower(tc) {
							matched = true
						}
					case "print":
						if IsPrint(tc) {
							matched = true
						}
					case "punct":
						if IsPunct(tc) {
							matched = true
						}
					case "space":
						if IsSpace(tc) {
							matched = true
						}
					case "upper":
						if IsUpper(tc) {
							matched = true
						}
					case "xdigit":
						if IsXDigit(tc) {
							matched = true
						}
					default:
						// Unknown POSIX character class fails the whole pattern.
						// See https://github.com/git/git/blob/master/wildmatch.c#L267
						return false
					}
				case pc == tc:
					matched = true
				}
			}
			if negate {
				matched = !matched
			}
			t += 1
		case '*':
			p += 1
			if p > pend {
				// Trailing "*" matches everything.
				return true
			}
			switch pattern[p] {
			case '/':
				p += 1
				// Match everything until the next path component.
				for t < tend && text[t] != '/' {
					t += 1
				}
				if t == tend {
					// No more path components.
					return false
				}
				t += 1
			case '*':
				p += 1
				if p > pend {
					// Trailing "**" matches everything.
					return true
				}
				// Consume all consecutive asterisks.
				for p < pend && pattern[p] == '*' {
					p += 1
				}
				if pattern[p] == '/' {
					// **/ matches all paths components.
					p += 1
					starStarP = p
					starStarT = t
				} else {
					// If anything else comes next, we treat the "**" as a single "*".
					starP = p
					starT = t
				}
			default:
				starP = p
				starT = t
			}
		default:
			if t > tend {
				matched = false
				continue
			}
			// Match literal and path component.
			tc := text[t]
			if tc == '/' {
				if pc == '/' {
					t += 1
					p += 1
				} else {
					matched = false
				}
			} else {
				matched = tc == pc
				if matched {
					t += 1
					p += 1
				}
			}
		}
	}
}

type ExtendedGlobPattern struct {
	GlobPattern
	// If a negation pattern is detected by the "!" prefix, this is set to true
	// AND the leading "!" is removed from the pattern.
	IsNegate bool
	BaseDir  string
}

func NewExtendedGlobPattern(pattern string, baseDir string) ExtendedGlobPattern {
	if pattern == "" {
		return ExtendedGlobPattern{GlobPattern: GlobPattern{}, IsNegate: false, BaseDir: baseDir}
	}
	isNegate := pattern[0] == '!'
	if isNegate {
		pattern = pattern[1:]
	}
	return ExtendedGlobPattern{PrepareGlobPattern(pattern), isNegate, baseDir}
}

type ExtendedGlobPatterns []ExtendedGlobPattern

// Parse a `.gitignore` or `.clingignore` file.
func ParseGlobIgnoreFile(dir string, patterns []string) ExtendedGlobPatterns {
	if dir == "." {
		dir = ""
	}
	if dir != "" && !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	ignorePatterns := []ExtendedGlobPattern{}
	for _, pattern := range patterns {
		if len(strings.TrimSpace(pattern)) == 0 {
			continue
		}
		if pattern[0] == '#' {
			continue
		}
		ignorePatterns = append(ignorePatterns, NewExtendedGlobPattern(pattern, dir))
	}
	return ignorePatterns
}

func (i ExtendedGlobPatterns) Match(path string, isDir bool) bool {
	matched := false
	for _, pattern := range i {
		if !strings.HasPrefix(path, pattern.BaseDir) {
			continue
		}
		relPath, err := filepath.Rel(pattern.BaseDir, path)
		if err != nil {
			continue
		}
		if relPath == "." {
			continue
		}
		if GlobMatch(pattern.GlobPattern, []byte(relPath), isDir) {
			matched = !pattern.IsNegate
		}
	}
	return matched
}

// Same as `fs.WalkDir`, but will respect all `.gitignore` and `.clingignore` files along the way.
func WalkDirIgnore(fs FS, dir string, f iofs.WalkDirFunc) error {
	ignorePatterns := ExtendedGlobPatterns{}
	return fs.WalkDir(dir, func(path string, d iofs.DirEntry, err error) error { //nolint:wrapcheck
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Actively search for ignore files.
			for _, ignoreFileName := range ignoreFileNames {
				ignoreFilePath := filepath.Join(path, ignoreFileName)
				content, err := ReadFile(fs, ignoreFilePath)
				if err != nil {
					if !errors.Is(err, iofs.ErrNotExist) {
						return WrapErrorf(err, "Failed to read ignore file %s", ignoreFilePath)
					}
				} else {
					parsed := ParseGlobIgnoreFile(path, strings.Split(string(content), "\n"))
					ignorePatterns = append(ignorePatterns, parsed...)
				}
			}
		}
		ignored := ignorePatterns.Match(path, d.IsDir())
		if ignored {
			if d.IsDir() {
				// No need to recurse. If a directory is ignored, none
				// of its contents can be included again.
				return filepath.SkipDir
			}
			return nil
		}
		return f(path, d, nil)
	})
}

func IsAlnum(c byte) bool {
	return IsAlpha(c) || IsDigit(c)
}

func IsAlpha(c byte) bool {
	return IsLower(c) || IsUpper(c)
}

func IsBlank(c byte) bool {
	return c == ' ' || c == '\t'
}

func IsCntrl(c byte) bool {
	return c <= 0x1f || c == 0x7f
}

func IsDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func IsGraph(c byte) bool {
	return IsPrint(c) && c != ' '
}

func IsLower(c byte) bool {
	return c >= 'a' && c <= 'z'
}

func IsPrint(c byte) bool {
	return c >= 0x20 && c <= 0x7e
}

func IsPunct(c byte) bool {
	return c >= 0x21 && c <= 0x2f || c >= 0x3a && c <= 0x40 || c >= 0x5b && c <= 0x60 || c >= 0x7b && c <= 0x7e
}

func IsSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

func IsUpper(c byte) bool {
	return c >= 'A' && c <= 'Z'
}

func IsXDigit(c byte) bool {
	return IsDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
