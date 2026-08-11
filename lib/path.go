package lib

import (
	"path/filepath"
	"strings"
)

const PathDelim = "/"

// MaxPathLen is the maximum allowed length (in bytes) for a `Path`.
// 4096 matches Linux PATH_MAX (including its terminating NUL) and is well
// above macOS (1024) and the Windows non-extended limit (260).
const MaxPathLen = 4096

type Path struct {
	p string
}

func NewPath(path string) (Path, error) {
	if len(path) > MaxPathLen {
		return Path{}, Errorf("invalid path, must not exceed %d bytes (got %d)", MaxPathLen, len(path))
	}
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "./") || path == "." || strings.HasPrefix(path, "..") {
		return Path{}, Errorf("invalid path %q, must not be relative", path)
	}
	if strings.HasPrefix(path, "../") {
		return Path{}, Errorf("invalid path %q, must not start with `.`", path)
	}
	if strings.HasPrefix(path, "/") {
		return Path{}, Errorf("invalid path %q, must not start with `/`", path)
	}
	if len(path) > 1 && path[1] == ':' {
		return Path{}, Errorf("invalid path %q, must not contain volume name", path)
	}
	if path != "" && strings.HasSuffix(path, "/") {
		return Path{}, Errorf("invalid path %q, must not end with `/`", path)
	}
	if path != "" && filepath.Clean(path) != path {
		return Path{}, Errorf("invalid path %q, must not contain `.` or `..`", path)
	}
	return Path{path}, nil
}

// Wrap a string that is already known to be a valid path, such as one derived
// from another `Path`. Use `NewPath` for anything coming from outside.
func NewPathUnchecked(path string) Path {
	return Path{path}
}

func (p Path) String() string {
	return p.p
}

func (p Path) Base() Path {
	if p.p == "" {
		return Path{""}
	}
	return NewPathUnchecked(filepath.Base(p.p))
}

func (p Path) Dir() Path {
	d := filepath.Dir(p.p)
	if d == "." {
		return Path{""}
	}
	return NewPathUnchecked(d)
}

func (p Path) Len() int {
	return len(p.p)
}

// The number of path segments. The empty path has depth 0.
func (p Path) Depth() int {
	if p.p == "" {
		return 0
	}
	return strings.Count(p.p, PathDelim) + 1
}

func (p Path) IsEmpty() bool {
	return p.p == ""
}

func (p Path) IsRelativeTo(base Path) bool {
	if len(p.p) == len(base.p) {
		return false
	}
	b := base.p
	if !strings.HasSuffix(b, "/") {
		b += "/"
	}
	return strings.HasPrefix(p.p, b)
}

// Trim the base path from the beginning of the path.
// Return the trimmed path and a boolean indicating whether the path was trimmed.
func (p Path) TrimBase(base Path) (Path, bool) {
	if len(base.p) == 0 {
		return p, true
	}
	if len(p.p) <= len(base.p) {
		return p, false
	}
	b := base.p
	if !strings.HasSuffix(b, "/") {
		b += "/"
	}
	result := strings.TrimPrefix(p.p, b)
	if len(result) == len(p.p) {
		return p, false
	}
	return NewPathUnchecked(result), true
}

func (p Path) Join(other Path) Path {
	return NewPathUnchecked(filepath.Join(p.p, other.p))
}

type PathFilter interface {
	Include(p Path, isDir bool) bool
}

// A PathFilter that can exclude paths.
// A path is excluded if it matches any of the exclude patterns and none of the include patterns.
// So the include patterns are only used to override exclude patterns.
//
// A nil `*PathExclusionFilter` means no filter and is safe to call: it keeps
// every path. Keep it nil rather than empty, so that the code asking whether a
// filter is in effect at all can tell the difference.
type PathExclusionFilter struct {
	Excludes ExtendedGlobPatterns
}

// Parse the exclude and include patterns and create a PathFilter.
func NewPathExclusionFilter(excludes []string) *PathExclusionFilter {
	e := make(ExtendedGlobPatterns, len(excludes))
	for i, pattern := range excludes {
		e[i] = NewExtendedGlobPattern(pattern, "")
	}
	return &PathExclusionFilter{e}
}

func (pef *PathExclusionFilter) Include(p Path, isDir bool) bool {
	if pef == nil {
		return true
	}
	return !pef.Excludes.Match(p.p, isDir)
}

// A PathFilter that keeps only the paths matching one of the include patterns.
//
// A nil `*PathInclusionFilter` means no filter and is safe to call: it keeps
// every path. An empty one is not the same thing, it matches nothing and so
// drops every path.
type PathInclusionFilter struct {
	Includes ExtendedGlobPatterns
}

func NewPathInclusionFilter(includes []string) *PathInclusionFilter {
	patterns := make(ExtendedGlobPatterns, len(includes))
	for i, pattern := range includes {
		patterns[i] = NewExtendedGlobPattern(pattern, "")
	}
	return &PathInclusionFilter{patterns}
}

func (pif *PathInclusionFilter) Include(p Path, isDir bool) bool {
	if pif == nil {
		return true
	}
	return pif.Includes.Match(p.p, isDir)
}

// Order two paths according to this sorting order:
//   - directory
//   - files inside the directory
//   - sub-directory
//   - files inside the sub-directory
//   - ...
//
// Example:
//   - a.txt
//   - z.txt
//   - sub/
//   - sub/a.txt
//   - sub/z.txt
//   - sub/sub/
//   - sub/sub/a.txt
//   - sub/sub/z.txt
//
// A file at the root has no directory to sit in. It sorts as though its name
// began with a `0`, so a directory named `.git` comes before it and `zzz` after.
// There is no particular reason for this other than it being a bug in a previous
// version.
func PathCompare(a Path, aIsDir bool, b Path, bIsDir bool) int {
	// todo: All of this should be a simple lexicographical compare with a special
	//       case that takes `aIsDir` and `bIsDir` into account on equal paths.
	//		 But this would change the on-disk format of existing repositories.
	as, bs := a.String(), b.String()
	// The separator before a file's last segment, the only position where a
	// file sorts differently from a directory. Directories have none and keep
	// -1, which no index in the loop below can match.
	aSep, bSep := -1, -1
	if !aIsDir {
		aSep = strings.LastIndexByte(as, '/')
	}
	if !bIsDir {
		bSep = strings.LastIndexByte(bs, '/')
	}
	// The previous implementation sorted a root-level file as if its name began
	// with a `0`. That is a bug: the byte lands in the middle of the alphabet,
	// so a directory named below it (`.git`) sorts ahead of every root file
	// while one above it (`zzz`) sorts behind. Revisions are stored in that
	// order, so compare against the literal `0` here. After it the names line
	// up and the loop below decides the rest. This is purely for backwards
	// compatibility.
	aRootFile, bRootFile := !aIsDir && aSep < 0, !bIsDir && bSep < 0
	if aRootFile != bRootFile {
		file, other, sign := as, bs, 1
		if bRootFile {
			file, other, sign = bs, as, -1
		}
		switch {
		case len(other) == 0 || other[0] < '0':
			return sign
		case other[0] > '0':
			return -sign
		}
		// The file has no separator, so nothing past the `0` can distinguish it
		// from a directory and the rest is a plain byte comparison.
		if c := strings.Compare(file, other[1:]); c != 0 {
			return sign * c
		}
		// The two names collide. Order the file first so they never compare equal.
		return -sign
	}
	for i := 0; i < len(as) && i < len(bs); i++ {
		if as[i] != bs[i] {
			if as[i] < bs[i] {
				return -1
			}
			return 1
		}
		if as[i] == '/' {
			// The paths agree up to here, so all that can still differ at a
			// shared separator is whether it is a file's last one.
			aLast, bLast := i == aSep, i == bSep
			if aLast != bLast {
				if aLast {
					return -1
				}
				return 1
			}
		}
	}
	switch {
	case len(as) < len(bs):
		return -1
	case len(as) > len(bs):
		return 1
	}
	return 0
}

// A path together with its directory bit. The two identify an entry, and the
// struct is comparable, so it can key a map without building a string.
type PathKey struct {
	Path  Path
	IsDir bool
}

func ComparePathKey(a, b PathKey) int {
	return PathCompare(a.Path, a.IsDir, b.Path, b.IsDir)
}
