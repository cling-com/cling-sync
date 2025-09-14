package lib

import (
	"path/filepath"
	"strings"
)

const PathDelim = "/"

type Path struct {
	p string
}

func NewPath(path string) (Path, error) {
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "./") || path == "." || strings.HasPrefix(path, "..") {
		return Path{""}, Errorf("invalid path %q, must not be relative", path)
	}
	if strings.HasPrefix(path, "../") {
		return Path{""}, Errorf("invalid path %q, must not start with `.`", path)
	}
	if strings.HasPrefix(path, "/") {
		return Path{""}, Errorf("invalid path %q, must not start with `/`", path)
	}
	if len(path) > 1 && path[1] == ':' && path[2] == '/' {
		return Path{""}, Errorf("invalid path %q, must not contain volume name", path)
	}
	if path != "" && strings.HasSuffix(path, "/") {
		return Path{""}, Errorf("invalid path %q, must not end with `/`", path)
	}
	if path != "" && filepath.Clean(path) != path {
		return Path{""}, Errorf("invalid path %q, must not contain `.` or `..`", path)
	}
	return Path{path}, nil
}

func (p Path) String() string {
	return p.p
}

func (p Path) Base() Path {
	return Path{filepath.Base(p.p)}
}

func (p Path) Dir() Path {
	d := filepath.Dir(p.p)
	if d == "." {
		return Path{""}
	}
	return Path{d}
}

func (p Path) Len() int {
	return len(p.p)
}

func (p Path) IsEmpty() bool {
	return p.p == ""
}

func (p Path) AsFilter() PathFilter {
	if p.p == "" {
		return nil
	}
	f := NewPathInclusionFilter([]string{p.String() + "/**"})
	return f
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
	return Path{result}, true
}

func (p Path) Join(other Path) Path {
	return Path{filepath.Join(p.p, other.p)}
}

type PathFilter interface {
	Include(p Path, isDir bool) bool
}

// A PathFilter that can exclude paths.
// A path is excluded if it matches any of the exclude patterns and none of the include patterns.
// So the include patterns are only used to override exclude patterns.
type PathExclusionFilter struct {
	Excludes ExtendedGlobPatterns
}

// Parse the exclude and include patterns and create a PathFilter.
func NewPathExclusionFilter(excludes []string) *PathExclusionFilter {
	e := make(ExtendedGlobPatterns, 0, len(excludes))
	for _, pattern := range excludes {
		p := NewExtendedGlobPattern(pattern, "")
		e = append(e, p)
	}
	return &PathExclusionFilter{e}
}

func (pef *PathExclusionFilter) Include(p Path, isDir bool) bool {
	return !pef.Excludes.Match(p.p, isDir)
}

type PathInclusionFilter struct {
	Includes ExtendedGlobPatterns
}

func NewPathInclusionFilter(includes []string) *PathInclusionFilter {
	patterns := make(ExtendedGlobPatterns, 0, len(includes))
	for _, pattern := range includes {
		p := NewExtendedGlobPattern(pattern, "")
		patterns = append(patterns, p)
	}
	return &PathInclusionFilter{patterns}
}

func (pif *PathInclusionFilter) Include(p Path, isDir bool) bool {
	return pif.Includes.Match(p.p, isDir)
}

// A PathFilter that combines multiple PathFilters.
// It returns true if *all* of the PathFilters returns true.
type AllPathFilter struct {
	Filters []PathFilter
}

func (cpf *AllPathFilter) Include(p Path, isDir bool) bool {
	for _, filter := range cpf.Filters {
		if !filter.Include(p, isDir) {
			return false
		}
	}
	return true
}

// Create a string that can be used to sort two paths.
//
// The sorting order is:
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
func PathCompareString(path Path, isDir bool) string {
	p := strings.ReplaceAll(path.String(), "/", "/1")
	if isDir {
		return p
	}
	lastSlash := strings.LastIndex(p, "/")
	if lastSlash == -1 || lastSlash == len(p)-1 {
		return "0" + p
	}
	return p[:lastSlash] + "/0" + p[lastSlash+2:]
}
