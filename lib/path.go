package lib

import (
	"fmt"
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
	f, err := NewPathInclusionFilter([]string{p.String() + "/**"})
	if err != nil {
		panic(fmt.Sprintf("failed to create path filter for %s", p))
	}
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

// A pattern that matches against a file path.
type PathPattern struct {
	parts []string
}

func NewPathPattern(pattern string) (PathPattern, error) {
	if pattern == "" {
		return PathPattern{}, Errorf("invalid pattern (empty): %s", pattern)
	}
	parts := []string{}
	i := 0
	for i < len(pattern) {
		c := pattern[i]
		i += 1
		switch c {
		case '?':
			parts = append(parts, "?")
		case '*':
			if i < len(pattern) && pattern[i] == '*' {
				i += 1
				if i > 2 && pattern[i-3] != '/' {
					return PathPattern{}, Errorf(
						"invalid pattern (** must be preceded by / or placed at the start): %s",
						pattern,
					)
				}
				if i < len(pattern) && pattern[i] != '/' {
					return PathPattern{}, Errorf(
						"invalid pattern (** must be followed by / or placed at the end): %s",
						pattern,
					)
				}
				parts = append(parts, "**")
			} else {
				parts = append(parts, "*")
			}
		default:
			text := string(c)
			for i < len(pattern) {
				c := pattern[i]
				if c == '?' || c == '*' {
					break
				}
				text += string(c)
				i += 1
			}
			parts = append(parts, text)
		}
	}
	for i, part := range parts {
		if i < len(parts)-1 && strings.HasSuffix(part, "/") && parts[i+1] == "**" {
			// Remove the trailing `/` from the previous part.
			part = strings.TrimSuffix(part, "/")
			parts[i] = part
		}
		if i > 0 && strings.HasPrefix(part, "/") && parts[i-1] == "**" {
			// Remove the leading `/` from the previous part.
			parts[i] = strings.TrimPrefix(part, "/")
		}
	}
	return PathPattern{parts: parts}, nil
}

func NewPathPatters(patterns []string) ([]PathPattern, error) {
	pp := []PathPattern{}
	for _, pattern := range patterns {
		p, err := NewPathPattern(pattern)
		if err != nil {
			return nil, err
		}
		pp = append(pp, p)
	}
	return pp, nil
}

func (pp PathPattern) Match(p Path) bool {
	if p.p == "" {
		return false
	}
	return match(p.p, pp.parts)
}

func match(path string, parts []string) bool { //nolint:funlen
	partIndex := 0
	pathIndex := 0
	for partIndex < len(parts) && pathIndex < len(path) {
		part := parts[partIndex]
		switch part {
		case "?":
			if path[pathIndex] == '/' {
				// `?` should not match the path delimiter.
				return false
			}
			pathIndex += 1
		case "*":
			if partIndex == len(parts)-1 {
				// `*` is the last part, so it can match anything,
				// except if we are at the beginning and the first character is a `/`.
				return pathIndex > 0 || path[0] != '/'
			}
			subparts := parts[partIndex+1:]
			for pathIndex < len(path) && path[pathIndex] != '/' {
				if match(path[pathIndex:], subparts) {
					return true
				}
				pathIndex += 1
			}
			return match(path[pathIndex:], subparts)
		case "**":
			if pathIndex > 0 && path[pathIndex] != '/' {
				return false
			}
			subparts := parts[partIndex+1:]
			if len(subparts) == 0 {
				return true
			}
			for pathIndex < len(path) {
				if match(path[pathIndex:], subparts) {
					return true
				}
				// Skip to next delimiter.
				for pathIndex < len(path) && path[pathIndex] != '/' {
					pathIndex += 1
				}
				pathIndex += 1
			}
			return false
		default:
			if !strings.HasPrefix(path[pathIndex:], part) {
				return false
			}
			pathIndex += len(part)
		}
		partIndex += 1
	}
	if partIndex < len(parts) {
		if partIndex == len(parts)-1 && parts[partIndex] == "*" {
			return true
		}
		return false
	}
	if pathIndex == len(path) {
		// This is a full match.
		return true
	}
	if path[pathIndex] == '/' {
		// This is a directory match. Everything below will match.
		return true
	}
	return false
}

type PathFilter interface {
	Include(p Path) bool
}

// A PathFilter that can exclude paths.
// A path is excluded if it matches any of the exclude patterns and none of the include patterns.
// So the include patterns are only used to override exclude patterns.
type PathExclusionFilter struct {
	Excludes []PathPattern
	Includes []PathPattern
}

// Parse the exclude and include patterns and create a PathFilter.
func NewPathExclusionFilter(excludes []string, includes []string) (*PathExclusionFilter, error) {
	e, err := NewPathPatters(excludes)
	if err != nil {
		return nil, err
	}
	i, err := NewPathPatters(includes)
	if err != nil {
		return nil, err
	}
	return &PathExclusionFilter{Excludes: e, Includes: i}, nil
}

func (pef *PathExclusionFilter) Include(p Path) bool {
	for _, exclude := range pef.Excludes {
		if exclude.Match(p) {
			for _, include := range pef.Includes {
				if include.Match(p) {
					return true
				}
			}
			return false
		}
	}
	return true
}

type PathInclusionFilter struct {
	Includes []PathPattern
}

func NewPathInclusionFilter(includes []string) (*PathInclusionFilter, error) {
	patterns, err := NewPathPatters(includes)
	if err != nil {
		return nil, err
	}
	return &PathInclusionFilter{Includes: patterns}, nil
}

func (pif *PathInclusionFilter) Include(p Path) bool {
	for _, include := range pif.Includes {
		if include.Match(p) {
			return true
		}
	}
	return false
}

// A PathFilter that combines multiple PathFilters.
// It returns true if *all* of the PathFilters returns true.
type AllPathFilter struct {
	Filters []PathFilter
}

func (cpf *AllPathFilter) Include(p Path) bool {
	for _, filter := range cpf.Filters {
		if !filter.Include(p) {
			return false
		}
	}
	return true
}
