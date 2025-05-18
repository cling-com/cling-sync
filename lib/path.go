package lib

import (
	"bytes"
	"strings"
)

const PathDelim = "/"

type (
	Path    string
	PathKey string
)

func NewPath(parts ...string) Path {
	var sb strings.Builder
	for i, part := range parts {
		if i > 0 {
			sb.WriteString(PathDelim)
		}
		sb.WriteString(escapePathPart(part))
	}
	return Path(sb.String())
}

// Return the path with all `%25` converted back to `%` but keeping all
// escaped `/` at `%2f`.
func (p Path) FSString() string {
	return strings.ReplaceAll(string(p), "%25", "%")
}

// Replace `/` with `%2f` and `%` with `%25`.
func escapePathPart(s string) string {
	b := []byte(s)
	b = bytes.ReplaceAll(b, []byte("%"), []byte("%25"))
	b = bytes.ReplaceAll(b, []byte("/"), []byte("%2f"))
	return string(b)
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

func (pp PathPattern) Match(p string) bool {
	if p == "" {
		return false
	}
	return match(p, pp.parts)
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
	Include(p string) bool
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
	e := []PathPattern{}
	for _, pattern := range excludes {
		pp, err := NewPathPattern(pattern)
		if err != nil {
			return nil, err
		}
		e = append(e, pp)
	}
	i := []PathPattern{}
	for _, pattern := range includes {
		pp, err := NewPathPattern(pattern)
		if err != nil {
			return nil, err
		}
		i = append(i, pp)
	}
	return &PathExclusionFilter{Excludes: e, Includes: i}, nil
}

func (pef *PathExclusionFilter) Include(p string) bool {
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

func (pif *PathInclusionFilter) Include(p string) bool {
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

func (cpf *AllPathFilter) Include(p string) bool {
	for _, filter := range cpf.Filters {
		if !filter.Include(p) {
			return false
		}
	}
	return true
}
