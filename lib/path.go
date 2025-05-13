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
	return match(p, pp.parts)
}

func match(p string, parts []string) bool {
	pi := 0
	i := 0
	for pi < len(parts) && i < len(p) {
		part := parts[pi]
		switch part {
		case "?":
			if p[i] == '/' {
				// `?` should not match the path delimiter.
				return false
			}
			i += 1
		case "*":
			if pi == len(parts)-1 {
				// `*` is the last part, so it can match anything.
				return !strings.Contains(p[i:], "/")
			}
			subparts := parts[pi+1:]
			for i < len(p) && p[i] != '/' {
				if match(p[i:], subparts) {
					return true
				}
				i += 1
			}
			return match(p[i:], subparts)
		case "**":
			if i > 0 && p[i] != '/' {
				return false
			}
			subparts := parts[pi+1:]
			if len(subparts) == 0 {
				return true
			}
			for i < len(p) {
				if match(p[i:], subparts) {
					return true
				}
				// Skip to next delimiter.
				for i < len(p) && p[i] != '/' {
					i += 1
				}
				i += 1
			}
			return false
		default:
			if !strings.HasPrefix(p[i:], part) {
				return false
			}
			i += len(part)
		}
		pi += 1
	}
	if i != len(p) {
		return false
	}
	if pi == len(parts) {
		return true
	}
	if pi == len(parts)-1 && parts[pi][0] == '*' {
		return true
	}
	return false
}
