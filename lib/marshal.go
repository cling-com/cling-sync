package lib

import (
	"encoding/base32"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// This is a very rudimentary TOML reader/writer. It only supports what
// is really needed.
type Toml map[string]map[string]string

// Return `value, true` if the key exists, `"", false` otherwise.
func (t Toml) GetValue(section string, key string) (string, bool) {
	if kvs, ok := t[section]; ok {
		if value, ok := kvs[key]; ok {
			return value, true
		}
	}
	return "", false
}

func (t Toml) GetIntValue(section string, key string) (int, bool) {
	if value, ok := t.GetValue(section, key); ok {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue, true
		}
	}
	return 0, false
}

func (t Toml) Eq(other Toml) bool {
	if t == nil || other == nil {
		return t == nil && other == nil
	}
	if len(t) != len(other) {
		return false
	}
	for section, kvs := range t {
		otherKvs, ok := other[section]
		if !ok {
			return false
		}
		if len(kvs) != len(otherKvs) {
			return false
		}
		for key, value := range kvs {
			otherValue, ok := otherKvs[key]
			if !ok || value != otherValue {
				return false
			}
		}
	}
	return true
}

// Sections and keys within sections are sorted alphabetically.
func WriteToml(dst io.Writer, headerComment string, toml Toml) error {
	if headerComment != "" {
		for i, line := range strings.Split(headerComment, "\n") {
			if i > 0 {
				if _, err := fmt.Fprintf(dst, "\n"); err != nil {
					return WrapErrorf(err, "failed to write header comment")
				}
			}
			if _, err := fmt.Fprintf(dst, "# %s", strings.TrimSpace(line)); err != nil {
				return WrapErrorf(err, "failed to write header comment")
			}
		}
		if len(toml) > 0 {
			if _, err := fmt.Fprintf(dst, "\n\n"); err != nil {
				return WrapErrorf(err, "failed to write header comment")
			}
		}
	}
	for i, section := range slices.Sorted(maps.Keys(toml)) {
		if i > 0 {
			if _, err := fmt.Fprintf(dst, "\n\n"); err != nil {
				return WrapErrorf(err, "failed to write section header %q", section)
			}
		}
		if _, err := fmt.Fprintf(dst, "[%s]", section); err != nil {
			return WrapErrorf(err, "failed to write section header %q", section)
		}
		kvs := toml[section]
		if len(kvs) > 0 {
			if _, err := fmt.Fprintf(dst, "\n"); err != nil {
				return WrapErrorf(err, "failed to write section header %q", section)
			}
		}
		for j, k := range slices.Sorted(maps.Keys(kvs)) {
			if j > 0 {
				if _, err := fmt.Fprintf(dst, "\n"); err != nil {
					return WrapErrorf(err, "failed to write key %q in section %q", k, section)
				}
			}
			if _, err := fmt.Fprintf(dst, "%s = %q", k, kvs[k]); err != nil {
				return WrapErrorf(err, "failed to write key %q in section %q", k, section)
			}
		}
	}
	if _, err := fmt.Fprintf(dst, "\n"); err != nil {
		return WrapErrorf(err, "failed add trailing newline")
	}
	return nil
}

func ReadToml(src io.Reader) (Toml, error) {
	buf, err := io.ReadAll(src)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read toml")
	}
	sections := make(map[string]map[string]string)
	var currentSection map[string]string
	for line := range strings.SplitSeq(string(buf), "\n") {
		line = strings.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		if line[0] == '[' {
			if line[len(line)-1] != ']' {
				return nil, Errorf("invalid section header: %s", line)
			}
			sectionName := line[1 : len(line)-1]
			currentSection = make(map[string]string)
			sections[sectionName] = currentSection

			continue
		} else if before, after, ok := strings.Cut(line, "="); ok {
			if currentSection == nil {
				return nil, Errorf("unexpected key-value pair outside of section: %s", line)
			}
			key := strings.TrimSpace(before)
			value := strings.TrimSpace(after)
			if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
				return nil, Errorf("invalid value: %s", line)
			}
			currentSection[key] = value[1 : len(value)-1]
		} else {
			return nil, Errorf("invalid line: %s", line)
		}
	}
	return sections, nil
}

// If the data length is not divisible by 4 then the last block will be shortened.
func FormatRecoveryCode(data []byte) string {
	encoding := base32.StdEncoding.WithPadding(base32.NoPadding)
	encoded := encoding.EncodeToString(data)
	chunkSize := 4
	var chunks []string
	for i := 0; i < len(encoded); i += chunkSize {
		chunks = append(chunks, encoded[i:min(i+chunkSize, len(encoded))])
	}
	return strings.Join(chunks, "-")
}

func ParseRecoveryCode(s string) ([]byte, error) {
	encoding := base32.StdEncoding.WithPadding(base32.NoPadding)
	decoded, err := encoding.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil {
		return nil, WrapErrorf(err, "failed to decode recovery code %q", s)
	}
	return decoded, nil
}
