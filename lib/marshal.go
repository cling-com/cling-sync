package lib

import (
	"encoding/base32"
	"encoding/binary"
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
	for _, line := range strings.Split(string(buf), "\n") {
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
		} else if idx := strings.Index(line, "="); idx != -1 {
			if currentSection == nil {
				return nil, Errorf("unexpected key-value pair outside of section: %s", line)
			}
			key := strings.TrimSpace(line[:idx])
			value := strings.TrimSpace(line[idx+1:])
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

// Wrapper around `encoding/binary` that encapsulates repetitive error handling and offers
// some convenience methods.
type BinaryWriter struct {
	Err error
	w   io.Writer
}

func NewBinaryWriter(w io.Writer) BinaryWriter {
	return BinaryWriter{nil, w}
}

func (bw *BinaryWriter) Write(data any) {
	if bw.Err != nil {
		return
	}
	bw.Err = binary.Write(bw.w, binary.LittleEndian, data)
}

func (bw *BinaryWriter) WriteString(s string) {
	if bw.Err != nil {
		return
	}
	b := []byte(s)
	bw.WriteLen(len(b))
	if bw.Err != nil {
		return
	}
	bw.Err = binary.Write(bw.w, binary.LittleEndian, b)
}

// Write the length of either a string or a slice. It does bounds checking, i.e. `len` must fit
// into a uint16.
func (bw *BinaryWriter) WriteLen(l int) {
	if bw.Err != nil {
		return
	}
	if l < 0 {
		bw.Err = Errorf("length must be positive: %d", l)
		return
	}
	if l >= 1<<16 {
		bw.Err = Errorf("length too long: %d", l)
		return
	}
	bw.Err = binary.Write(bw.w, binary.LittleEndian, uint16(l)) //nolint:gosec
}

type BinaryReader struct {
	Err error
	r   io.Reader
}

func NewBinaryReader(r io.Reader) BinaryReader {
	return BinaryReader{nil, r}
}

func (br *BinaryReader) Read(data any) {
	if br.Err != nil {
		return
	}
	br.Err = binary.Read(br.r, binary.LittleEndian, data)
}

func (br *BinaryReader) Skip(n int) {
	if br.Err != nil {
		return
	}
	if n < 0 {
		br.Err = Errorf("length must be positive: %d", n)
		return
	}
	if n >= 1<<16 {
		br.Err = Errorf("length too long: %d", n)
		return
	}
	if _, err := io.CopyN(io.Discard, br.r, int64(n)); err != nil {
		br.Err = err
	}
}

func (br *BinaryReader) ReadString() string {
	if br.Err != nil {
		return ""
	}
	l := br.ReadLen()
	if l == 0 {
		return ""
	}
	b := make([]byte, l)
	br.Read(b)
	if br.Err != nil {
		return ""
	}
	return string(b)
}

// Return 0 if there was an error.
func (br *BinaryReader) ReadLen() int {
	if br.Err != nil {
		return 0
	}
	var l uint16
	br.Read(&l)
	if br.Err != nil {
		return 0
	}
	return int(l)
}
