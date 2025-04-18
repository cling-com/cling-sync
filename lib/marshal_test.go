package lib

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

func TestTomlReadWrite(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		toml := Toml{
			"section2": {
				"key3": "value3",
			},
			"section1": {
				"key2": "value2",
				"key1": "value1",
			},
		}
		var output strings.Builder
		err := WriteToml(&output, "Header comment\n  with two lines and white-space ", toml)
		assert.NoError(err)
		// Note how this is sorted.
		expected := `# Header comment
# with two lines and white-space

[section1]
key1 = "value1"
key2 = "value2"

[section2]
key3 = "value3"`
		assert.Equal(expected, output.String())
		parsed, err := ReadToml(strings.NewReader(output.String()))
		assert.NoError(err)
		assert.Equal(toml, parsed)
	})
}

func TestRecoveryCode(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		formatted := FormatRecoveryCode([]byte("this is my test string"))
		assert.Equal("ORUG-S4ZA-NFZS-A3LZ-EB2G-K43U-EBZX-I4TJ-NZTQ", formatted)
		parsed, err := ParseRecoveryCode(formatted)
		assert.NoError(err)
		assert.Equal("this is my test string", string(parsed))
	})
	t.Run("Fuzzy test", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		for i := range 1000 {
			original := make([]byte, i)
			_, err := io.ReadFull(rand.Reader, original)
			assert.NoError(err)
			formatted := FormatRecoveryCode(original)
			parsed, err := ParseRecoveryCode(formatted)
			assert.NoError(err)
			assert.Equal(original, parsed)
		}
	})
}

func TestBinaryWriter(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := NewBinaryWriter(&buf)
		sut.Write(int8(0x41))
		sut.Write(int8(0x20))
		sut.Write([]byte("string"))
		assert.NoError(sut.Err)
		assert.Equal("A string", buf.String())
	})
	t.Run("WriteString", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := NewBinaryWriter(&buf)
		// Test with Unicode characters from different ranges.
		input := "ASCII 💡 中文 кириллица العربية" //nolint:gosmopolitan
		sut.WriteString(input)
		assert.NoError(sut.Err)
		lengthBytes := buf.Bytes()[0:2]
		length := binary.LittleEndian.Uint16(lengthBytes)
		payload := buf.Bytes()[2:]
		assert.Equal(input, string(payload))
		assert.Equal(int(length), len(payload))
	})
	t.Run("WriteLen", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := NewBinaryWriter(&buf)
		sut.WriteLen(1)
		assert.NoError(sut.Err)
		sut.WriteLen(-1)
		assert.Error(sut.Err, "length must be positive: -1")
		sut.Err = nil
		sut.WriteLen(65535)
		assert.NoError(sut.Err)
		sut.WriteLen(65536)
		assert.Error(sut.Err, "length too long: 65536")
	})
}

func TestBinaryReader(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		buf := bytes.NewBuffer([]byte{0x41, 0x06, 0x00, 's', 't', 'r', 'i', 'n', 'g'})
		sut := NewBinaryReader(buf)
		var b uint8
		sut.Read(&b)
		assert.NoError(sut.Err)
		assert.Equal(uint8(0x41), b)
		s := sut.ReadString()
		assert.NoError(sut.Err)
		assert.Equal("string", s)
	})
	t.Run("ReadLen returns 0 in case of an error", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		buf := bytes.NewBuffer([]byte{0x10, 0x00})
		sut := NewBinaryReader(buf)
		// Simulate a previous call has failed.
		sut.Err = Errorf("previous error")
		l := sut.ReadLen()
		assert.Equal(0, l)
	})
	t.Run("ReadString returns an empty string in case of an error", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		bw := NewBinaryWriter(&buf)
		bw.WriteString("string")
		sut := NewBinaryReader(&buf)
		// Simulate a previous call has failed.
		sut.Err = Errorf("previous error")
		s := sut.ReadString()
		assert.Equal("", s)
	})
}
