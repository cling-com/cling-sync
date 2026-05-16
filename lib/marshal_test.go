package lib

import (
	"crypto/rand"
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
key3 = "value3"
`
		assert.Equal(expected, output.String())
		parsed, err := ReadToml(strings.NewReader(output.String()))
		assert.NoError(err)
		assert.Equal(toml, parsed)
	})
}

func TestTomlEq(t *testing.T) {
	t.Parallel()
	t.Run("Nil", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		assert.Equal(true, Toml(nil).Eq(nil))
		assert.Equal(false, Toml(nil).Eq(Toml{}))
		assert.Equal(false, Toml{}.Eq(nil))
	})

	t.Run("Equal with different map order", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		a := Toml{
			"section2": {
				"key3": "value3",
			},
			"section1": {
				"key2": "value2",
				"key1": "value1",
			},
		}
		b := Toml{
			"section1": {
				"key1": "value1",
				"key2": "value2",
			},
			"section2": {
				"key3": "value3",
			},
		}
		assert.Equal(true, a.Eq(b))
		assert.Equal(true, b.Eq(a))
	})

	t.Run("Different value", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		a := Toml{"section": {"key": "value1"}}
		b := Toml{"section": {"key": "value2"}}
		assert.Equal(false, a.Eq(b))
	})

	t.Run("Different key", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		a := Toml{"section": {"key1": "value"}}
		b := Toml{"section": {"key2": "value"}}
		assert.Equal(false, a.Eq(b))
	})

	t.Run("Different section", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		a := Toml{"section1": {"key": "value"}}
		b := Toml{"section2": {"key": "value"}}
		assert.Equal(false, a.Eq(b))
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

func FuzzReadToml(f *testing.F) {
	f.Add("")
	f.Add("[encryption]\nversion = \"1\"\n")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = ReadToml(strings.NewReader(s))
	})
}
