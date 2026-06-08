package main

import (
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestPagerWrapLine(t *testing.T) {
	t.Parallel()
	assert := lib.NewAssert(t)
	p := NewPager(nil, nil)

	t.Run("Plain text wraps at the column boundary", func(t *testing.T) {
		t.Parallel()
		assert.Equal([]string{"hello", " worl", "d"}, p.wrapLine("hello world", 5))
	})

	t.Run("A tab expands to the next tab stop", func(t *testing.T) {
		t.Parallel()
		assert.Equal([]string{"a       b"}, p.wrapLine("a\tb", 80))
		assert.Equal([]string{"ab      c"}, p.wrapLine("ab\tc", 80))
	})

	t.Run("Wide runes count as two columns and never split across rows", func(t *testing.T) {
		t.Parallel()
		// Each CJK rune is two columns wide, so only one fits in a width of 3.
		assert.Equal([]string{"你", "好"}, p.wrapLine("你好", 3))   //nolint:gosmopolitan
		assert.Equal([]string{"a你", "b"}, p.wrapLine("a你b", 3)) //nolint:gosmopolitan
	})

	t.Run("Combining marks add no width", func(t *testing.T) {
		t.Parallel()
		// "e" + combining acute accent (U+0301) occupies one column, so it
		// plus "x" fit within a width of 2.
		assert.Equal([]string{"e\u0301x"}, p.wrapLine("e\u0301x", 2))
	})

	t.Run("Control characters render as caret notation", func(t *testing.T) {
		t.Parallel()
		assert.Equal([]string{"a^Ab"}, p.wrapLine("a\x01b", 80))
	})
}

func TestPagerWrap(t *testing.T) {
	t.Parallel()
	assert := lib.NewAssert(t)
	p := NewPager(nil, nil)

	t.Run("A trailing newline does not add a blank row", func(t *testing.T) {
		t.Parallel()
		assert.Equal([]string{"a"}, p.wrap([]byte("a\n"), 80))
	})

	t.Run("Blank lines in the middle are preserved", func(t *testing.T) {
		t.Parallel()
		assert.Equal([]string{"a", "", "b"}, p.wrap([]byte("a\n\nb"), 80))
	})

	t.Run("A trailing carriage return is stripped", func(t *testing.T) {
		t.Parallel()
		assert.Equal([]string{"a", "b"}, p.wrap([]byte("a\r\nb"), 80))
	})
}
