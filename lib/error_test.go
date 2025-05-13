package lib

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestError(t *testing.T) {
	t.Parallel()
	t.Run("Error()", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		e1 := Errorf("i am down here")
		e2 := WrapErrorf(e1, "i am above: %q", "some string")
		e3 := WrapErrorf(e2, "and I am on top")
		assert.Equal(strings.TrimSpace(`
Error at /Users/pero/src/pero/cling-sync/lib/error_test.go:17: and I am on top  
  Cause at /Users/pero/src/pero/cling-sync/lib/error_test.go:16: i am above: "some string"    
    Cause at /Users/pero/src/pero/cling-sync/lib/error_test.go:15: i am down here
			`), e3.Error())
	})
	t.Run("errors.Is", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		e1 := WrapErrorf(io.EOF, "this is an EOF")
		e2 := WrapErrorf(e1, "wrapped again")
		assert.Equal(true, errors.Is(e1, io.EOF))
		assert.Equal(true, errors.Is(e2, io.EOF))
	})
}
