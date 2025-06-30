package lib

import (
	"errors"
	"io"
	"regexp"
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
		errStr := e3.Error()
		errStr = regexp.MustCompile(` at .*error_test.go`).ReplaceAllString(errStr, " at error_test.go")
		assert.Equal(strings.TrimSpace(`
Error at error_test.go:18: and I am on top  
  Cause at error_test.go:17: i am above: "some string"    
    Cause at error_test.go:16: i am down here
			`), errStr)
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
