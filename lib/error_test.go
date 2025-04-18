package lib

import (
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
Error at /Users/pero/src/pero/cling-sync/lib/error_test.go:15: and I am on top  
  Cause at /Users/pero/src/pero/cling-sync/lib/error_test.go:14: i am above: "some string"    
    Cause at /Users/pero/src/pero/cling-sync/lib/error_test.go:13: i am down here
			`), e3.Error())
	})
}
