package lib

import (
	"bytes"
	"testing"
)

func TestRevision(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := td.Revision(RevisionId{})
		err := MarshalRevision(sut, &buf)
		assert.NoError(err)
		read, err := UnmarshalRevision(&buf)
		assert.NoError(err)
		assert.Equal(sut, read)
	})
}
