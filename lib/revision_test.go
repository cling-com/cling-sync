package lib

import (
	"bytes"
	"testing"
)

func TestRevisionEntry(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		test := func(entryType RevisionEntryType) {
			t.Helper()
			var buf bytes.Buffer
			sut := fakeRevisionEntry("a.txt", entryType)
			err := MarshalRevisionEntry(sut, &buf)
			assert.NoError(err)
			read, err := UnmarshalRevisionEntry(&buf)
			assert.NoError(err)
			assert.Equal(sut, read)
		}
		test(RevisionEntryAdd)
		test(RevisionEntryUpdate)
		test(RevisionEntryDelete)
	})
}

func fakeRevisionEntry(path string, entryType RevisionEntryType) *RevisionEntry {
	var metadata *FileMetadata
	if entryType != RevisionEntryDelete {
		metadata = fakeFileMetadata(0)
	}
	return &RevisionEntry{
		Path:     NewPath(path),
		Type:     entryType,
		Metadata: metadata,
	}
}
