package lib

import (
	"bytes"
	"testing"
)

func TestFileMetadata(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := fakeFileMetadata(0)
		err := MarshalFileMetadata(sut, &buf)
		assert.NoError(err)
		read, err := UnmarshalFileMetadata(&buf)
		assert.NoError(err)
		assert.Equal(sut, read)
	})
}

func BenchmarkFileMetadataMarshalUnmarshal(b *testing.B) {
	b.Run("MarshalFileMetadata", func(b *testing.B) {
		assert := NewAssert(b)
		sut := fakeFileMetadata(0)
		var buf bytes.Buffer
		for b.Loop() {
			err := MarshalFileMetadata(sut, &buf)
			assert.NoError(err)
		}
	})
	b.Run("UnmarshalFileMetadata", func(b *testing.B) {
		assert := NewAssert(b)
		sut := fakeFileMetadata(0)
		var buf bytes.Buffer
		err := MarshalFileMetadata(sut, &buf)
		assert.NoError(err)
		readBuf := buf.Bytes()
		for b.Loop() {
			_, err := UnmarshalFileMetadata(bytes.NewReader(readBuf))
			assert.NoError(err)
		}
	})
}

func fakeFileMetadata(mode ModeAndPerm) *FileMetadata {
	return &FileMetadata{
		ModeAndPerm:   mode,
		MTimeSec:      4567890,
		MTimeNSec:     567890,
		Size:          67890,
		FileHash:      fakeSHA256("1"),
		BlockIds:      []BlockId{fakeBlockId("1"), fakeBlockId("2")},
		SymlinkTarget: "some/target",
		UID:           7890,
		GID:           890,
		BirthtimeSec:  90,
		BirthtimeNSec: 12345,
	}
}
