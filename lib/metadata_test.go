package lib

import (
	"bytes"
	"testing"
)

func TestFileRevision(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := fakeFileRevision()
		err := MarshalFileRevision(sut, &buf)
		assert.NoError(err)
		read, err := UnmarshalFileRevision(&buf)
		assert.NoError(err)
		assert.Equal(sut, read)
	})
}

func BenchmarkFileRevisionMarshalUnmarshal(b *testing.B) {
	b.Run("MarshalFileRevision", func(b *testing.B) {
		assert := NewAssert(b)
		sut := fakeFileRevision()
		var buf bytes.Buffer
		for b.Loop() {
			err := MarshalFileRevision(sut, &buf)
			assert.NoError(err)
		}
	})
	b.Run("UnmarshalFileRevision", func(b *testing.B) {
		assert := NewAssert(b)
		sut := fakeFileRevision()
		var buf bytes.Buffer
		err := MarshalFileRevision(sut, &buf)
		assert.NoError(err)
		readBuf := buf.Bytes()
		for b.Loop() {
			_, err := UnmarshalFileRevision(bytes.NewReader(readBuf))
			assert.NoError(err)
		}
	})
}

func fakeFileRevision() FileRevision {
	return FileRevision{
		SyncTimeSec:   1234567890,
		SyncTimeNSec:  234567890,
		ModeAndPerm:   3456,
		MTimeSec:      4567890,
		MTimeNSec:     567890,
		Size:          67890,
		FileHash:      fakeSHA256("1"),
		BlockIds:      []BlockId{fakeBlockId("1"), fakeBlockId("2")},
		Target:        "some target",
		UID:           7890,
		GID:           890,
		BirthtimeSec:  90,
		BirthtimeNSec: 12345,
	}
}
