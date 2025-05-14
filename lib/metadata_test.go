package lib

import (
	"bytes"
	"io/fs"
	"testing"
)

func TestFileMetadata(t *testing.T) {
	t.Parallel()
	t.Run("ModeAndPerm", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		m := ModeAndPerm(0)
		assert.Equal("--------------", m.String())

		assert.Equal(false, m.IsDir())
		m |= ModeDir
		assert.Equal(true, m.IsDir())
		assert.Equal("d-------------", m.String())

		assert.Equal(false, m.IsSymlink())
		m |= ModeSymlink
		assert.Equal(true, m.IsSymlink())
		assert.Equal("dL------------", m.String())

		assert.Equal(false, m.IsSetUID())
		m |= ModeSetUID
		assert.Equal(true, m.IsSetUID())
		assert.Equal("dLu-----------", m.String())

		assert.Equal(false, m.IsSetGUID())
		m |= ModeSetGUID
		assert.Equal(true, m.IsSetGUID())
		assert.Equal("dLug----------", m.String())

		assert.Equal(false, m.IsSticky())
		m |= ModeSticky
		assert.Equal(true, m.IsSticky())
		assert.Equal("dLugt---------", m.String())

		m |= 1
		assert.Equal("dLugt--------x", m.String())
		m |= 2
		assert.Equal("dLugt-------wx", m.String())
		m |= 4
		assert.Equal("dLugt------rwx", m.String())
		m |= 8
		assert.Equal("dLugt-----xrwx", m.String())
		m |= 16
		assert.Equal("dLugt----wxrwx", m.String())
		m |= 32
		assert.Equal("dLugt---rwxrwx", m.String())
		m |= 64
		assert.Equal("dLugt--xrwxrwx", m.String())
		m |= 128
		assert.Equal("dLugt-wxrwxrwx", m.String())
		m |= 256
		assert.Equal("dLugtrwxrwxrwx", m.String())
	})

	t.Run("NewModeAndPerm", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var fsm fs.FileMode
		assert.Equal(0, int(NewModeAndPerm(fsm)))
		fsm |= fs.ModeDir
		assert.Equal(ModeDir, int(NewModeAndPerm(fsm)))
		fsm |= fs.ModeSymlink
		assert.Equal(ModeSymlink|ModeDir, int(NewModeAndPerm(fsm)))
		fsm |= fs.ModeSetuid
		assert.Equal(ModeSetUID|ModeSymlink|ModeDir, int(NewModeAndPerm(fsm)))
		fsm |= fs.ModeSetgid
		assert.Equal(ModeSetGUID|ModeSetUID|ModeSymlink|ModeDir, int(NewModeAndPerm(fsm)))
		fsm |= fs.ModeSticky
		assert.Equal(ModeSticky|ModeSetGUID|ModeSetUID|ModeSymlink|ModeDir, int(NewModeAndPerm(fsm)))
		// These are ignored.
		for _, ignored := range []fs.FileMode{fs.ModeTemporary, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeIrregular, fs.ModeCharDevice, fs.ModeAppend, fs.ModeExclusive} {
			assert.Equal(0, int(NewModeAndPerm(ignored)))
		}
		// Permissions are the same bits.
		assert.Equal(0o777, int(NewModeAndPerm(0o777)))
	})

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
