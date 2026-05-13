package lib

import (
	"io/fs"
	"testing"
)

func TestFileMode(t *testing.T) {
	t.Parallel()
	t.Run("String and ShortString", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		m := FileMode(0)
		assert.Equal("--------------", m.String())
		assert.Equal("----------", m.ShortString())

		assert.Equal(false, m.IsDir())
		m |= FileModeDir
		assert.Equal(true, m.IsDir())
		assert.Equal("d-------------", m.String())
		assert.Equal("d---------", m.ShortString())

		assert.Equal(false, m.IsSymlink())
		m |= FileModeSymlink
		assert.Equal(true, m.IsSymlink())
		assert.Equal("dL------------", m.String())
		assert.Equal("l---------", m.ShortString())

		assert.Equal(false, m.IsSetUID())
		m |= FileModeSetUid
		assert.Equal(true, m.IsSetUID())
		assert.Equal("dLu-----------", m.String())
		assert.Equal("l--S------", m.ShortString())

		assert.Equal(false, m.IsSetGID())
		m |= FileModeSetGid
		assert.Equal(true, m.IsSetGID())
		assert.Equal("dLug----------", m.String())
		assert.Equal("l--S--S---", m.ShortString())

		assert.Equal(false, m.IsSticky())
		m |= FileModeSticky
		assert.Equal(true, m.IsSticky())
		assert.Equal("dLugt---------", m.String())
		assert.Equal("l--S--S--T", m.ShortString())

		// Test all permission bits.
		m |= 1
		assert.Equal("dLugt--------x", m.String())
		assert.Equal("l--S--S--t", m.ShortString())

		m |= 2
		assert.Equal("dLugt-------wx", m.String())
		assert.Equal("l--S--S-wt", m.ShortString())

		m |= 4
		assert.Equal("dLugt------rwx", m.String())
		assert.Equal("l--S--Srwt", m.ShortString())

		m |= 8
		assert.Equal("dLugt-----xrwx", m.String())
		assert.Equal("l--S--srwt", m.ShortString())

		m |= 16
		assert.Equal("dLugt----wxrwx", m.String())
		assert.Equal("l--S-wsrwt", m.ShortString())

		m |= 32
		assert.Equal("dLugt---rwxrwx", m.String())
		assert.Equal("l--Srwsrwt", m.ShortString())

		m |= 64
		assert.Equal("dLugt--xrwxrwx", m.String())
		assert.Equal("l--srwsrwt", m.ShortString())

		m |= 128
		assert.Equal("dLugt-wxrwxrwx", m.String())
		assert.Equal("l-wsrwsrwt", m.ShortString())

		m |= 256
		assert.Equal("dLugtrwxrwxrwx", m.String())
		assert.Equal("lrwsrwsrwt", m.ShortString())
	})

	t.Run("NewFileMode and AsFsFileMode", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var fsm fs.FileMode
		assert.Equal(0, int(NewFileMode(fsm)))
		assert.Equal(fs.FileMode(0), NewFileMode(fsm).AsFsFileMode())
		fsm |= fs.ModeDir
		assert.Equal(int(FileModeDir), int(NewFileMode(fsm)))
		assert.Equal(fs.ModeDir, NewFileMode(fsm).AsFsFileMode())
		fsm |= fs.ModeSymlink
		assert.Equal(int(FileModeSymlink|FileModeDir), int(NewFileMode(fsm)))
		assert.Equal(fs.ModeSymlink|fs.ModeDir, NewFileMode(fsm).AsFsFileMode())
		fsm |= fs.ModeSetuid
		assert.Equal(int(FileModeSetUid|FileModeSymlink|FileModeDir), int(NewFileMode(fsm)))
		assert.Equal(fs.ModeSetuid|fs.ModeSymlink|fs.ModeDir, NewFileMode(fsm).AsFsFileMode())
		fsm |= fs.ModeSetgid
		assert.Equal(int(FileModeSetGid|FileModeSetUid|FileModeSymlink|FileModeDir), int(NewFileMode(fsm)))
		assert.Equal(fs.ModeSetgid|fs.ModeSetuid|fs.ModeSymlink|fs.ModeDir, NewFileMode(fsm).AsFsFileMode())
		fsm |= fs.ModeSticky
		assert.Equal(
			int(FileModeSticky|FileModeSetGid|FileModeSetUid|FileModeSymlink|FileModeDir),
			int(NewFileMode(fsm)),
		)
		assert.Equal(
			fs.ModeSticky|fs.ModeSetgid|fs.ModeSetuid|fs.ModeSymlink|fs.ModeDir,
			NewFileMode(fsm).AsFsFileMode(),
		)
		// These are ignored.
		for _, ignored := range []fs.FileMode{fs.ModeTemporary, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeIrregular, fs.ModeCharDevice, fs.ModeAppend, fs.ModeExclusive} {
			assert.Equal(0, int(NewFileMode(ignored)))
			assert.Equal(fs.FileMode(0), NewFileMode(ignored).AsFsFileMode())
		}
		// Permissions are the same bits.
		assert.Equal(0o777, int(NewFileMode(0o777)))
		assert.Equal(fs.FileMode(0o777), NewFileMode(0o777).AsFsFileMode())
	})
}
