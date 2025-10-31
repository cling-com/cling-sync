package lib

import (
	"bytes"
	"io/fs"
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestFileMetadata(t *testing.T) {
	t.Parallel()
	t.Run("ModeAndPerm", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		m := ModeAndPerm(0)
		assert.Equal("--------------", m.String())
		assert.Equal("----------", m.ShortString())

		assert.Equal(false, m.IsDir())
		m |= ModeDir
		assert.Equal(true, m.IsDir())
		assert.Equal("d-------------", m.String())
		assert.Equal("d---------", m.ShortString())

		assert.Equal(false, m.IsSymlink())
		m |= ModeSymlink
		assert.Equal(true, m.IsSymlink())
		assert.Equal("dL------------", m.String())
		assert.Equal("l---------", m.ShortString())

		assert.Equal(false, m.IsSetUID())
		m |= ModeSetUID
		assert.Equal(true, m.IsSetUID())
		assert.Equal("dLu-----------", m.String())
		assert.Equal("l--S------", m.ShortString())

		assert.Equal(false, m.IsSetGUID())
		m |= ModeSetGID
		assert.Equal(true, m.IsSetGUID())
		assert.Equal("dLug----------", m.String())
		assert.Equal("l--S--S---", m.ShortString())

		assert.Equal(false, m.IsSticky())
		m |= ModeSticky
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

	t.Run("NewModeAndPerm and AsFileMode", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var fsm fs.FileMode
		assert.Equal(0, int(NewModeAndPerm(fsm)))
		assert.Equal(0, int(NewModeAndPerm(fsm).AsFileMode()))
		fsm |= fs.ModeDir
		assert.Equal(ModeDir, int(NewModeAndPerm(fsm)))
		assert.Equal(fs.ModeDir, NewModeAndPerm(fsm).AsFileMode())
		fsm |= fs.ModeSymlink
		assert.Equal(ModeSymlink|ModeDir, int(NewModeAndPerm(fsm)))
		assert.Equal(fs.ModeSymlink|fs.ModeDir, NewModeAndPerm(fsm).AsFileMode())
		fsm |= fs.ModeSetuid
		assert.Equal(ModeSetUID|ModeSymlink|ModeDir, int(NewModeAndPerm(fsm)))
		assert.Equal(fs.ModeSetuid|fs.ModeSymlink|fs.ModeDir, NewModeAndPerm(fsm).AsFileMode())
		fsm |= fs.ModeSetgid
		assert.Equal(ModeSetGID|ModeSetUID|ModeSymlink|ModeDir, int(NewModeAndPerm(fsm)))
		assert.Equal(fs.ModeSetgid|fs.ModeSetuid|fs.ModeSymlink|fs.ModeDir, NewModeAndPerm(fsm).AsFileMode())
		fsm |= fs.ModeSticky
		assert.Equal(ModeSticky|ModeSetGID|ModeSetUID|ModeSymlink|ModeDir, int(NewModeAndPerm(fsm)))
		assert.Equal(
			fs.ModeSticky|fs.ModeSetgid|fs.ModeSetuid|fs.ModeSymlink|fs.ModeDir,
			NewModeAndPerm(fsm).AsFileMode(),
		)
		// These are ignored.
		for _, ignored := range []fs.FileMode{fs.ModeTemporary, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeIrregular, fs.ModeCharDevice, fs.ModeAppend, fs.ModeExclusive} {
			assert.Equal(0, int(NewModeAndPerm(ignored)))
			assert.Equal(0, int(NewModeAndPerm(ignored).AsFileMode()))
		}
		// Permissions are the same bits.
		assert.Equal(0o777, int(NewModeAndPerm(0o777)))
		assert.Equal(0o777, int(NewModeAndPerm(0o777).AsFileMode()))
	})

	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := TestData{}.FileMetadata(0)
		err := MarshalFileMetadata(sut, &buf)
		assert.NoError(err)
		read, err := UnmarshalFileMetadata(&buf)
		assert.NoError(err)
		assert.Equal(sut, read)
	})

	t.Run("MarshalledSize", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := td.FileMetadata(0)
		err := MarshalFileMetadata(sut, &buf)
		assert.NoError(err)
		assert.Equal(sut.MarshalledSize(), buf.Len())

		sut.SymlinkTarget = "some/symlink"
		buf.Reset()
		err = MarshalFileMetadata(sut, &buf)
		assert.NoError(err)
		assert.Equal(sut.MarshalledSize(), buf.Len())

		sut.BlockIds = []BlockId{td.BlockId("1")}
		buf.Reset()
		err = MarshalFileMetadata(sut, &buf)
		assert.NoError(err)
		assert.Equal(sut.MarshalledSize(), buf.Len())
	})

	t.Run("IsEqualRestorableAttributes", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)

		base := td.FileMetadata(0)
		actualFields := make([]string, 0)
		typ := reflect.TypeOf(*base)
		for i := range typ.NumField() {
			actualFields = append(actualFields, typ.Field(i).Name)
		}
		slices.Sort(actualFields)
		assert.Equal(
			[]string{
				"BirthtimeNSec",
				"BirthtimeSec",
				"BlockIds",
				"FileHash",
				"GID",
				"MTimeNSec",
				"MTimeSec",
				"ModeAndPerm",
				"Size",
				"SymlinkTarget",
				"UID",
			}, actualFields, "FileMetadata field names have changed, make sure to update IsEqualRestorableAttributes",
		)

		actual := *base
		assert.Equal(true, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))

		actual = *base
		actual.BlockIds = append(actual.BlockIds, td.BlockId("3"))
		assert.Equal(true, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll), "BlockIds are ignored")

		actual = *base
		actual.ModeAndPerm = 0o111
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll^RestorableMetadataMode))

		actual = *base
		actual.ModeAndPerm |= ModeDir
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll^RestorableMetadataMode))

		actual = *base
		actual.ModeAndPerm |= ModeSymlink
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll^RestorableMetadataMode))

		actual = *base
		actual.MTimeSec += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll^RestorableMetadataMTime))

		actual = *base
		actual.MTimeNSec += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll^RestorableMetadataMTime))

		actual = *base
		actual.Size += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))

		actual = *base
		actual.FileHash[0] += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))

		actual = *base
		actual.SymlinkTarget += "_modified"
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))

		actual = *base
		actual.UID += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll^RestorableMetadataOwnership))

		actual = *base
		actual.GID += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll^RestorableMetadataOwnership))

		// Birthtime is ignored because it is not restorable (on most systems).
		actual = *base
		actual.BirthtimeSec += 1
		assert.Equal(true, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))

		actual = *base
		actual.BirthtimeNSec += 1
		assert.Equal(true, base.IsEqualRestorableAttributes(&actual, RestorableMetadataAll))
	})

	t.Run("NewEmptyDirFileMetadata", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		now := time.Now()
		actual := NewEmptyDirFileMetadata(now)
		assert.Equal(FileMetadata{
			ModeAndPerm: 0o700 | ModeDir,
			MTimeSec:    now.Unix(),
			MTimeNSec:   int32(now.Nanosecond()), //nolint:gosec
			Size:        0,
			FileHash:    Sha256{},
			BlockIds:    nil,

			SymlinkTarget: "",

			UID:           UIDUnset,
			GID:           UIDUnset,
			BirthtimeSec:  now.Unix(),
			BirthtimeNSec: int32(now.Nanosecond()), //nolint:gosec
		}, actual)
	})
}

func BenchmarkFileMetadataMarshalUnmarshal(b *testing.B) {
	b.Run("MarshalFileMetadata", func(b *testing.B) {
		assert := NewAssert(b)
		sut := td.FileMetadata(0)
		var buf bytes.Buffer
		for b.Loop() {
			err := MarshalFileMetadata(sut, &buf)
			assert.NoError(err)
		}
	})
	b.Run("UnmarshalFileMetadata", func(b *testing.B) {
		assert := NewAssert(b)
		sut := td.FileMetadata(0)
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
