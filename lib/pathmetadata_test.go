package lib

import (
	"bytes"
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestPathMetadata(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := TestData{}.PathMetadata(0)
		err := MarshalPathMetadata(sut, &buf)
		assert.NoError(err)
		read, err := UnmarshalPathMetadata(&buf)
		assert.NoError(err)
		assert.Equal(*sut, *read)
	})

	t.Run("IsEqualRestorableAttributes", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)

		base := td.PathMetadata(0)
		typ := reflect.TypeFor[PathMetadata]()
		actualFields := []string{}
		for field := range typ.Fields() {
			actualFields = append(actualFields, field.Name)
		}
		slices.Sort(actualFields)
		assert.Equal(
			[]string{
				"Birthtime",
				"BlockIds",
				"FileHash",
				"FileMode",
				"Gid",
				"Mtime",
				"Size",
				"SymLinkTarget",
				"Uid",
			}, actualFields, "PathMetadata field names have changed, make sure to update IsEqualRestorableAttributes",
		)

		actual := *base
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))

		actual = *base
		actual.BlockIds = append(actual.BlockIds, td.BlockId("3"))
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll), "BlockIds are ignored")

		actual = *base
		actual.FileMode = 0o111
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll^RestorableMetadataMode))

		actual = *base
		actual.FileMode |= FileModeDir
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll^RestorableMetadataMode))

		actual = *base
		actual.FileMode |= FileModeSymlink
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll^RestorableMetadataMode))

		actual = *base
		actual.Mtime.Sec += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll^RestorableMetadataMTime))

		actual = *base
		actual.Mtime.Nsec += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll^RestorableMetadataMTime))

		actual = *base
		actual.Size += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))

		actual = *base
		actual.FileHash[0] += 1
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))

		// SymLinkTarget comparisons require a base with a symlink.
		symBase := td.PathMetadata(FileModeSymlink)
		actual = *symBase
		modifiedLink := *symBase.SymLinkTarget + "_modified"
		actual.SymLinkTarget = &modifiedLink
		assert.Equal(false, symBase.IsEqualRestorableAttributes(actual, RestorableMetadataAll))

		actual = *symBase
		actual.SymLinkTarget = nil
		assert.Equal(false, symBase.IsEqualRestorableAttributes(actual, RestorableMetadataAll))

		actual = *base
		uid := *base.Uid + 1
		actual.Uid = &uid
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll^RestorableMetadataOwnership))

		actual = *base
		actual.Uid = nil
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll^RestorableMetadataOwnership))

		actual = *base
		gid := *base.Gid + 1
		actual.Gid = &gid
		assert.Equal(false, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll^RestorableMetadataOwnership))

		// Birthtime is ignored because it is not restorable (on most systems).
		actual = *base
		modifiedBirth := Timestamp{Sec: base.Birthtime.Sec + 1, Nsec: base.Birthtime.Nsec}
		actual.Birthtime = &modifiedBirth
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))

		actual = *base
		actual.Birthtime = nil
		assert.Equal(true, base.IsEqualRestorableAttributes(actual, RestorableMetadataAll))
	})

	t.Run("NewEmptyDirPathMetadata", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		now := time.Now()
		actual := NewEmptyDirPathMetadata(now)
		ts := Timestamp{Sec: now.Unix(), Nsec: uint32(now.Nanosecond())}
		birth := ts
		assert.Equal(PathMetadata{ //nolint:exhaustruct
			FileMode:  0o700 | FileModeDir,
			Mtime:     ts,
			Birthtime: &birth,
		}, actual)
	})
}
