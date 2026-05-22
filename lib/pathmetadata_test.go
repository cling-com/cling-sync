package lib

import (
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestPathMetadata(t *testing.T) {
	t.Parallel()
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
		modifiedLink, err := NewPath(symBase.SymLinkTarget.String() + "_modified")
		assert.NoError(err)
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

	t.Run("NewPathMetadataFromFileInfo regular file", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fsys := NewMemoryFS(1024 * 1024)

		f, err := fsys.OpenWrite("f.txt")
		assert.NoError(err)
		_, err = f.Write([]byte("hello"))
		assert.NoError(err)
		assert.NoError(f.Close())
		fileInfo, err := fsys.Stat("f.txt")
		assert.NoError(err)
		md := NewPathMetadataFromFileInfo(fileInfo, Sha256{1}, []BlockId{{2}})
		assert.Equal(int64(5), md.Size)
		assert.Equal(false, md.FileMode.IsDir())
		assert.Equal(false, md.FileMode.IsSymlink())
		assert.Equal(Sha256{1}, md.FileHash)
		assert.Equal([]BlockId{{2}}, md.BlockIds)
	})

	t.Run("NewPathMetadataFromFileInfo directory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fsys := NewMemoryFS(1024 * 1024)

		assert.NoError(fsys.Mkdir("d"))
		info, err := fsys.Stat("d")
		assert.NoError(err)
		md := NewPathMetadataFromFileInfo(info, Sha256{}, nil)
		assert.Equal(int64(0), md.Size)
		assert.Equal(true, md.FileMode.IsDir())
	})

	t.Run("NewPathMetadataFromFileInfo symlink", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fsys := NewMemoryFS(1024 * 1024)

		assert.NoError(fsys.Symlink("anywhere", "link"))
		info, err := fsys.Stat("link")
		assert.NoError(err)
		md := NewPathMetadataFromFileInfo(info, Sha256{}, nil)
		assert.Equal(int64(0), md.Size)
		assert.Equal(FileModeSymlink, md.FileMode)
		assert.Equal(Sha256{}, md.FileHash)
		assert.Equal(([]BlockId)(nil), md.BlockIds)
		assert.Equal(false, md.HasUID())
		assert.Equal(false, md.HasGID())
		assert.Equal(false, md.HasBirthtime())
	})

	t.Run("NewPathMetadataFromFileInfo panics on symlink with FileHash or BlockIds", func(t *testing.T) {
		t.Parallel()
		fsys := NewMemoryFS(1024 * 1024)
		_ = fsys.Symlink("anywhere", "link")
		info, _ := fsys.Stat("link")

		mustPanic := func(label string, fn func()) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s", label)
				}
			}()
			fn()
		}
		mustPanic("symlink + FileHash", func() {
			NewPathMetadataFromFileInfo(info, Sha256{1}, nil)
		})
		mustPanic("symlink + BlockIds", func() {
			NewPathMetadataFromFileInfo(info, Sha256{}, []BlockId{{2}})
		})
	})

	t.Run("NewPathMetadataFromFileInfo panics on dir with FileHash or BlockIds", func(t *testing.T) {
		t.Parallel()
		fsys := NewMemoryFS(1024 * 1024)
		_ = fsys.Mkdir("d")
		info, _ := fsys.Stat("d")

		mustPanic := func(label string, fn func()) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for %s", label)
				}
			}()
			fn()
		}
		mustPanic("dir + FileHash", func() {
			NewPathMetadataFromFileInfo(info, Sha256{1}, nil)
		})
		mustPanic("dir + BlockIds", func() {
			NewPathMetadataFromFileInfo(info, Sha256{}, []BlockId{{2}})
		})
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
