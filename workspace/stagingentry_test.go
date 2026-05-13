package workspace

import (
	"bytes"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestStagingEntry(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		var buf bytes.Buffer
		sut := StagingEntry{
			RepoPath: td.Path("a.txt"),
			Metadata: *td.PathMetadata(0o600),
			Ctime:    lib.Timestamp{Sec: 123, Nsec: 456},
			Size:     1234,
			Inode:    789,
		}
		err := MarshalStagingEntry(&sut, &buf)
		assert.NoError(err)
		read, err := UnmarshalStagingEntry(&buf)
		assert.NoError(err)
		assert.Equal(sut, *read)
	})

	t.Run("StagingEntryDiskSize is exact", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		var buf bytes.Buffer
		sut := StagingEntry{
			RepoPath: td.Path("a.txt"),
			Metadata: *td.PathMetadata(0o600),
			Ctime:    lib.Timestamp{Sec: 1, Nsec: 2},
			Size:     3,
			Inode:    4,
		}
		err := MarshalStagingEntry(&sut, &buf)
		assert.NoError(err)
		assert.Equal(buf.Len(), StagingEntryDiskSize(&sut))
	})

	t.Run("TempWriter and TempCache round-trip", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		fs := td.NewFS(t)
		tempWriter := NewStagingCacheWriter(fs, lib.MaxBlockDataSize)
		a := StagingEntry{
			RepoPath: td.Path("a.txt"),
			Metadata: *td.PathMetadata(0o600),
			Ctime:    lib.Timestamp{Sec: 123, Nsec: 456},
			Size:     789,
			Inode:    987654,
		}
		b := StagingEntry{
			RepoPath: td.Path("b.txt"),
			Metadata: *td.PathMetadata(0o700),
			Ctime:    lib.Timestamp{Sec: 234, Nsec: 567},
			Size:     890,
			Inode:    876543,
		}
		assert.NoError(tempWriter.Add(&a))
		assert.NoError(tempWriter.Add(&b))
		_, err := tempWriter.Finalize()
		assert.NoError(err)
		cache, err := OpenStagingCache(fs, 2)
		assert.NoError(err)

		entry, ok, err := cache.Get(lib.PathCompareString(a.RepoPath, a.Metadata.FileMode.IsDir()))
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal(a, *entry)

		entry, ok, err = cache.Get(lib.PathCompareString(b.RepoPath, b.Metadata.FileMode.IsDir()))
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal(b, *entry)
	})
}
