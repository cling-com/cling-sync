package workspace

import (
	"bytes"
	"io/fs"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestStaging(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Setup the workspace.
		w.Write("a.txt", "a")
		w.Write("b/c.txt", "cc")
		w.Write("b/e/f.txt", "fff")
		w.Chmod("b/c.txt", 0o400)

		// Create a remote commit with a modified file, missing files, and a new file.
		commit, err := lib.NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		assert.NoError(commit.Add(td.RevisionEntryExt("a.txt", lib.RevisionEntryAdd, 0o600, "a")))
		bDirEntry := td.RevisionEntry("b", lib.RevisionEntryAdd)
		bDirEntry.Metadata = w.FileMetadata("b")
		assert.NoError(commit.Add(bDirEntry))
		assert.NoError(commit.Add(td.RevisionEntryExt("b/remote.txt", lib.RevisionEntryAdd, 0o123, "rrr")))
		remoteRev1, err := commit.Commit(td.CommitInfo())
		assert.NoError(err)
		assert.Equal(remoteRev1, r.Head())
		assert.Equal(false, remoteRev1.IsRoot())
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"a.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("a")},
			{"b", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/remote.txt", lib.RevisionEntryAdd, 0o123, td.SHA256("rrr")},
		}, r.RevisionInfos(remoteRev1))

		// Create a staging.
		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, false, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"a.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("a")},
			{"b", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/c.txt", lib.RevisionEntryAdd, 0o400, td.SHA256("cc")},
			{"b/e", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/e/f.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("fff")},
		}, r.RevisionTempInfos(finalized))

		// Merge the staging with a snapshot of the remote revision.
		snapshot, err := lib.NewRevisionSnapshot(r.Repository, remoteRev1, td.NewFS(t))
		assert.NoError(err)
		merged, err := staging.MergeWithSnapshot(snapshot, lib.RestorableMetadataAll)
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"a.txt", lib.RevisionEntryUpdate, 0o600, td.SHA256("a")},
			// Note that `b/` did not change (and is hence omitted).
			{"b/c.txt", lib.RevisionEntryAdd, 0o400, td.SHA256("cc")},
			// Metadata of `b/remote.txt` should match the repository version.
			{"b/remote.txt", lib.RevisionEntryDelete, 0o123, td.SHA256("rrr")},
			{"b/e", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/e/f.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("fff")},
		}, r.RevisionTempInfos(merged))
	})

	t.Run("With .clingignore and .gitignore files", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Setup the workspace.
		w.Write(".clingignore", "*.png")
		w.Write("a.txt", "a")
		w.Write("b.png", "b")
		w.Write("dir1/.gitignore", "dir2\n*.txt")
		w.Write("dir1/a.txt", "a")
		w.Write("dir1/b.md", "b")
		w.Write("dir1/dir2/a.md", "c")
		w.Write("dir1/dir3/a.txt", "a")
		w.Write("dir1/dir3/b.png", "b")
		w.Write("dir1/dir3/c.md", "c")

		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, false, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{".clingignore", lib.RevisionEntryAdd, 0o600, td.SHA256("*.png")},
			{"a.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("a")},
			{"dir1", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir1/.gitignore", lib.RevisionEntryAdd, 0o600, td.SHA256("dir2\n*.txt")},
			{"dir1/b.md", lib.RevisionEntryAdd, 0o600, td.SHA256("b")},
			{"dir1/dir3", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir1/dir3/c.md", lib.RevisionEntryAdd, 0o600, td.SHA256("c")},
		}, r.RevisionTempInfos(finalized))
	})
}

func TestStagingCache(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal StagingCacheEntry", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		var buf bytes.Buffer
		sut := StagingCacheEntry{
			Path:      td.Path("a.txt"),
			Metadata:  td.FileMetadata(0o600),
			CTimeSec:  123,
			CTimeNSec: 456,
			Inode:     789,
			Size:      1234,
		}
		err := MarshalStagingCacheEntry(&sut, &buf)
		assert.NoError(err)
		read, err := UnmarshalStagingCacheEntry(&buf)
		assert.NoError(err)
		assert.Equal(sut, *read)
	})

	t.Run("TempWriter and TempCache", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		fs := td.NewFS(t)
		tempWriter := NewStagingCacheWriter(fs, lib.MaxBlockDataSize)
		a := StagingCacheEntry{
			Path:      td.Path("a.txt"),
			Metadata:  td.FileMetadata(0o600),
			CTimeSec:  123,
			CTimeNSec: 456,
			Size:      789,
			Inode:     987654,
		}
		b := StagingCacheEntry{
			Path:      td.Path("b.txt"),
			Metadata:  td.FileMetadata(0o700),
			CTimeSec:  234,
			CTimeNSec: 567,
			Size:      890,
			Inode:     876543,
		}
		assert.NoError(tempWriter.Add(&a))
		assert.NoError(tempWriter.Add(&b))
		_, err := tempWriter.Finalize()
		assert.NoError(err)
		cache, err := OpenStagingCache(fs, 2)
		assert.NoError(err)

		entry, ok, err := cache.Get(lib.PathCompareString(a.Path, a.Metadata.ModeAndPerm.IsDir()))
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal(a, *entry)

		entry, ok, err = cache.Get(lib.PathCompareString(b.Path, b.Metadata.ModeAndPerm.IsDir()))
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal(b, *entry)
	})

	t.Run("Existing cache is used and new cache is created", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("b.txt", "b")
		w.Write("dir/a.txt", "a")
		w.Chmod("dir/a.txt", 0o600)

		// Create the cache with an entry for `a.txt`.
		cacheFS, err := w.Workspace.FS.MkSub(".cling/workspace/cache/staging")
		assert.NoError(err)
		tempWriter := NewStagingCacheWriter(cacheFS, lib.MaxBlockDataSize)
		fileInfo, err := w.Workspace.FS.Stat("dir/a.txt")
		assert.NoError(err)
		// Note: We set a different mode here to verify that the mode is not taken from the cache.
		amd := td.FileMetadata(0o777)
		amd.FileHash = td.SHA256("from_cache")
		a, err := NewStagingCacheEntry(td.Path("dir/a.txt"), fileInfo, amd.FileHash, amd.BlockIds)
		assert.NoError(err)
		assert.NoError(tempWriter.Add(a))
		_, err = tempWriter.Finalize()
		assert.NoError(err)

		// Create a staging that should use the cache.
		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, true, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"b.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("b")},
			{"dir", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir/a.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("from_cache")},
		}, r.RevisionTempInfos(finalized))

		// The previous run should have retained the cache entry for `a.txt`. So we should see the
		// same result.
		staging, err = NewStaging(w.Workspace.FS, lib.Path{}, nil, true, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err = staging.Finalize()
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"b.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("b")},
			{"dir", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir/a.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("from_cache")},
		}, r.RevisionTempInfos(finalized))

		// Not using the cache should ignore our fake cache entry and rebuild the cache correctly.
		// Note: The cache will be re-created even if `useCache` is false.
		staging, err = NewStaging(w.Workspace.FS, lib.Path{}, nil, false, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err = staging.Finalize()
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"b.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("b")},
			{"dir", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir/a.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("a")},
		}, r.RevisionTempInfos(finalized))
		cache, err := OpenStagingCache(cacheFS, 2)
		assert.NoError(err)
		entry, ok, err := cache.Get(lib.PathCompareString(td.Path("dir/a.txt"), false))
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal(lib.ModeAndPerm(0o600), entry.Metadata.ModeAndPerm)
		assert.Equal(td.SHA256("a"), entry.Metadata.FileHash)
	})
}
