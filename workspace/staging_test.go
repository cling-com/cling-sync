package workspace

import (
	"errors"
	"io"
	"io/fs"
	"testing"

	"github.com/cling-com/cling-sync/lib"
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
		commit, err := lib.NewCommit(t.Context(), r.Repository, td.NewFS(t))
		assert.NoError(err)
		assert.NoError(commit.Add(td.RevisionEntryExt("a.txt", lib.RevisionEntryKindAdd, 0o600, "a")))
		bDirEntry := td.RevisionEntry("b", lib.RevisionEntryKindAdd)
		bDirEntry.Metadata = *w.PathMetadata("b")
		assert.NoError(commit.Add(bDirEntry))
		assert.NoError(commit.Add(td.RevisionEntryExt("b/remote.txt", lib.RevisionEntryKindAdd, 0o123, "rrr")))
		remoteRev1, err := commit.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		assert.Equal(remoteRev1, r.Head())
		assert.Equal(false, remoteRev1.IsRoot())
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"a.txt", lib.RevisionEntryKindAdd, 0o600, td.SHA256("a")},
			{"b", lib.RevisionEntryKindAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/remote.txt", lib.RevisionEntryKindAdd, 0o123, td.SHA256("rrr")},
		}, r.RevisionInfos(remoteRev1))

		// Create a staging.
		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"a.txt", 0o600, td.SHA256("a")},
			{"b", 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/c.txt", 0o400, td.SHA256("cc")},
			{"b/e", 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/e/f.txt", 0o600, td.SHA256("fff")},
		}, wstd.StagingEntryInfos(finalized))

		// Merge the staging with a snapshot of the remote revision.
		snapshot, err := lib.NewRevisionSnapshot(
			t.Context(),
			r.Repository,
			remoteRev1,
			td.NewFS(t),
			wstd.SnapshotMonitor(),
		)
		assert.NoError(err)
		merged, err := staging.MergeWithSnapshot(snapshot, lib.RestorableMetadataAll, false)
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"a.txt", lib.RevisionEntryKindUpdate, 0o600, td.SHA256("a")},
			// Note that `b/` did not change (and is hence omitted).
			{"b/c.txt", lib.RevisionEntryKindAdd, 0o400, td.SHA256("cc")},
			// Metadata of `b/remote.txt` should match the repository version.
			{"b/remote.txt", lib.RevisionEntryKindDelete, 0o123, td.SHA256("rrr")},
			{"b/e", lib.RevisionEntryKindAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"b/e/f.txt", lib.RevisionEntryKindAdd, 0o600, td.SHA256("fff")},
		}, r.RevisionTempInfos(merged))
	})

	t.Run("Path changes type", func(t *testing.T) {
		// A file and a directory of one path compare by different keys, so a
		// type change comes out as a delete of the old entry plus an add of the
		// new one. The snapshot merge relies on that delete.
		t.Parallel()
		assert := lib.NewAssert(t)

		// A file that became a directory. The delete sorts first, at key `0x`.
		r1 := td.NewTestRepository(t, td.NewFS(t))
		w1 := wstd.NewTestWorkspace(t, r1.Repository)
		w1.Write("x/inner.txt", "inner")
		commit1, err := lib.NewCommit(t.Context(), r1.Repository, td.NewFS(t))
		assert.NoError(err)
		assert.NoError(commit1.Add(td.RevisionEntryExt("x", lib.RevisionEntryKindAdd, 0o600, "a file")))
		rev1, err := commit1.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"x", lib.RevisionEntryKindDelete, 0o600, td.SHA256("a file")},
			{"x", lib.RevisionEntryKindAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"x/inner.txt", lib.RevisionEntryKindAdd, 0o600, td.SHA256("inner")},
		}, stagingDiff(t, r1, w1, rev1))

		// A directory that became a file. Now the add sorts first. The
		// directory is created first so the revision holds a real one.
		r2 := td.NewTestRepository(t, td.NewFS(t))
		w2 := wstd.NewTestWorkspace(t, r2.Repository)
		w2.Write("x/inner.txt", "inner")
		commit2, err := lib.NewCommit(t.Context(), r2.Repository, td.NewFS(t))
		assert.NoError(err)
		xDirEntry := td.RevisionEntry("x", lib.RevisionEntryKindAdd)
		xDirEntry.Metadata = *w2.PathMetadata("x")
		assert.NoError(commit2.Add(xDirEntry))
		assert.NoError(commit2.Add(td.RevisionEntryExt("x/inner.txt", lib.RevisionEntryKindAdd, 0o600, "inner")))
		rev2, err := commit2.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		w2.RmAll("x")
		w2.Write("x", "a file")
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"x", lib.RevisionEntryKindAdd, 0o600, td.SHA256("a file")},
			{"x", lib.RevisionEntryKindDelete, 0o700 | fs.ModeDir, td.SHA256("")},
			{"x/inner.txt", lib.RevisionEntryKindDelete, 0o600, td.SHA256("inner")},
		}, stagingDiff(t, r2, w2, rev2))
	})

	t.Run("MergeWithSnapshot with suppressDeletes drops delete entries", func(t *testing.T) {
		// Used by the attach --allow-non-empty merge path: paths that only
		// exist in the snapshot are fetched on merge rather than deleted.
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Workspace has `a.txt` (modified) and `local.txt` (new); the
		// snapshot adds a third file `b/remote.txt` that staging never sees.
		w.Write("a.txt", "aa")
		w.Write("local.txt", "L")

		commit, err := lib.NewCommit(t.Context(), r.Repository, td.NewFS(t))
		assert.NoError(err)
		assert.NoError(commit.Add(td.RevisionEntryExt("a.txt", lib.RevisionEntryKindAdd, 0o600, "a")))
		assert.NoError(commit.Add(td.RevisionEntryExt("b/remote.txt", lib.RevisionEntryKindAdd, 0o600, "r")))
		remoteRev, err := commit.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)

		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		snapshot, err := lib.NewRevisionSnapshot(
			t.Context(),
			r.Repository,
			remoteRev,
			td.NewFS(t),
			wstd.SnapshotMonitor(),
		)
		assert.NoError(err)

		// With suppressDeletes=true the DELETE for `b/remote.txt` is skipped;
		// only the UPDATE for `a.txt` and the ADD for `local.txt` remain.
		merged, err := staging.MergeWithSnapshot(snapshot, lib.RestorableMetadataAll, true)
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"a.txt", lib.RevisionEntryKindUpdate, 0o600, td.SHA256("aa")},
			{"local.txt", lib.RevisionEntryKindAdd, 0o600, td.SHA256("L")},
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

		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{".clingignore", 0o600, td.SHA256("*.png")},
			{"a.txt", 0o600, td.SHA256("a")},
			{"dir1", 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir1/.gitignore", 0o600, td.SHA256("dir2\n*.txt")},
			{"dir1/b.md", 0o600, td.SHA256("b")},
			{"dir1/dir3", 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir1/dir3/c.md", 0o600, td.SHA256("c")},
		}, wstd.StagingEntryInfos(finalized))
	})

	t.Run("With path prefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")

		// Add first commit to the root workspace.
		w.Write("a.txt", "a")

		staging, err := NewStaging(
			w.Workspace.FS,
			td.Path("look/here/"),
			nil,
			nil,
			nil,
			w.TempFS,
			wstd.StagingMonitor(),
		)
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			// Make sure that the path prefix is included in the StagingEntry.
			{"look/here/a.txt", 0o600, td.SHA256("a")},
		}, wstd.StagingEntryInfos(finalized))
	})

	t.Run("Include filter selects files below the top level", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("src/a.go", "a")
		w.Write("src/sub/b.go", "b")
		w.Write("docs/c.md", "c")

		include := lib.NewPathInclusionFilter([]string{"src/**"})
		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, include, nil, nil, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			// `src` itself is not staged because it does not match `src/**`.
			{"src/a.go", 0o600, td.SHA256("a")},
			{"src/sub", 0o700 | fs.ModeDir, td.SHA256("")},
			{"src/sub/b.go", 0o600, td.SHA256("b")},
		}, wstd.StagingEntryInfos(finalized))
	})

	t.Run("Exclude filter skips a directory and its contents", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Write("build/x.o", "x")
		w.Write("build/deep/y.o", "y")

		exclude := lib.NewPathExclusionFilter([]string{"build"})
		mon := &recordingStagingMonitor{Visited: nil}
		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, exclude, nil, w.TempFS, mon)
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"a.txt", 0o600, td.SHA256("a")},
		}, wstd.StagingEntryInfos(finalized))
		// `build` is not descended into.
		assert.Equal([]string{"a.txt", "build"}, mon.Visited)
	})

	t.Run("Include and exclude filters are both applied", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("src/a.go", "a")
		w.Write("src/vendor/v.go", "v")
		w.Write("docs/c.md", "c")

		include := lib.NewPathInclusionFilter([]string{"src/**"})
		exclude := lib.NewPathExclusionFilter([]string{"src/vendor"})
		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, include, exclude, nil, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"src/a.go", 0o600, td.SHA256("a")},
		}, wstd.StagingEntryInfos(finalized))
	})

	t.Run("Filters match paths relative to the path prefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")

		w.Write("src/a.go", "a")
		w.Write("docs/c.md", "c")

		commit, err := lib.NewCommit(t.Context(), r.Repository, td.NewFS(t))
		assert.NoError(err)
		assert.NoError(commit.Add(td.RevisionEntryExt("look/here/src/a.go", lib.RevisionEntryKindAdd, 0o600, "old")))
		assert.NoError(commit.Add(td.RevisionEntryExt("look/here/docs/c.md", lib.RevisionEntryKindAdd, 0o600, "c")))
		assert.NoError(commit.Add(td.RevisionEntryExt("other.txt", lib.RevisionEntryKindAdd, 0o600, "o")))
		remoteRev, err := commit.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)

		include := lib.NewPathInclusionFilter([]string{"src/**"})
		staging, err := NewStaging(
			w.Workspace.FS,
			td.Path("look/here/"),
			include,
			nil,
			nil,
			w.TempFS,
			wstd.StagingMonitor(),
		)
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"look/here/src/a.go", 0o600, td.SHA256("a")},
		}, wstd.StagingEntryInfos(finalized))

		// The snapshot holds repository paths. Neither the filtered out
		// `look/here/docs/c.md` nor `other.txt` outside the prefix may be deleted.
		snapshot, err := lib.NewRevisionSnapshot(
			t.Context(),
			r.Repository,
			remoteRev,
			td.NewFS(t),
			wstd.SnapshotMonitor(),
		)
		assert.NoError(err)
		merged, err := staging.MergeWithSnapshot(snapshot, lib.RestorableMetadataAll, false)
		assert.NoError(err)
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"look/here/src/a.go", lib.RevisionEntryKindUpdate, 0o600, td.SHA256("a")},
		}, r.RevisionTempInfos(merged))
	})

	t.Run("Cancel", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w.Write("a.txt", "a")

		mon := &cancelStagingMonitor{}
		_, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, mon)
		assert.ErrorIs(err, lib.ErrCancel)
	})
}

func TestStagingSymlinks(t *testing.T) {
	t.Parallel()

	t.Run("symlink in same dir", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w.Write("a.txt", "a")
		w.Symlink("a.txt", "link")

		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)

		entries := readAllStagingEntries(t, finalized)
		assert.Equal(2, len(entries))
		assert.Equal("a.txt", entries[0].RepoPath.String())
		assert.Equal("link", entries[1].RepoPath.String())
		assert.Equal(true, entries[1].Metadata.FileMode.IsSymlink())
		if entries[1].Metadata.SymLinkTarget == nil {
			t.Fatal("SymLinkTarget should be set")
		}
		assert.Equal("a.txt", entries[1].Metadata.SymLinkTarget.String())
	})

	t.Run("symlink to sibling dir", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w.Write("dir1/a.txt", "a")
		w.Symlink("../dir1/a.txt", "dir2/link")

		staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		entries := readAllStagingEntries(t, finalized)
		linkEntry := findEntry(entries, "dir2/link")
		if linkEntry == nil {
			t.Fatal("expected staging entry for dir2/link")
		}
		assert.Equal(true, linkEntry.Metadata.FileMode.IsSymlink())
		assert.Equal("dir1/a.txt", linkEntry.Metadata.SymLinkTarget.String())
	})

	t.Run("symlink with path prefix rebases target", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")
		w.Write("a.txt", "a")
		w.Symlink("a.txt", "link")

		staging, err := NewStaging(
			w.Workspace.FS,
			td.Path("look/here/"),
			nil,
			nil,
			nil,
			w.TempFS,
			wstd.StagingMonitor(),
		)
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		entries := readAllStagingEntries(t, finalized)
		linkEntry := findEntry(entries, "look/here/link")
		if linkEntry == nil {
			t.Fatal("expected staging entry for look/here/link")
		}
		assert.Equal("look/here/a.txt", linkEntry.Metadata.SymLinkTarget.String())
	})

	t.Run("absolute target rejected", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		// The cleanup walker chmods every entry. On macOS that follows the
		// link and stalls on sensitive paths. Use a clearly nonexistent
		// absolute target so the chmod fails fast with ENOENT.
		w.Symlink("/nonexistent_absolute_target", "bad")

		_, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, wstd.StagingMonitor())
		assert.Equal(true, errors.Is(err, ErrSymLinkTargetEscapes))
	})

	t.Run("absolute target from a subdirectory rejected", func(t *testing.T) {
		// `filepath.Join` absorbs leading `/` when joined with a non-"."
		// directory, so `dir1/link → /etc/foo` would otherwise become a
		// valid-looking `dir1/etc/foo` Path and silently mis-stored.
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w.Symlink("/nonexistent_absolute_target", "dir1/bad")

		_, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, wstd.StagingMonitor())
		assert.Equal(true, errors.Is(err, ErrSymLinkTargetEscapes))
	})

	t.Run("escaping target rejected", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w.Symlink("../../outside", "dir1/bad")

		_, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, wstd.StagingMonitor())
		assert.Equal(true, errors.Is(err, ErrSymLinkTargetEscapes))
	})
}

type cancelStagingMonitor struct{}

func (m *cancelStagingMonitor) OnStart(path lib.Path, dirEntry fs.DirEntry) error {
	return lib.ErrCancel
}

func (m *cancelStagingMonitor) OnEnd(path lib.Path, excluded bool, metadata *lib.PathMetadata) error {
	return nil
}

type recordingStagingMonitor struct {
	Visited []string
}

func (m *recordingStagingMonitor) OnStart(path lib.Path, dirEntry fs.DirEntry) error {
	m.Visited = append(m.Visited, path.String())
	return nil
}

func (m *recordingStagingMonitor) OnEnd(path lib.Path, excluded bool, metadata *lib.PathMetadata) error {
	return nil
}

func TestStagingCache(t *testing.T) {
	t.Parallel()
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
		amd := td.PathMetadata(0o777)
		amd.FileHash = td.SHA256("from_cache")
		a, err := NewStagingEntry(td.Path("dir/a.txt"), fileInfo, fileInfo.Size(), amd.FileHash, amd.BlockIds)
		assert.NoError(err)
		assert.NoError(tempWriter.Add(a))
		_, err = tempWriter.Finalize()
		assert.NoError(err)

		// Create a staging that should use the cache.
		staging, err := NewStaging(
			w.Workspace.FS,
			lib.Path{},
			nil,
			nil,
			stagingCache(t, w, true),
			w.TempFS,
			wstd.StagingMonitor(),
		)
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"b.txt", 0o600, td.SHA256("b")},
			{"dir", 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir/a.txt", 0o600, td.SHA256("from_cache")},
		}, wstd.StagingEntryInfos(finalized))

		// The previous run should have retained the cache entry for `a.txt`. So we should see the
		// same result.
		staging, err = NewStaging(
			w.Workspace.FS,
			lib.Path{},
			nil,
			nil,
			stagingCache(t, w, true),
			w.TempFS,
			wstd.StagingMonitor(),
		)
		assert.NoError(err)
		finalized, err = staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"b.txt", 0o600, td.SHA256("b")},
			{"dir", 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir/a.txt", 0o600, td.SHA256("from_cache")},
		}, wstd.StagingEntryInfos(finalized))

		// Not using the cache should ignore our fake cache entry and rebuild the cache correctly.
		// Note: The cache will be re-created even if `useCache` is false.
		staging, err = NewStaging(
			w.Workspace.FS,
			lib.Path{},
			nil,
			nil,
			stagingCache(t, w, false),
			w.TempFS,
			wstd.StagingMonitor(),
		)
		assert.NoError(err)
		finalized, err = staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"b.txt", 0o600, td.SHA256("b")},
			{"dir", 0o700 | fs.ModeDir, lib.Sha256{}},
			{"dir/a.txt", 0o600, td.SHA256("a")},
		}, wstd.StagingEntryInfos(finalized))
		cache, err := OpenStagingCache(cacheFS, 2)
		assert.NoError(err)
		entry, ok, err := cache.Get(lib.PathKey{td.Path("dir/a.txt"), false})
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal(lib.FileMode(0o600), entry.Metadata.FileMode)
		assert.Equal(td.SHA256("a"), entry.Metadata.FileHash)
	})

	t.Run("Without a cache nothing is written to the source directory", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		src.Write("sub/b.txt", "b")
		before := src.Ls(".")

		staging, err := NewStaging(src.FS, lib.Path{}, nil, nil, nil, td.NewFS(t), wstd.StagingMonitor())
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"a.txt", 0o600, td.SHA256("a")},
			{"sub", 0o700 | fs.ModeDir, lib.Sha256{}},
			{"sub/b.txt", 0o600, td.SHA256("b")},
		}, wstd.StagingEntryInfos(finalized))

		// `Ls` covers every path, mode, size, and content, but skips `.cling`,
		// where the cache would live, so that one is checked separately.
		assert.Equal(before, src.Ls("."))
		_, err = src.FS.Stat(".cling")
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("Cache detects same-size content changes", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Write a file with content "aaa" (3 bytes).
		w.Write("a.txt", "aaa")

		// Build the cache by running staging.
		// This seeds the cache with the hash of "aaa".
		staging, err := NewStaging(
			w.Workspace.FS,
			lib.Path{},
			nil,
			nil,
			stagingCache(t, w, false),
			w.TempFS,
			wstd.StagingMonitor(),
		)
		assert.NoError(err)
		finalized, err := staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"a.txt", 0o600, td.SHA256("aaa")},
		}, wstd.StagingEntryInfos(finalized))

		// Now modify the file content with the SAME size (3 bytes).
		// This changes ctime and mtime but not size.
		w.Write("a.txt", "bbb")

		// Run staging WITH cache. The cache has the hash for "aaa" but the file
		// now contains "bbb" (same size). HasChanged() should detect the ctime
		// change and the staging should return the hash of "bbb".
		staging, err = NewStaging(
			w.Workspace.FS,
			lib.Path{},
			nil,
			nil,
			stagingCache(t, w, true),
			w.TempFS,
			wstd.StagingMonitor(),
		)
		assert.NoError(err)
		finalized, err = staging.Finalize()
		assert.NoError(err)
		assert.Equal([]TestStagingEntryInfo{
			{"a.txt", 0o600, td.SHA256("bbb")},
		}, wstd.StagingEntryInfos(finalized))
	})
}

func stagingCache(t *testing.T, w *TestWorkspace, useCache bool) *StagingCache {
	t.Helper()
	cache, err := NewStagingCache(w.Workspace.FS, useCache)
	lib.NewAssert(t).NoError(err)
	return cache
}

func readAllStagingEntries(t *testing.T, temp *lib.Temp[*StagingEntry]) []*StagingEntry {
	t.Helper()
	r := temp.Reader(nil)
	buf := lib.NewBlockBuf()
	out := []*StagingEntry{}
	for {
		e, err := r.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read staging entry: %v", err)
		}
		out = append(out, e)
	}
	return out
}

func findEntry(entries []*StagingEntry, path string) *StagingEntry {
	for _, e := range entries {
		if e.RepoPath.String() == path {
			return e
		}
	}
	return nil
}

// Scan the workspace and diff it against `revisionId`, the way `Merge` does.
func stagingDiff(
	t *testing.T,
	r *lib.TestRepository,
	w *TestWorkspace,
	revisionId lib.RevisionId,
) []lib.TestRevisionEntryInfo {
	t.Helper()
	assert := lib.NewAssert(t)
	staging, err := NewStaging(w.Workspace.FS, lib.Path{}, nil, nil, nil, w.TempFS, wstd.StagingMonitor())
	assert.NoError(err)
	snapshot, err := lib.NewRevisionSnapshot(t.Context(), r.Repository, revisionId, td.NewFS(t), wstd.SnapshotMonitor())
	assert.NoError(err)
	merged, err := staging.MergeWithSnapshot(snapshot, lib.RestorableMetadataAll, false)
	assert.NoError(err)
	return r.RevisionTempInfos(merged)
}
