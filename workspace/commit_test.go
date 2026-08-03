package workspace

import (
	"io/fs"
	"testing"

	"github.com/cling-com/cling-sync/lib"
)

func TestCommitFiles(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		src.Write("sub/b.txt", "b")

		rev, err := CommitFiles(
			t.Context(),
			commitFilesSrc(t, r, src, lib.Path{}),
			commitFilesDest(t, r),
			wstd.CommitFilesOptions(),
			td.NewFS(t),
		)
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"sub", 0o700 | fs.ModeDir, 0, ""},
			{"sub/b.txt", 0o600, 1, "b"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("Committing no files", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))

		_, err := CommitFiles(
			t.Context(),
			commitFilesSrc(t, r, src, lib.Path{}),
			commitFilesDest(t, r),
			wstd.CommitFilesOptions(),
			td.NewFS(t),
		)
		assert.ErrorIs(err, lib.ErrEmptyCommit)
	})

	t.Run("Change metadata only", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		_, err := CommitFiles(
			t.Context(),
			commitFilesSrc(t, r, src, lib.Path{}),
			commitFilesDest(t, r),
			wstd.CommitFilesOptions(),
			td.NewFS(t),
		)
		assert.NoError(err)

		src.Chmod("a.txt", 0o640)
		mon := wstd.CommitMonitor()
		opts := wstd.CommitFilesOptions()
		opts.Monitor = mon
		opts.RestorableMetadataFlag = lib.RestorableMetadataMode
		rev, err := CommitFiles(
			t.Context(),
			commitFilesSrc(t, r, src, lib.Path{}),
			commitFilesDest(t, r),
			opts,
			td.NewFS(t),
		)
		assert.NoError(err)
		// The content is unchanged, so the blocks in the repository are reused.
		assert.Equal(0, len(mon.OnAddBlockCalls), "the file should not have been read again")
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o640, 1, "a"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("A file the destination already holds is left out", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		files := commitFilesSrc(t, r, src, lib.Path{})
		_, err := CommitFiles(t.Context(), files, commitFilesDest(t, r), wstd.CommitFilesOptions(), td.NewFS(t))
		assert.NoError(err)

		// The same files again, committed onto the revision that now holds them.
		// Nothing is left, which is what `merge` runs into when its changes were
		// computed against the workspace head rather than the repository head.
		_, err = CommitFiles(t.Context(), files, commitFilesDest(t, r), wstd.CommitFilesOptions(), td.NewFS(t))
		assert.ErrorIs(err, lib.ErrEmptyCommit)
	})

	t.Run("Deleted files need no source file", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		src.Write("gone.txt", "gone")
		_, err := CommitFiles(
			t.Context(),
			commitFilesSrc(t, r, src, lib.Path{}),
			commitFilesDest(t, r),
			wstd.CommitFilesOptions(),
			td.NewFS(t),
		)
		assert.NoError(err)

		src.Rm("gone.txt")
		rev, err := CommitFiles(
			t.Context(),
			commitFilesSrc(t, r, src, lib.Path{}),
			commitFilesDest(t, r),
			wstd.CommitFilesOptions(),
			td.NewFS(t),
		)
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("Vanished source file", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		files := commitFilesSrc(t, r, src, lib.Path{})

		src.Rm("a.txt")
		_, err := CommitFiles(t.Context(), files, commitFilesDest(t, r), wstd.CommitFilesOptions(), td.NewFS(t))
		assert.ErrorIs(err, ErrSourceVanished)
	})

	t.Run("Modified source file", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		files := commitFilesSrc(t, r, src, lib.Path{})

		// Same size, so the change is only caught by the hash.
		src.Write("a.txt", "b")
		_, err := CommitFiles(t.Context(), files, commitFilesDest(t, r), wstd.CommitFilesOptions(), td.NewFS(t))
		assert.ErrorIs(err, ErrSourceModified)
	})

	t.Run("Commit is aborted if the repository changed", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		files := commitFilesSrc(t, r, src, lib.Path{})
		dest := commitFilesDest(t, r)

		// Someone else commits in between.
		other := td.NewTestFS(t, td.NewFS(t))
		other.Write("b.txt", "b")
		_, err := CommitFiles(
			t.Context(),
			commitFilesSrc(t, r, other, lib.Path{}),
			commitFilesDest(t, r),
			wstd.CommitFilesOptions(),
			td.NewFS(t),
		)
		assert.NoError(err)

		_, err = CommitFiles(t.Context(), files, dest, wstd.CommitFilesOptions(), td.NewFS(t))
		assert.ErrorIs(err, lib.ErrHeadChanged)
	})

	t.Run("Cancel", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		// Every hook is a cancellation point, so every one of them has to abort
		// the commit rather than swallow the error.
		for _, hook := range []string{"OnBeforeCommit", "OnStart", "OnAddBlock", "OnEnd"} {
			r := td.NewTestRepository(t, td.NewFS(t))
			src := td.NewTestFS(t, td.NewFS(t))
			src.Write("a.txt", "a")
			opts := wstd.CommitFilesOptions()
			opts.Monitor = &cancelCommitMonitor{TestCommitMonitor{}, hook} //nolint:exhaustruct

			_, err := CommitFiles(
				t.Context(),
				commitFilesSrc(t, r, src, lib.Path{}),
				commitFilesDest(t, r),
				opts,
				td.NewFS(t),
			)
			assert.ErrorIs(err, lib.ErrCancel, "cancelling from %s should abort the commit", hook)
			assert.Equal(true, r.Head().IsRoot(), "cancelling from %s must not create a revision", hook)
		}
	})

	t.Run("Commit is aborted if the repository changes while committing", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		// The head is still current when the commit starts and moves underneath it.
		mon := &changeRemoteCommitMonitor{TestCommitMonitor{}, r.Repository, t, assert, false} //nolint:exhaustruct
		opts := wstd.CommitFilesOptions()
		opts.Monitor = mon

		_, err := CommitFiles(
			t.Context(),
			commitFilesSrc(t, r, src, lib.Path{}),
			commitFilesDest(t, r),
			opts,
			td.NewFS(t),
		)
		assert.ErrorIs(err, lib.ErrHeadChanged)
	})

	t.Run("SrcPrefix places the source below it", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")

		rev, err := CommitFiles(
			t.Context(),
			commitFilesSrc(t, r, src, td.Path("look/here")),
			commitFilesDest(t, r),
			wstd.CommitFilesOptions(),
			td.NewFS(t),
		)
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			// The prefix is created even though the source has no such directory.
			{"look", 0o700 | fs.ModeDir, 0, ""},
			{"look/here", 0o700 | fs.ModeDir, 0, ""},
			{"look/here/a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})
}

// A commit monitor that returns `lib.ErrCancel` from one of its hooks.
type cancelCommitMonitor struct {
	TestCommitMonitor
	at string
}

func newCancelCommitMonitor() *cancelCommitMonitor {
	return &cancelCommitMonitor{TestCommitMonitor{}, "OnStart"} //nolint:exhaustruct
}

func (m *cancelCommitMonitor) OnBeforeCommit() error {
	return m.cancel("OnBeforeCommit")
}

func (m *cancelCommitMonitor) OnStart(entry *lib.RevisionEntry) error {
	return m.cancel("OnStart")
}

func (m *cancelCommitMonitor) OnAddBlock(
	entry *lib.RevisionEntry,
	blockId lib.BlockId,
	dataSize int,
	dataBytesWritten *int,
) error {
	return m.cancel("OnAddBlock")
}

func (m *cancelCommitMonitor) OnEnd(entry *lib.RevisionEntry) error {
	return m.cancel("OnEnd")
}

func (m *cancelCommitMonitor) cancel(hook string) error {
	if m.at != hook {
		return nil
	}
	return lib.ErrCancel
}

// A commit monitor that commits an unrelated revision while the commit it is
// watching is still running.
type changeRemoteCommitMonitor struct {
	TestCommitMonitor
	repository *lib.Repository
	t          *testing.T
	assert     lib.Assert
	committed  bool
}

func (m *changeRemoteCommitMonitor) OnStart(entry *lib.RevisionEntry) error {
	if m.committed {
		return nil
	}
	m.committed = true
	commit, err := lib.NewCommit(m.t.Context(), m.repository, td.NewFS(m.t))
	m.assert.NoError(err)
	err = commit.Add(td.RevisionEntry("update.txt", lib.RevisionEntryKindAdd))
	m.assert.NoError(err)
	_, err = commit.Commit(m.t.Context(), td.CommitInfo())
	m.assert.NoError(err)
	return nil
}

// The head revision of `r`, to commit onto.
func commitFilesDest(t *testing.T, r *lib.TestRepository) *CommitFilesDest {
	t.Helper()
	assert := lib.NewAssert(t)
	head := r.Head()
	snapshot, err := lib.NewRevisionSnapshot(t.Context(), r.Repository, head, td.NewFS(t), wstd.SnapshotMonitor())
	assert.NoError(err)
	cache, err := lib.NewRevisionEntryTempCache(snapshot, 10)
	assert.NoError(err)
	return &CommitFilesDest{Repository: r.Repository, RevisionId: head, Snapshot: cache}
}

// Scan `src` and diff it against the head revision of `r`.
func commitFilesSrc(t *testing.T, r *lib.TestRepository, src *lib.TestFS, prefix lib.Path) *CommitFilesSrc {
	t.Helper()
	assert := lib.NewAssert(t)
	staging, err := NewStaging(src.FS, prefix, nil, nil, nil, td.NewFS(t), wstd.StagingMonitor())
	assert.NoError(err)
	snapshot, err := lib.NewRevisionSnapshot(t.Context(), r.Repository, r.Head(), td.NewFS(t), wstd.SnapshotMonitor())
	assert.NoError(err)
	files, err := staging.MergeWithSnapshot(snapshot, lib.RestorableMetadataAll, false)
	assert.NoError(err)
	return &CommitFilesSrc{Src: src.FS, SrcPrefix: prefix, Files: files}
}
