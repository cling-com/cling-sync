package workspace

import (
	"io/fs"
	"testing"
	"time"

	"github.com/cling-com/cling-sync/lib"
)

func TestImport(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// `docs/` comes from an unrelated import and must survive untouched.
		docs := td.NewTestFS(t, td.NewFS(t))
		docs.Write("readme.md", "readme")
		imp, err := NewImport(t.Context(), r.Repository, docs.FS, wstd.ImportOptions(td.Path("docs")), td.NewFS(t))
		assert.NoError(err)
		_, err = imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)

		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")
		src.Write("sub/b.txt", "b")

		blocks := blockCount(t, r)
		head := r.Head()
		imp, err = NewImport(t.Context(), r.Repository, src.FS, wstd.ImportOptions(td.Path("photos")), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{
			"A photos/a.txt",
			"A photos/sub/",
			"A photos/sub/b.txt",
		}, statusFilesString(imp.Changes))
		// The changes can be shown and refused before anything is uploaded.
		assert.Equal(blocks, blockCount(t, r), "scanning must not write blocks")
		assert.Equal(head, r.Head())

		rev, err := imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		assert.Equal(true, blockCount(t, r) > blocks, "committing should write blocks")
		assert.Equal([]lib.TestFileInfo{
			{"docs", 0o700 | fs.ModeDir, 0, ""},
			{"docs/readme.md", 0o600, 6, "readme"},
			// The destination is created, the source only provides its contents.
			{"photos", 0o700 | fs.ModeDir, 0, ""},
			{"photos/a.txt", 0o600, 1, "a"},
			{"photos/sub", 0o700 | fs.ModeDir, 0, ""},
			{"photos/sub/b.txt", 0o600, 1, "b"},
		}, r.RevisionSnapshotFileInfos(rev, nil))

		// The same source again is not a new revision.
		imp, err = NewImport(t.Context(), r.Repository, src.FS, wstd.ImportOptions(td.Path("photos")), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{}, statusFilesString(imp.Changes))
		_, err = imp.Commit(t.Context(), td.CommitInfo())
		assert.ErrorIs(err, lib.ErrEmptyCommit)
		assert.Equal(rev, r.Head())
	})

	t.Run("Existing paths are updated", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.jpg", "a")

		imp, err := NewImport(t.Context(), r.Repository, src.FS, wstd.ImportOptions(td.Path("photos")), td.NewFS(t))
		assert.NoError(err)
		_, err = imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)

		src.Write("a.jpg", "changed")
		imp, err = NewImport(t.Context(), r.Repository, src.FS, wstd.ImportOptions(td.Path("photos")), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{"M photos/a.jpg"}, statusFilesString(imp.Changes))

		rev, err := imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"photos", 0o700 | fs.ModeDir, 0, ""},
			{"photos/a.jpg", 0o600, 7, "changed"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("Paths the source no longer has are kept", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("keep.jpg", "keep")
		src.Write("gone.jpg", "gone")

		imp, err := NewImport(t.Context(), r.Repository, src.FS, wstd.ImportOptions(td.Path("photos")), td.NewFS(t))
		assert.NoError(err)
		_, err = imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)

		src.Rm("gone.jpg")
		src.Write("new.jpg", "new")
		imp, err = NewImport(t.Context(), r.Repository, src.FS, wstd.ImportOptions(td.Path("photos")), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{"A photos/new.jpg"}, statusFilesString(imp.Changes))

		rev, err := imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"photos", 0o700 | fs.ModeDir, 0, ""},
			{"photos/gone.jpg", 0o600, 4, "gone"},
			{"photos/keep.jpg", 0o600, 4, "keep"},
			{"photos/new.jpg", 0o600, 3, "new"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("Change metadata only", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")

		imp, err := NewImport(t.Context(), r.Repository, src.FS, wstd.ImportOptions(td.Path("photos")), td.NewFS(t))
		assert.NoError(err)
		_, err = imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)

		src.Touch("a.txt", time.Now().Add(time.Hour))
		opts := wstd.ImportOptions(td.Path("photos"))
		mon := wstd.CommitMonitor()
		opts.CommitMonitor = mon
		imp, err = NewImport(t.Context(), r.Repository, src.FS, opts, td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{"M photos/a.txt"}, statusFilesString(imp.Changes))

		rev, err := imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		// The content is unchanged, so the blocks in the repository are reused.
		assert.Equal(0, len(mon.OnAddBlockCalls), "the file should not have been read again")
		assert.Equal([]lib.TestFileInfo{
			{"photos", 0o700 | fs.ModeDir, 0, ""},
			{"photos/a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("Include and Exclude", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.jpg", "a")
		src.Write("raw/b.jpg", "b")
		src.Write("raw/b.raw", "raw")
		src.Write("notes.txt", "notes")

		opts := wstd.ImportOptions(td.Path("photos"))
		opts.Include = lib.NewPathInclusionFilter([]string{"**/*.jpg"})
		opts.Exclude = lib.NewPathExclusionFilter([]string{"raw"})
		imp, err := NewImport(t.Context(), r.Repository, src.FS, opts, td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{"A photos/a.jpg"}, statusFilesString(imp.Changes))

		rev, err := imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"photos", 0o700 | fs.ModeDir, 0, ""},
			{"photos/a.jpg", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("PathPrefix scopes the destination and the reported paths", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")

		opts := wstd.ImportOptions(td.Path("backup"))
		opts.PathPrefix = td.Path("look/here")
		imp, err := NewImport(t.Context(), r.Repository, src.FS, opts, td.NewFS(t))
		assert.NoError(err)
		// The prefix is not repeated in the reported paths, the same as `ls`.
		assert.Equal([]string{"A backup/a.txt"}, statusFilesString(imp.Changes))

		rev, err := imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"look", 0o700 | fs.ModeDir, 0, ""},
			{"look/here", 0o700 | fs.ModeDir, 0, ""},
			{"look/here/backup", 0o700 | fs.ModeDir, 0, ""},
			{"look/here/backup/a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("Importing into the root", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")

		imp, err := NewImport(t.Context(), r.Repository, src.FS, wstd.ImportOptions(lib.Path{}), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{"A a.txt"}, statusFilesString(imp.Changes))

		rev, err := imp.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("Commit is aborted if the repository changed", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		src := td.NewTestFS(t, td.NewFS(t))
		src.Write("a.txt", "a")

		imp, err := NewImport(t.Context(), r.Repository, src.FS, wstd.ImportOptions(td.Path("photos")), td.NewFS(t))
		assert.NoError(err)

		// Someone else commits between the scan and the commit.
		other := td.NewTestFS(t, td.NewFS(t))
		other.Write("b.txt", "b")
		imp2, err := NewImport(t.Context(), r.Repository, other.FS, wstd.ImportOptions(td.Path("docs")), td.NewFS(t))
		assert.NoError(err)
		_, err = imp2.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)

		_, err = imp.Commit(t.Context(), td.CommitInfo())
		assert.ErrorIs(err, lib.ErrHeadChanged)
	})
}

func blockCount(t *testing.T, r *lib.TestRepository) int {
	t.Helper()
	count := 0
	err := r.Storage.ReadBlockIds(t.Context(), func(lib.BlockId) bool {
		count++
		return true
	})
	lib.NewAssert(t).NoError(err)
	return count
}
