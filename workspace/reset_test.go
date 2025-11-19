package workspace

import (
	"io/fs"
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

func TestReset(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("dir1/a.txt", "da")
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"dir1", 0o700 | fs.ModeDir, 0, ""},
			{"dir1/a.txt", 0o600, 2, "da"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Add a second commit.
		w.Write("a.txt", "aa")
		w.Rm("b.txt")
		w.Write("dir1/b.txt", "db")
		remoteRev2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 2, "aa"},
			{"dir1", 0o700 | fs.ModeDir, 0, ""},
			{"dir1/a.txt", 0o600, 2, "da"},
			{"dir1/b.txt", 0o600, 2, "db"},
		}, r.RevisionSnapshotFileInfos(remoteRev2, nil))

		// Reset to the first commit.
		err = Reset(w.Workspace, r.Repository, wstd.ResetOptions(remoteRev1, false))
		assert.NoError(err)
		assert.Equal(remoteRev1, w.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"dir1", 0o700 | fs.ModeDir, 0, ""},
			{"dir1/a.txt", 0o600, 2, "da"},
		}, w.Ls("."))

		// Reset to the second commit again.
		err = Reset(w.Workspace, r.Repository, wstd.ResetOptions(remoteRev2, false))
		assert.NoError(err)
		assert.Equal(remoteRev2, w.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 2, "aa"},
			{"dir1", 0o700 | fs.ModeDir, 0, ""},
			{"dir1/a.txt", 0o600, 2, "da"},
			{"dir1/b.txt", 0o600, 2, "db"},
		}, w.Ls("."))
	})

	t.Run("Handle local changes (fail or force)", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Add a second commit.
		w.Write("b.txt", "b")
		remoteRev2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
		}, r.RevisionSnapshotFileInfos(remoteRev2, nil))

		// Make a local change, reset should fail.
		w.Write("a.txt", "aa")
		w.Write("b.txt", "b")
		err = Reset(w.Workspace, r.Repository, wstd.ResetOptions(remoteRev1, false))
		assert.Error(err, "Reset aborted due to local changes")
		assert.Equal(remoteRev2, w.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 2, "aa"},
			{"b.txt", 0o600, 1, "b"},
		}, w.Ls("."))

		// Force to ignore local changes.
		err = Reset(w.Workspace, r.Repository, wstd.ResetOptions(remoteRev1, true))
		assert.NoError(err)
		assert.Equal(remoteRev1, w.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, w.Ls("."))
	})

	t.Run("With path-prefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		prefixW := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "prefix/")

		// Add first commit.
		w.Write("a.txt", "a")
		w.Write("prefix/a.txt", "prefix_a")
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add a second commit.
		w.Write("b.txt", "b")
		w.Write("prefix/a.txt", "prefix_a2")
		w.Write("prefix/b.txt", "prefix_b")
		remoteRev2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Reset to the first commit.
		err = Reset(prefixW.Workspace, r.Repository, wstd.ResetOptions(remoteRev1, false))
		assert.NoError(err)
		assert.Equal(remoteRev1, prefixW.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 8, "prefix_a"},
		}, prefixW.Ls("."))

		// Reset to second commit.
		err = Reset(prefixW.Workspace, r.Repository, wstd.ResetOptions(remoteRev2, false))
		assert.NoError(err)
		assert.Equal(remoteRev2, prefixW.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 9, "prefix_a2"},
			{"b.txt", 0o600, 8, "prefix_b"},
		}, prefixW.Ls("."))
	})

	t.Run("RestorableMetadata is respected", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		rev1Time := time.Now()
		w.Touch("a.txt", rev1Time)
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Modify mtime of a.txt.
		modTime := time.Now()
		w.Touch("a.txt", modTime)

		// Reset to the first commit ignoring all metadata changes.
		opts := wstd.ResetOptions(remoteRev1, false)
		opts.RestorableMetadataFlag = 0
		err = Reset(w.Workspace, r.Repository, opts)
		assert.NoError(err)
		assert.Equal(modTime, w.Stat("a.txt").ModTime(), "file modification time should be preserved")

		// Reset with `chtime` should fail because a local change is detected.
		opts.RestorableMetadataFlag = lib.RestorableMetadataMTime
		err = Reset(w.Workspace, r.Repository, opts)
		assert.Error(err, "Reset aborted due to local changes")
		assert.Equal(modTime, w.Stat("a.txt").ModTime(), "file modification time should not be changed")

		// Reset with `chtime` and `force` should succeed.
		opts.Force = true
		err = Reset(w.Workspace, r.Repository, opts)
		assert.NoError(err)
		assert.Equal(rev1Time, w.Stat("a.txt").ModTime(), "file modification time should have been reset")
	})
}
