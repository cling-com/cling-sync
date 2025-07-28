package workspace

import (
	"io/fs"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestMerge(t *testing.T) {
	t.Parallel()
	t.Run("Happy path (no conflicts)", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		w2 := wstd.NewTestWorkspace(t, r.Repository)
		assert.Equal(true, w2.Head().IsRoot())

		// Add first commit.
		w.Write("a.txt", "a")
		w.Write("b/c.txt", "c")
		w.Write("b/e/f.txt", "f")
		w.Chmod("a.txt", 0o612)
		w.Chmod("b", 0o734)
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o612, 1, "a"},
			{"b", 0o734 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
			{"b/e", 0o700 | fs.ModeDir, 0, ""},
			{"b/e/f.txt", 0o600, 1, "f"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Merge first commit into workspace.
		_, err = Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, w2.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o612, 1, "a"},
			{"b", 0o734 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
			{"b/e", 0o700 | fs.ModeDir, 0, ""},
			{"b/e/f.txt", 0o600, 1, "f"},
		}, w2.Ls("."))

		// Add second commit that adds, updates, and removes files/directories.
		w.Write("b/d.txt", "d")
		w.Write("b/c.txt", "cc")
		// The file should be restored even though it is not writable.
		w.Chmod("b/c.txt", 0o400)
		w.Rm("a.txt")
		w.Rm("b/e")
		remoteRev2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"b", 0o734 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o400, 2, "cc"},
			{"b/d.txt", 0o600, 1, "d"},
		}, r.RevisionSnapshotFileInfos(remoteRev2, nil))

		// Merge second commit into workspace.
		localRev, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev2, w2.Head())
		assert.Equal(localRev, w2.Head())
		assert.Equal([]lib.TestFileInfo{
			{"b", 0o734 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o400, 2, "cc"},
			{"b/d.txt", 0o600, 1, "d"},
		}, w2.Ls("."))
	})

	t.Run("Local non-conflicting changes (add, update, remove) are committed", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/d.txt", "d")
		w.Write("c/e.txt", "e")
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 1, "d"},
			{"c/e.txt", 0o600, 1, "e"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Merge first commit into workspace.
		localRev1, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, w2.Head())
		assert.Equal(localRev1, w2.Head())

		// Add a second commit that adds, updates, and removes files/directories.
		w.Rm("a.txt")
		w.Write("b.txt", "bb")
		w.Write("c/f.txt", "f")
		remoteRev2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"b.txt", 0o600, 2, "bb"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 1, "d"},
			{"c/e.txt", 0o600, 1, "e"},
			{"c/f.txt", 0o600, 1, "f"},
		}, r.RevisionSnapshotFileInfos(remoteRev2, nil))

		// Add non-conflicting changes to workspace.
		w2.Write("c/d.txt", "dd")
		w2.Rm("c/e.txt")
		w2.Write("c/g/h.txt", "h")

		// Merge second commit into workspace.
		localRev2, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(localRev2, w2.Head())
		// A commit should have been created with the local changes.
		assert.Equal(w2.Head(), r.Head())
		expected := []lib.TestFileInfo{
			{"b.txt", 0o600, 2, "bb"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 2, "dd"},
			{"c/f.txt", 0o600, 1, "f"},
			{"c/g", 0o700 | fs.ModeDir, 0, ""},
			{"c/g/h.txt", 0o600, 1, "h"},
		}
		assert.Equal(expected, r.RevisionSnapshotFileInfos(w2.Head(), nil))
		assert.Equal(expected, w2.Ls("."))
	})

	t.Run("Removed files are not committed again", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		w.Write("b/c.txt", "c")
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Merge first commit into workspace.
		localRev1, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, localRev1)
		assert.Equal(localRev1, w2.Head())

		// Add second commit removing `b/`.
		w.Rm("b")
		remoteRev2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(remoteRev2, nil))

		// `b/` should still be in the workspace.
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
		}, w2.Ls("."))

		// Merge second commit into workspace. This should remove `b/`.
		localRev2, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev2, localRev2)
		assert.Equal(localRev2, w2.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, w2.Ls("."))
	})

	t.Run("Adding a file in a removed directory should be fine", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		w.Write("b/c.txt", "c")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
		}, r.RevisionSnapshotFileInfos(revId1, nil))

		// Merge first commit into workspace.
		localRev1, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(revId1, localRev1)
		assert.Equal(localRev1, w2.Head())

		// Add second commit removing `b/`.
		w.Rm("b")
		remoteRev2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(remoteRev2, nil))

		// Add `b/d.txt` in the workspace.
		w2.Write("b/d.txt", "d")

		// Merge second commit into workspace.
		localRev2, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | fs.ModeDir, 0, ""},
			{"b/d.txt", 0o600, 1, "d"},
		}, w2.Ls("."))

		// A merge commit should have been created.
		assert.NotEqual(revId1, localRev2)
		assert.NotEqual(remoteRev2, localRev2)
		assert.Equal(localRev2, w2.Head())
		assert.Equal(w2.Head(), r.Head())
	})

	t.Run("Conflict (modified file)", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Add conflicting `a.txt` in the workspace.
		w2.Write("a.txt", "aa")

		// Merge first commit into workspace.
		_, err = Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")
		conflicts, ok := err.(MergeConflictsError) //nolint:errorlint
		assert.Equal(true, ok)
		assert.Equal(1, len(conflicts))
		assert.Equal("a.txt", conflicts[0].WorkspaceEntry.Path.String())
		assert.Equal("a.txt", conflicts[0].RepositoryEntry.Path.String())
		assert.Equal(lib.RevisionEntryAdd, conflicts[0].WorkspaceEntry.Type)
		assert.Equal(lib.RevisionEntryAdd, conflicts[0].RepositoryEntry.Type)
		assert.Equal(int64(2), conflicts[0].WorkspaceEntry.Metadata.Size)
		assert.Equal(int64(1), conflicts[0].RepositoryEntry.Metadata.Size)

		assert.Equal(true, w2.Head().IsRoot(), "workspace head should not be forwarded")
	})

	t.Run("Commit is aborted if remote changed", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Add local changes.
		w2.Write("b.txt", "bb")

		// Merge first commit into workspace.
		mockMon := &changeRemoteCommitMonitor{TestCommitMonitor{}, r.Repository, t, assert, false}
		mergeOptions := wstd.MergeOptions()
		mergeOptions.CommitMonitor = mockMon
		_, err = Merge(w2.Workspace, r.Repository, mergeOptions)
		assert.ErrorIs(err, lib.ErrHeadChanged)
	})

	// todo: implement
	// t.Run("MTime is restored", func(t *testing.T) {
	// 	// Make sure that mtime is restored even for directories.
	// 	t.Parallel()
	// 	t.Skip("implement")
	// })
}

func TestForceCommit(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/d.txt", "d")
		remoteRev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 1, "d"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Merge first commit into workspace.
		localRev, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, localRev)
		assert.Equal(localRev, w2.Head())

		// Add a second commit that adds, updates, and removes files/directories.
		w.Write("a.txt", "aa")
		w.Write("b.txt", "bb")
		w.Rm("c/d.txt")
		w.Write("c/f.txt", "f")
		remoteRev2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 2, "aa"},
			{"b.txt", 0o600, 2, "bb"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/f.txt", 0o600, 1, "f"},
		}, r.RevisionSnapshotFileInfos(remoteRev2, nil))

		// Add conflicting `a.txt` in the workspace.
		w2.Write("a.txt", "aaa")

		// Test that a merge would result in a conflict.
		_, err = Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")

		// Force commit local changes.
		opts := ForceCommitOptions{MergeOptions: *wstd.MergeOptions()}
		commitRev, err := ForceCommit(w2.Workspace, r.Repository, &opts)
		assert.NoError(err)
		// Both the remote and local state should be the same.
		assert.Equal(commitRev, r.Head())
		assert.Equal(commitRev, w2.Head())
		expectedState := []lib.TestFileInfo{
			{"a.txt", 0o600, 3, "aaa"},
			{"b.txt", 0o600, 2, "bb"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/f.txt", 0o600, 1, "f"},
		}
		assert.Equal(expectedState, r.RevisionSnapshotFileInfos(commitRev, nil))
		assert.Equal(expectedState, w2.Ls("."))
	})
}

type changeRemoteCommitMonitor struct {
	TestCommitMonitor
	repository *lib.Repository
	t          *testing.T
	assert     lib.Assert
	committed  bool
}

func (m *changeRemoteCommitMonitor) OnStart(entry *lib.RevisionEntry) {
	if m.committed {
		return
	}
	m.committed = true
	commit, err := lib.NewCommit(m.repository, td.NewFS(m.t))
	m.assert.NoError(err)
	err = commit.Add(td.RevisionEntry("update.txt", lib.RevisionEntryAdd))
	m.assert.NoError(err)
	_, err = commit.Commit(td.CommitInfo())
	m.assert.NoError(err)
}
