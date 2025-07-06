package workspace

import (
	"os"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestMerge(t *testing.T) {
	t.Parallel()
	t.Run("Happy path (no conflicts)", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)

		// Create a second workspace tied to the same repository.
		wt := NewWorkspaceTest(t, rt.RepositoryStorage.Dir)
		assert.Equal(true, wt.LocalHead().IsRoot())

		// Add first commit.
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b/c.txt", "c")
		rt.AddLocal("b/e/f.txt", "f")
		rt.UpdateLocalMode("a.txt", 0o612)
		rt.UpdateLocalMode("b", 0o734)
		remoteRev1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev1, nil, []FileInfo{
			{"a.txt", 0o612, 1, "a"},
			{"b", 0o734 | os.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
			{"b/e", 0o700 | os.ModeDir, 0, ""},
			{"b/e/f.txt", 0o600, 1, "f"},
		})

		// Merge first commit into workspace.
		_, err = Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, wt.LocalHead())
		assert.Equal([]FileInfo{
			{"a.txt", 0o612, 1, "a"},
			{"b", 0o734 | os.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
			{"b/e", 0o700 | os.ModeDir, 0, ""},
			{"b/e/f.txt", 0o600, 1, "f"},
		}, readDir(t, wt.WorkspacePath))

		// Add second commit that adds, updates, and removes files/directories.
		rt.AddLocal("b/d.txt", "d")
		rt.UpdateLocal("b/c.txt", "cc")
		// The file should be restored even though it is not writable.
		rt.UpdateLocalMode("b/c.txt", 0o400)
		rt.RemoveLocal("a.txt")
		rt.RemoveLocal("b/e")
		remoteRev2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev2, nil, []FileInfo{
			{"b", 0o734 | os.ModeDir, 0, ""},
			{"b/c.txt", 0o400, 2, "cc"},
			{"b/d.txt", 0o600, 1, "d"},
		})

		// Merge second commit into workspace.
		localRev, err := Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev2, wt.LocalHead())
		assert.Equal(localRev, wt.LocalHead())
		assert.Equal([]FileInfo{
			{"b", 0o734 | os.ModeDir, 0, ""},
			{"b/c.txt", 0o400, 2, "cc"},
			{"b/d.txt", 0o600, 1, "d"},
		}, readDir(t, wt.WorkspacePath))
	})

	t.Run("Local non-conflicting changes (add, update, remove) are committed", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)
		wt := NewWorkspaceTest(t, rt.RepositoryStorage.Dir)

		// Add first commit.
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b.txt", "b")
		rt.AddLocal("c/d.txt", "d")
		rt.AddLocal("c/e.txt", "e")
		remoteRev1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev1, nil, []FileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 1, "d"},
			{"c/e.txt", 0o600, 1, "e"},
		})

		// Merge first commit into workspace.
		localRev1, err := Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, wt.LocalHead())
		assert.Equal(localRev1, wt.LocalHead())

		// Add a second commit that adds, updates, and removes files/directories.
		rt.RemoveLocal("a.txt")
		rt.UpdateLocal("b.txt", "bb")
		rt.AddLocal("c/f.txt", "f")
		remoteRev2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev2, nil, []FileInfo{
			{"b.txt", 0o600, 2, "bb"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 1, "d"},
			{"c/e.txt", 0o600, 1, "e"},
			{"c/f.txt", 0o600, 1, "f"},
		})

		// Add non-conflicting changes to workspace.
		wt.UpdateLocal("c/d.txt", "dd")
		wt.RemoveLocal("c/e.txt")
		wt.AddLocal("c/g/h.txt", "h")

		// Merge second commit into workspace.
		localRev2, err := Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		assert.Equal(localRev2, wt.LocalHead())
		// A commit should have been created with the local changes.
		assert.Equal(wt.LocalHead(), rt.RemoteHead())
		expected := []FileInfo{
			{"b.txt", 0o600, 2, "bb"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 2, "dd"},
			{"c/f.txt", 0o600, 1, "f"},
			{"c/g", 0o700 | os.ModeDir, 0, ""},
			{"c/g/h.txt", 0o600, 1, "h"},
		}
		rt.VerifyRevisionSnapshot(wt.LocalHead(), nil, expected)
		assert.Equal(expected, readDir(t, wt.WorkspacePath))
	})

	t.Run("Removed files are not committed again", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)
		wt := NewWorkspaceTest(t, rt.RepositoryStorage.Dir)

		// Add first commit.
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b/c.txt", "c")
		remoteRev1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev1, nil, []FileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | os.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
		})

		// Merge first commit into workspace.
		localRev1, err := Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, localRev1)
		assert.Equal(localRev1, wt.LocalHead())

		// Add second commit removing `b/`.
		rt.RemoveLocal("b")
		remoteRev2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev2, nil, []FileInfo{
			{"a.txt", 0o600, 1, "a"},
		})

		// `b/` should still be in the workspace.
		assert.Equal([]FileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | os.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
		}, readDir(t, wt.WorkspacePath))

		// Merge second commit into workspace. This should remove `b/`.
		localRev2, err := Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev2, localRev2)
		assert.Equal(localRev2, wt.LocalHead())
		assert.Equal([]FileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, readDir(t, wt.WorkspacePath))
	})

	t.Run("Adding a file in a removed directory should be fine", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)
		wt := NewWorkspaceTest(t, rt.RepositoryStorage.Dir)

		// Add first commit.
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b/c.txt", "c")
		revId1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(revId1, nil, []FileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | os.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
		})

		// Merge first commit into workspace.
		localRev1, err := Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		assert.Equal(revId1, localRev1)
		assert.Equal(localRev1, wt.LocalHead())

		// Add second commit removing `b/`.
		rt.RemoveLocal("b")
		remoteRev2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev2, nil, []FileInfo{
			{"a.txt", 0o600, 1, "a"},
		})

		// Add `b/d.txt` in the workspace.
		wt.AddLocal("b/d.txt", "d")

		// Merge second commit into workspace.
		localRev2, err := Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		assert.Equal([]FileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | os.ModeDir, 0, ""},
			{"b/d.txt", 0o600, 1, "d"},
		}, readDir(t, wt.WorkspacePath))

		// A merge commit should have been created.
		assert.NotEqual(revId1, localRev2)
		assert.NotEqual(remoteRev2, localRev2)
		assert.Equal(localRev2, wt.LocalHead())
		assert.Equal(wt.LocalHead(), rt.RemoteHead())
	})

	t.Run("Conflict (modified file)", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)
		wt := NewWorkspaceTest(t, rt.RepositoryStorage.Dir)

		// Add first commit.
		rt.AddLocal("a.txt", "a")
		remoteRev1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev1, nil, []FileInfo{
			{"a.txt", 0o600, 1, "a"},
		})

		// Add conflicting `a.txt` in the workspace.
		wt.AddLocal("a.txt", "aa")

		// Merge first commit into workspace.
		_, err = Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.Error(err, "MergeConflictsError")
		conflicts, ok := err.(MergeConflictsError) //nolint:errorlint
		assert.Equal(true, ok)
		assert.Equal(1, len(conflicts))
		assert.Equal("a.txt", conflicts[0].WorkspaceEntry.Path.FSString())
		assert.Equal("a.txt", conflicts[0].RepositoryEntry.Path.FSString())
		assert.Equal(lib.RevisionEntryAdd, conflicts[0].WorkspaceEntry.Type)
		assert.Equal(lib.RevisionEntryAdd, conflicts[0].RepositoryEntry.Type)
		assert.Equal(int64(2), conflicts[0].WorkspaceEntry.Metadata.Size)
		assert.Equal(int64(1), conflicts[0].RepositoryEntry.Metadata.Size)

		assert.Equal(true, wt.LocalHead().IsRoot(), "workspace head should not be forwarded")
	})

	t.Run("Commit is aborted if remote changed", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)
		wt := NewWorkspaceTest(t, rt.RepositoryStorage.Dir)

		// Add first commit.
		rt.AddLocal("a.txt", "a")
		remoteRev1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev1, nil, []FileInfo{
			{"a.txt", 0o600, 1, "a"},
		})

		// Add local changes.
		wt.AddLocal("b.txt", "bb")

		// Merge first commit into workspace.
		mockMon := &changeRemoteCommitMonitor{testCommitMonitor{}, rt, false}
		mergeOptions := fakeMergeOptions()
		mergeOptions.CommitMonitor = mockMon
		_, err = Merge(wt.Workspace, rt.Repository, mergeOptions)
		assert.ErrorIs(err, lib.ErrHeadChanged)
	})

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
		rt := NewRepositoryTest(t)
		wt := NewWorkspaceTest(t, rt.RepositoryStorage.Dir)

		// Add first commit.
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b.txt", "b")
		rt.AddLocal("c/d.txt", "d")
		remoteRev1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev1, nil, []FileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 1, "d"},
		})

		// Merge first commit into workspace.
		localRev, err := Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, localRev)
		assert.Equal(localRev, wt.LocalHead())

		// Add a second commit that adds, updates, and removes files/directories.
		rt.UpdateLocal("a.txt", "aa")
		rt.UpdateLocal("b.txt", "bb")
		rt.RemoveLocal("c/d.txt")
		rt.AddLocal("c/f.txt", "f")
		remoteRev2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(remoteRev2, nil, []FileInfo{
			{"a.txt", 0o600, 2, "aa"},
			{"b.txt", 0o600, 2, "bb"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/f.txt", 0o600, 1, "f"},
		})

		// Add conflicting `a.txt` in the workspace.
		wt.UpdateLocal("a.txt", "aaa")

		// Test that a merge would result in a conflict.
		_, err = Merge(wt.Workspace, rt.Repository, fakeMergeOptions())
		assert.Error(err, "MergeConflictsError")

		// Force commit local changes.
		opts := ForceCommitOptions{MergeOptions: *fakeMergeOptions()}
		commitRev, err := ForceCommit(wt.Workspace, rt.Repository, &opts)
		assert.NoError(err)
		// Both the remote and local state should be the same.
		assert.Equal(commitRev, rt.RemoteHead())
		assert.Equal(commitRev, wt.LocalHead())
		expectedState := []FileInfo{
			{"a.txt", 0o600, 3, "aaa"},
			{"b.txt", 0o600, 2, "bb"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/f.txt", 0o600, 1, "f"},
		}
		rt.VerifyRevisionSnapshot(commitRev, nil, expectedState)
		assert.Equal(expectedState, readDir(t, wt.WorkspacePath))
	})
}

type changeRemoteCommitMonitor struct {
	testCommitMonitor
	rt        *RepositoryTest
	committed bool
}

func (m *changeRemoteCommitMonitor) OnStart(entry *lib.RevisionEntry) {
	if m.committed {
		return
	}
	m.committed = true
	commit, err := lib.NewCommit(m.rt.Repository, m.rt.t.TempDir())
	m.rt.assert.NoError(err)
	err = commit.Add(td.RevisionEntry("update.txt", lib.RevisionEntryAdd))
	m.rt.assert.NoError(err)
	_, err = commit.Commit(td.CommitInfo())
	m.rt.assert.NoError(err)
}
