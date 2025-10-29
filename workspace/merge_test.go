package workspace

import (
	"io/fs"
	"syscall"
	"testing"
	"time"

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

		// Merging again should not do anything.
		_, err = Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.ErrorIs(err, ErrUpToDate)
	})

	t.Run("Nested directories are deleted depth-first", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit with a nested directory.
		w.Write("dir1/a.txt", "a")
		w.Write("dir1/dir2/b.txt", "b")
		w.Write("dir1/dir2/dir3/c.txt", "c")
		_, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge into the second workspace.
		_, err = Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Delete everything in the first workspace.
		w.Rm("dir1")
		_, err = Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge into the second workspace again.
		_, err = Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// The second workspace should be empty.
		assert.Equal([]lib.TestFileInfo{}, w2.Ls("."))
	})

	t.Run("Change metadata only", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		_, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add a second commit that changes the metadata only.
		w.Chmod("a.txt", 0o700)
		_, err = Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o700, 1, "a"},
		}, r.RevisionSnapshotFileInfos(w.Head(), nil))
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

	t.Run("File ownership can be ignored when detecting local changes", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		// We use a memory FS because with a real FS, we might not be able to change the ownership.
		w2 := wstd.NewTestWorkspaceExtra(t, r.Repository, "", lib.NewMemoryFS(10000000))

		w.Write("a.txt", "a")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w2revId1, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(revId1, w2revId1)

		// Change the ownership of `a.txt` in w2.
		w2.Chown("a.txt", 1234, 5678)
		w2.Chmod("a.txt", 0o700)
		_, err = Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge the changes - this should fail because the GUID and/or UID should not exist.
		opts := wstd.MergeOptions()
		opts.Chown = true
		_, err = Merge(w.Workspace, r.Repository, opts)
		assert.Error(err, "failed to restore file owner 1234 and group 5678 for a.txt")

		// Try a second time without `Chown`.
		opts.Chown = false
		_, err = Merge(w.Workspace, r.Repository, opts)
		assert.NoError(err)
		assert.Equal(fs.FileMode(0o700).Perm(), w.Stat("a.txt").Mode().Perm())
	})

	t.Run("File ownership can be ignored when detecting conflicts", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		// We use a memory FS because with a real FS, we might not be able to change the ownership.
		w := wstd.NewTestWorkspaceExtra(t, r.Repository, "", lib.NewMemoryFS(10000000))
		w2 := wstd.NewTestWorkspaceExtra(t, r.Repository, "", lib.NewMemoryFS(10000000))

		w.Write("a.txt", "a")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w2revId1, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(revId1, w2revId1)

		// Change the ownership of `a.txt` in `w2` and merge.
		w2.Chown("a.txt", 1234, 5678)
		w2revId2, err := Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Change the ownership of `a.txt` in `w`.
		w.Chown("a.txt", 8765, 4321)

		// Merge should fail if we take ownership into account.
		opts := wstd.MergeOptions()
		opts.Chown = true
		_, err = Merge(w.Workspace, r.Repository, opts)
		assert.Error(err, "MergeConflictsError")

		// Merge should succeed if we ignore ownership.
		opts.Chown = false
		revId1, err = Merge(w.Workspace, r.Repository, opts)
		assert.NoError(err)
		assert.Equal(revId1, w2revId2)
		// The ownership should not have been changed.
		stat := w2.Stat("a.txt")
		assert.Equal(uint32(1234), stat.Sys().(*syscall.Stat_t).Uid) //nolint:forcetypeassert
		assert.Equal(uint32(5678), stat.Sys().(*syscall.Stat_t).Gid) //nolint:forcetypeassert
	})

	t.Run("CpMonitor", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Create a commit with only local changes. We don't expect any calls to the CpMonitor.
		w.Write("a.txt", "a")
		mon := wstd.CpMonitor()
		opts := wstd.MergeOptions()
		opts.CpMonitor = mon
		_, err := Merge(w.Workspace, r.Repository, opts)
		assert.NoError(err)
		assert.Equal(0, len(mon.OnStartCalls))
		assert.Equal(0, len(mon.OnWriteCalls))
		assert.Equal(0, len(mon.OnExistsCalls))
		assert.Equal(0, len(mon.OnEndCalls))
		assert.Equal(0, len(mon.OnErrorCalls))

		// Create a second workspace and merge. This should trigger the CpMonitor.
		w2 := wstd.NewTestWorkspace(t, r.Repository)
		_, err = Merge(w2.Workspace, r.Repository, opts)
		assert.NoError(err)

		assert.Equal(1, len(mon.OnStartCalls))
		assert.Equal(1, len(mon.OnWriteCalls))
		assert.Equal(0, len(mon.OnExistsCalls))
		assert.Equal(1, len(mon.OnEndCalls))
		assert.Equal(0, len(mon.OnErrorCalls))
	})

	t.Run("Adding the same file in repository and workspace is ignored", func(t *testing.T) {
		// This is the case if a merge is aborted, but files were already copied from the repository.
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		mtime := time.Now()
		w.Write("a.txt", "a")
		w.Touch("a.txt", mtime)
		_, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Adding the same file with the same attributes should be ignored.
		w2.Write("a.txt", "a")
		w2.Touch("a.txt", mtime)
		_, err = Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.ErrorIs(err, lib.ErrEmptyCommit)

		// But having a different mtime should not be ignored.
		w2.Touch("a.txt", time.Now())
		_, err = Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")
	})

	// todo: implement
	// t.Run("MTime is restored", func(t *testing.T) {
	// 	// Make sure that mtime is restored even for directories.
	// 	t.Parallel()
	// 	t.Skip("implement")
	// })
}

func TestMergeWithPathPrefix(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		rootW := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		prefixW := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")

		// Add first commit	to the workspace that sees the whole repository (rootW).
		rootW.Write("a.txt", "a")
		rootW.Mkdir("dir1")
		rootW.Write("dir1/b.txt", "b")
		rootW.MkdirAll("look/here")
		_, err := Merge(rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"dir1", 0o700 | fs.ModeDir, 0, ""},
			{"dir1/b.txt", 0o600, 1, "b"},
			{"look", 0o700 | fs.ModeDir, 0, ""},
			{"look/here", 0o700 | fs.ModeDir, 0, ""},
		}, r.RevisionSnapshotFileInfos(rootW.Head(), nil))

		// Merging the commit into the prefixed workspace should not create any files.
		_, err = Merge(prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(0, len(prefixW.Ls(".")))
		assert.Equal(rootW.Head(), prefixW.Head())

		// Adding files to the prefixed workspace.
		prefixW.Write("c.txt", "c")
		prefixW.Mkdir("dir2")
		prefixW.Write("dir2/d.txt", "d")
		rev, err := Merge(prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(rev, prefixW.Head())
		assert.Equal(rev, r.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"dir1", 0o700 | fs.ModeDir, 0, ""},
			{"dir1/b.txt", 0o600, 1, "b"},
			{"look", 0o700 | fs.ModeDir, 0, ""},
			{"look/here", 0o700 | fs.ModeDir, 0, ""},
			{"look/here/c.txt", 0o600, 1, "c"},
			{"look/here/dir2", 0o700 | fs.ModeDir, 0, ""},
			{"look/here/dir2/d.txt", 0o600, 1, "d"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"look/here/c.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("c")},
			{"look/here/dir2", lib.RevisionEntryAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"look/here/dir2/d.txt", lib.RevisionEntryAdd, 0o600, td.SHA256("d")},
		}, r.RevisionInfos(rev))

		// Merging again should not do anything.
		_, err = Merge(prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.ErrorIs(err, ErrUpToDate)
		assert.Equal(rev, prefixW.Head())
	})

	t.Run("PathPrefix is created if it does not exist", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		// Create a second workspace tied to the same repository.
		prefixW := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")

		// Merging the commit into the prefixed workspace should create the path prefix.
		prefixW.Write("a.txt", "a")
		rev, err := Merge(prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"look", 0o700 | fs.ModeDir, 0, ""},
			{"look/here", 0o700 | fs.ModeDir, 0, ""},
			{"look/here/a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("Remote and local changes are merged (non-conflicting)", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		rootW := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		prefixW := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")

		// Add first commit	to the workspace that sees the whole repository (rootW).
		rootW.Write("a.txt", "a")
		rootW.MkdirAll("look/here/dir1")
		rootW.Write("look/here/b.txt", "b")
		rootW.Write("look/here/dir1/b.txt", "b")
		rootW.Write("look/here.txt", "here")
		_, err := Merge(rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge into the prefixed workspace.
		rev, err := Merge(prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(rev, prefixW.Head())
		assert.Equal(rev, r.Head())
		assert.Equal([]lib.TestFileInfo{
			{"b.txt", 0o600, 1, "b"},
			{"dir1", 0o700 | fs.ModeDir, 0, ""},
			{"dir1/b.txt", 0o600, 1, "b"},
		}, prefixW.Ls("."))

		// Now add changes to the repository.
		rootW.Write("a.txt", "aa")
		rootW.Rm("look/here/b.txt")
		rootW.Write("c.txt", "c")
		_, err = Merge(rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add changes to the prefixed workspace and merge.
		prefixW.Write("a.txt", "this is a different file")
		prefixW.Rm("dir1/b.txt")

		rev, err = Merge(prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(rev, prefixW.Head())
		assert.Equal(rev, r.Head())
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 2, "aa"},
			{"c.txt", 0o600, 1, "c"},
			{"look", 0o700 | fs.ModeDir, 0, ""},
			{"look/here.txt", 0o600, 4, "here"},
			{"look/here", 0o700 | fs.ModeDir, 0, ""},
			{"look/here/a.txt", 0o600, 24, "this is a different file"},
			{"look/here/dir1", 0o700 | fs.ModeDir, 0, ""},
		}, r.RevisionSnapshotFileInfos(rev, nil))
	})

	t.Run("Conflict (modified file)", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		wRoot := wstd.NewTestWorkspace(t, r.Repository)
		wPrefix := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")

		// Add first commit to the root workspace.
		wRoot.Write("look/here/a.txt", "a")
		_, err := Merge(wRoot.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add conflicting `a.txt` in the prefixed workspace.
		wPrefix.Write("a.txt", "aa")

		// Merge first commit into the prefixed workspace.
		_, err = Merge(wPrefix.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")

		conflicts, ok := err.(MergeConflictsError) //nolint:errorlint
		assert.Equal(true, ok)
		assert.Equal(1, len(conflicts))
		assert.Equal("a.txt", conflicts[0].WorkspaceEntry.Path.String())
		assert.Equal("look/here/a.txt", conflicts[0].RepositoryEntry.Path.String())
		assert.Equal(lib.RevisionEntryAdd, conflicts[0].WorkspaceEntry.Type)
		assert.Equal(lib.RevisionEntryAdd, conflicts[0].RepositoryEntry.Type)
		assert.Equal(int64(2), conflicts[0].WorkspaceEntry.Metadata.Size)
		assert.Equal(int64(1), conflicts[0].RepositoryEntry.Metadata.Size)

		assert.Equal(true, wPrefix.Head().IsRoot(), "prefixed workspace head should not be forwarded")
	})
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

	t.Run("Workspace with path prefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)

		r := td.NewTestRepository(t, td.NewFS(t))
		rootW := wstd.NewTestWorkspace(t, r.Repository)
		// Create a second workspace tied to the same repository.
		prefixW := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")

		// Add first commit to the root workspace.
		rootW.Write("a.txt", "a")
		rootW.MkdirAll("look/here")
		rootW.Write("look/here/b.txt", "b")
		_, err := Merge(rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge into the prefixed workspace.
		_, err = Merge(prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add conflicts inside and outside the path prefix.
		rootW.Write("a.txt", "from root workspace")
		rootW.Write("look/here/b.txt", "bb")
		_, err = Merge(rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		prefixW.Write("b.txt", "from prefixed workspace")

		// Merge should fail.
		_, err = Merge(prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")

		// But force commit should succeed.
		commitRev, err := ForceCommit(prefixW.Workspace, r.Repository, &ForceCommitOptions{*wstd.MergeOptions()})
		assert.NoError(err)
		assert.Equal(r.Head(), commitRev)
		assert.Equal(prefixW.Head(), commitRev)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 19, "from root workspace"},
			{"look", 0o700 | fs.ModeDir, 0, ""},
			{"look/here", 0o700 | fs.ModeDir, 0, ""},
			{"look/here/b.txt", 0o600, 23, "from prefixed workspace"},
		}, r.RevisionSnapshotFileInfos(commitRev, nil))
		assert.Equal([]lib.TestFileInfo{
			{"b.txt", 0o600, 23, "from prefixed workspace"},
		}, prefixW.Ls("."))
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
