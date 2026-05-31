package workspace

import (
	"errors"
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
		remoteRev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o612, 1, "a"},
			{"b", 0o734 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
			{"b/e", 0o700 | fs.ModeDir, 0, ""},
			{"b/e/f.txt", 0o600, 1, "f"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Merge first commit into workspace.
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
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
		remoteRev2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"b", 0o734 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o400, 2, "cc"},
			{"b/d.txt", 0o600, 1, "d"},
		}, r.RevisionSnapshotFileInfos(remoteRev2, nil))

		// Merge second commit into workspace.
		localRev, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev2, w2.Head())
		assert.Equal(localRev, w2.Head())
		assert.Equal([]lib.TestFileInfo{
			{"b", 0o734 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o400, 2, "cc"},
			{"b/d.txt", 0o600, 1, "d"},
		}, w2.Ls("."))

		// Merging again should not do anything.
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.ErrorIs(err, ErrUpToDate)
	})

	t.Run("Merge into non-writable directories", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		// First commit: create a file inside a directory.
		w.Write("dir/a.txt", "a")
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge into workspace 2.
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Make the directory non-writable in w2.
		w2.Chmod("dir", 0o500)

		// w1: add a new file inside the directory.
		w.Write("dir/b.txt", "b")
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge the remote changes into w2.
		// The merge should succeed despite `dir` being non-writable,
		// because `makeDirsWritable` should temporarily make it writable.
		// After the merge, `dir` should be restored to its original mode (0o500).
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Verify the new file was written.
		assert.Equal("b", w2.Cat("dir/b.txt"))

		// Verify the directory permissions were restored.
		dirStat := w2.Stat("dir")
		assert.Equal(fs.FileMode(0o500)|fs.ModeDir, dirStat.Mode(),
			"directory permissions should be restored to 0o500 after merge")
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
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge into the second workspace.
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Delete everything in the first workspace.
		w.Rm("dir1")
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge into the second workspace again.
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
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
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add a second commit that changes the metadata only.
		w.Chmod("a.txt", 0o700)
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
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
		remoteRev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 1, "d"},
			{"c/e.txt", 0o600, 1, "e"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Merge first commit into workspace.
		localRev1, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, w2.Head())
		assert.Equal(localRev1, w2.Head())

		// Add a second commit that adds, updates, and removes files/directories.
		w.Rm("a.txt")
		w.Write("b.txt", "bb")
		w.Write("c/f.txt", "f")
		remoteRev2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
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
		localRev2, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
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
		remoteRev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Merge first commit into workspace.
		localRev1, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, localRev1)
		assert.Equal(localRev1, w2.Head())

		// Add second commit removing `b/`.
		w.Rm("b")
		remoteRev2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
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
		localRev2, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
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
		revId1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b", 0o700 | fs.ModeDir, 0, ""},
			{"b/c.txt", 0o600, 1, "c"},
		}, r.RevisionSnapshotFileInfos(revId1, nil))

		// Merge first commit into workspace.
		localRev1, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(revId1, localRev1)
		assert.Equal(localRev1, w2.Head())

		// Add second commit removing `b/`.
		w.Rm("b")
		remoteRev2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
		}, r.RevisionSnapshotFileInfos(remoteRev2, nil))

		// Add `b/d.txt` in the workspace.
		w2.Write("b/d.txt", "d")

		// Merge second commit into workspace.
		localRev2, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
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

		// Both workspaces start at the same revision so the workspace head
		// is non-root and we exercise the regular conflict-detection path
		// (not the attach-non-empty adoption path).
		w.Write("a.txt", "a")
		remoteRev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, w2.Head())

		// `w` commits a divergent change.
		w.Write("a.txt", "aa")
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// `w2` introduces its own divergent change.
		w2.Write("a.txt", "aaa")

		// Merging `w2` should detect the conflict.
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")
		conflicts, ok := err.(MergeConflictsError) //nolint:errorlint
		assert.Equal(true, ok)
		assert.Equal(1, len(conflicts))
		assert.Equal("a.txt", conflicts[0].WorkspaceEntry.Path.String())
		assert.Equal("a.txt", conflicts[0].RepositoryEntry.Path.String())
		assert.Equal(lib.RevisionEntryKindUpdate, conflicts[0].WorkspaceEntry.Kind)
		assert.Equal(lib.RevisionEntryKindUpdate, conflicts[0].RepositoryEntry.Kind)
		assert.Equal(int64(3), conflicts[0].WorkspaceEntry.Metadata.Size)
		assert.Equal(int64(2), conflicts[0].RepositoryEntry.Metadata.Size)

		assert.Equal(remoteRev1, w2.Head(), "workspace head should not be forwarded")
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
		remoteRev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
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
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, mergeOptions)
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
		revId1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w2revId1, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(revId1, w2revId1)

		// Change the ownership of `a.txt` in w2.
		w2.Chown("a.txt", 1234, 5678)
		w2.Chmod("a.txt", 0o700)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge the changes - this should fail because the GUID and/or UID should not exist.
		opts := wstd.MergeOptions()
		opts.RestorableMetadataFlag |= lib.RestorableMetadataOwnership
		_, err = Merge(t.Context(), w.Workspace, r.Repository, opts)
		assert.Error(err, "failed to restore file owner 1234 and group 5678 for a.txt")

		// Try a second time without `Chown`.
		opts.RestorableMetadataFlag ^= lib.RestorableMetadataOwnership
		_, err = Merge(t.Context(), w.Workspace, r.Repository, opts)
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
		revId1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w2revId1, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(revId1, w2revId1)

		// Change the ownership of `a.txt` in `w2` and merge.
		w2.Chown("a.txt", 1234, 5678)
		w2revId2, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Change the ownership of `a.txt` in `w`.
		w.Chown("a.txt", 8765, 4321)

		// Merge should fail if we take ownership into account.
		opts := wstd.MergeOptions()
		opts.RestorableMetadataFlag |= lib.RestorableMetadataOwnership
		_, err = Merge(t.Context(), w.Workspace, r.Repository, opts)
		assert.Error(err, "MergeConflictsError")

		// Merge should succeed if we ignore ownership.
		opts.RestorableMetadataFlag ^= lib.RestorableMetadataOwnership
		revId1, err = Merge(t.Context(), w.Workspace, r.Repository, opts)
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
		_, err := Merge(t.Context(), w.Workspace, r.Repository, opts)
		assert.NoError(err)
		assert.Equal(0, len(mon.OnStartCalls))
		assert.Equal(0, len(mon.OnWriteCalls))
		assert.Equal(0, len(mon.OnExistsCalls))
		assert.Equal(0, len(mon.OnEndCalls))
		assert.Equal(0, len(mon.OnErrorCalls))

		// Create a second workspace and merge. This should trigger the CpMonitor.
		w2 := wstd.NewTestWorkspace(t, r.Repository)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, opts)
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

		// Both workspaces start at the same revision so the workspace head
		// is non-root and we exercise the regular merge code path.
		mtime1 := time.Now()
		w.Write("a.txt", "a")
		w.Touch("a.txt", mtime1)
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// `w` updates `a.txt` and commits.
		mtime2 := mtime1.Add(time.Second)
		w.Write("a.txt", "aa")
		w.Touch("a.txt", mtime2)
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// `w2` happens to make the identical update locally.
		w2.Write("a.txt", "aa")
		w2.Touch("a.txt", mtime2)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.ErrorIs(err, lib.ErrEmptyCommit)

		// But having a different mtime should not be ignored.
		w2.Touch("a.txt", time.Now())
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")
	})

	t.Run("First merge after attach to a non-empty directory", func(t *testing.T) {
		// The workspace was attached with `--allow-non-empty` and holds a
		// mix of matching, modified, local-only, and missing-remote files.
		// The first merge should:
		// - adopt matching files (no commit entry),
		// - commit modified files as UPDATE,
		// - commit local-only files as ADD,
		// - fetch remote-only files (no DELETE entry).
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		mtime := time.Now()
		w.Write("match.txt", "match")
		w.Touch("match.txt", mtime)
		w.Write("modified.txt", "old")
		w.Write("remote-only.txt", "remote")
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// `match.txt` is set to the identical mtime so the default flag (which
		// compares MTime) sees the file as truly matching.
		w2.Write("match.txt", "match")
		w2.Touch("match.txt", mtime)
		w2.Write("modified.txt", "new")
		w2.Write("local-only.txt", "local")
		assert.Equal(true, w2.Head().IsRoot())

		newHead, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(newHead, w2.Head())

		assert.Equal("match", w2.Cat("match.txt"))
		assert.Equal("new", w2.Cat("modified.txt"))
		assert.Equal("local", w2.Cat("local-only.txt"))
		assert.Equal("remote", w2.Cat("remote-only.txt"))

		// match.txt is adopted (no commit entry), remote-only.txt is fetched
		// (no DELETE), only the local divergences appear in the new revision.
		assert.Equal([]lib.TestRevisionEntryInfo{
			{"local-only.txt", lib.RevisionEntryKindAdd, 0o600, td.SHA256("local")},
			{"modified.txt", lib.RevisionEntryKindUpdate, 0o600, td.SHA256("new")},
		}, r.RevisionInfos(newHead))
	})

	t.Run("First merge after attach when workspace already matches the repository", func(t *testing.T) {
		// The workspace was attached with `--allow-non-empty` and its files
		// already match the repository byte-for-byte. The first merge
		// fast-forwards the workspace head without creating a commit.
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		mtime := time.Now()
		w.Write("a.txt", "a")
		w.Touch("a.txt", mtime)
		remoteRev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w2.Write("a.txt", "a")
		w2.Touch("a.txt", mtime)
		head, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, head, "no new revision should be created")
		assert.Equal(remoteRev1, w2.Head())
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
		_, err := Merge(t.Context(), rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"dir1", 0o700 | fs.ModeDir, 0, ""},
			{"dir1/b.txt", 0o600, 1, "b"},
			{"look", 0o700 | fs.ModeDir, 0, ""},
			{"look/here", 0o700 | fs.ModeDir, 0, ""},
		}, r.RevisionSnapshotFileInfos(rootW.Head(), nil))

		// Merging the commit into the prefixed workspace should not create any files.
		_, err = Merge(t.Context(), prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(0, len(prefixW.Ls(".")))
		assert.Equal(rootW.Head(), prefixW.Head())

		// Adding files to the prefixed workspace.
		prefixW.Write("c.txt", "c")
		prefixW.Mkdir("dir2")
		prefixW.Write("dir2/d.txt", "d")
		rev, err := Merge(t.Context(), prefixW.Workspace, r.Repository, wstd.MergeOptions())
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
			{"look/here/c.txt", lib.RevisionEntryKindAdd, 0o600, td.SHA256("c")},
			{"look/here/dir2", lib.RevisionEntryKindAdd, 0o700 | fs.ModeDir, td.SHA256("")},
			{"look/here/dir2/d.txt", lib.RevisionEntryKindAdd, 0o600, td.SHA256("d")},
		}, r.RevisionInfos(rev))

		// Merging again should not do anything.
		_, err = Merge(t.Context(), prefixW.Workspace, r.Repository, wstd.MergeOptions())
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
		rev, err := Merge(t.Context(), prefixW.Workspace, r.Repository, wstd.MergeOptions())
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
		_, err := Merge(t.Context(), rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge into the prefixed workspace.
		rev, err := Merge(t.Context(), prefixW.Workspace, r.Repository, wstd.MergeOptions())
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
		_, err = Merge(t.Context(), rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add changes to the prefixed workspace and merge.
		prefixW.Write("a.txt", "this is a different file")
		prefixW.Rm("dir1/b.txt")

		rev, err = Merge(t.Context(), prefixW.Workspace, r.Repository, wstd.MergeOptions())
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

		// Both workspaces start at the same revision so the prefix workspace
		// head is non-root and we exercise the regular conflict-detection
		// path (not the attach-non-empty adoption path).
		wRoot.Write("look/here/a.txt", "a")
		remoteRev1, err := Merge(t.Context(), wRoot.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), wPrefix.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, wPrefix.Head())

		// `wRoot` commits a divergent change.
		wRoot.Write("look/here/a.txt", "aa")
		_, err = Merge(t.Context(), wRoot.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// `wPrefix` introduces its own divergent change.
		wPrefix.Write("a.txt", "aaa")

		_, err = Merge(t.Context(), wPrefix.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")

		conflicts, ok := err.(MergeConflictsError) //nolint:errorlint
		assert.Equal(true, ok)
		assert.Equal(1, len(conflicts))
		assert.Equal("a.txt", conflicts[0].WorkspaceEntry.Path.String())
		assert.Equal("look/here/a.txt", conflicts[0].RepositoryEntry.Path.String())
		assert.Equal(lib.RevisionEntryKindUpdate, conflicts[0].WorkspaceEntry.Kind)
		assert.Equal(lib.RevisionEntryKindUpdate, conflicts[0].RepositoryEntry.Kind)
		assert.Equal(int64(3), conflicts[0].WorkspaceEntry.Metadata.Size)
		assert.Equal(int64(2), conflicts[0].RepositoryEntry.Metadata.Size)

		assert.Equal(remoteRev1, wPrefix.Head(), "prefixed workspace head should not be forwarded")
	})

	t.Run("First merge after attach to a non-empty directory with path prefix", func(t *testing.T) {
		// Same `attach --allow-non-empty` semantics as the non-prefix case
		// but the workspace's view is rooted at `look/here/`.
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		rootW := wstd.NewTestWorkspace(t, r.Repository)
		prefixW := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")

		// Populate the repo with files under the prefix.
		mtime := time.Now()
		rootW.Write("look/here/match.txt", "match")
		rootW.Touch("look/here/match.txt", mtime)
		rootW.Write("look/here/modified.txt", "old")
		rootW.Write("look/here/remote-only.txt", "remote")
		_, err := Merge(t.Context(), rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Prefix workspace (head=root) holds the prefix-relative view.
		prefixW.Write("match.txt", "match")
		prefixW.Touch("match.txt", mtime)
		prefixW.Write("modified.txt", "new")
		prefixW.Write("local-only.txt", "local")
		assert.Equal(true, prefixW.Head().IsRoot())

		newHead, err := Merge(t.Context(), prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(newHead, prefixW.Head())

		assert.Equal("match", prefixW.Cat("match.txt"))
		assert.Equal("new", prefixW.Cat("modified.txt"))
		assert.Equal("local", prefixW.Cat("local-only.txt"))
		assert.Equal("remote", prefixW.Cat("remote-only.txt"))

		assert.Equal([]lib.TestRevisionEntryInfo{
			{"look/here/local-only.txt", lib.RevisionEntryKindAdd, 0o600, td.SHA256("local")},
			{"look/here/modified.txt", lib.RevisionEntryKindUpdate, 0o600, td.SHA256("new")},
		}, r.RevisionInfos(newHead))
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
		remoteRev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/d.txt", 0o600, 1, "d"},
		}, r.RevisionSnapshotFileInfos(remoteRev1, nil))

		// Merge first commit into workspace.
		localRev, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal(remoteRev1, localRev)
		assert.Equal(localRev, w2.Head())

		// Add a second commit that adds, updates, and removes files/directories.
		w.Write("a.txt", "aa")
		w.Write("b.txt", "bb")
		w.Rm("c/d.txt")
		w.Write("c/f.txt", "f")
		remoteRev2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
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
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")

		// Force commit local changes.
		opts := ForceCommitOptions{MergeOptions: *wstd.MergeOptions()}
		commitRev, err := ForceCommit(t.Context(), w2.Workspace, r.Repository, &opts)
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
		_, err := Merge(t.Context(), rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Merge into the prefixed workspace.
		_, err = Merge(t.Context(), prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add conflicts inside and outside the path prefix.
		rootW.Write("a.txt", "from root workspace")
		rootW.Write("look/here/b.txt", "bb")
		_, err = Merge(t.Context(), rootW.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		prefixW.Write("b.txt", "from prefixed workspace")

		// Merge should fail.
		_, err = Merge(t.Context(), prefixW.Workspace, r.Repository, wstd.MergeOptions())
		assert.Error(err, "MergeConflictsError")

		// But force commit should succeed.
		commitRev, err := ForceCommit(
			t.Context(),
			prefixW.Workspace,
			r.Repository,
			&ForceCommitOptions{*wstd.MergeOptions()},
		)
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

	t.Run("Cancel", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		opts := wstd.MergeOptions()
		opts.CommitMonitor = newCancelCommitMonitor()

		_, err := Merge(t.Context(), w.Workspace, r.Repository, opts)
		assert.ErrorIs(err, lib.ErrCancel)
	})
}

type cancelCommitMonitor struct {
	TestCommitMonitor
}

func newCancelCommitMonitor() *cancelCommitMonitor {
	return &cancelCommitMonitor{TestCommitMonitor: TestCommitMonitor{}}
}

func (m *cancelCommitMonitor) OnStart(entry *lib.RevisionEntry) error {
	return lib.ErrCancel
}

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

func TestMergeSymlinks(t *testing.T) {
	t.Parallel()

	t.Run("commit and restore symlink", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Symlink("a.txt", "link")
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		assert.Equal("a.txt", w2.ReadLink("link"))
		info, err := w2.Workspace.FS.Stat("link")
		assert.NoError(err)
		assert.Equal(false, info.IsDir())
	})

	t.Run("update symlink target", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Symlink("a.txt", "link")
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w.Rm("link")
		w.Symlink("b.txt", "link")
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal("b.txt", w2.ReadLink("link"))
	})

	t.Run("path prefix skips outside-prefix targets", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		w := wstd.NewTestWorkspace(t, r.Repository)
		w.Write("outside.txt", "x")
		w.Write("look/here/a.txt", "a")
		w.Symlink("../../outside.txt", "look/here/link")
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w2 := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		_, err = w2.Workspace.FS.Stat("link")
		assert.Equal(true, errors.Is(err, fs.ErrNotExist))
		info, err := w2.Workspace.FS.Stat("a.txt")
		assert.NoError(err)
		assert.Equal(false, info.IsDir())

		// A second merge from w2 must not treat the skipped link as a
		// local delete. Otherwise w2 would commit a DELETE entry and the
		// link would silently disappear from the repository for everyone.
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.ErrorIs(err, ErrUpToDate)
	})

	t.Run("path prefix workspace replacing an outside-prefix symlink commits an update", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		w := wstd.NewTestWorkspace(t, r.Repository)
		w.Write("outside.txt", "x")
		w.Write("look/here/a.txt", "a")
		w.Symlink("../../outside.txt", "look/here/link")
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w2 := wstd.NewTestWorkspaceWithPathPrefix(t, r.Repository, "look/here/")
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// The link was silently skipped on restore. Now create a regular
		// file at the same path. The commit must record this as UPDATE,
		// not ADD, because the path already exists in the previous
		// revision (even though w2 couldn't see the original target).
		w2.Write("link", "newfile")
		rev, err := Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		assert.Equal([]lib.TestRevisionEntryInfo{
			{"look/here/link", lib.RevisionEntryKindUpdate, 0o600, td.SHA256("newfile")},
		}, r.RevisionInfos(rev))
	})

	t.Run("local symlink overwritten by file and directory", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("target_file", "tf")
		w.Write("target_dir/inner.txt", "td")
		w.Symlink("target_file", "linkf")
		w.Symlink("target_dir", "linkd")
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		assert.Equal("target_file", w2.ReadLink("linkf"))
		assert.Equal("target_dir", w2.ReadLink("linkd"))

		w.Rm("linkf")
		w.Write("linkf", "now a file")
		w.Rm("linkd")
		w.Write("linkd/inside.txt", "inside")
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		linkfInfo, err := w2.Workspace.FS.Stat("linkf")
		assert.NoError(err)
		assert.Equal(false, linkfInfo.Mode()&fs.ModeSymlink != 0)
		assert.Equal(false, linkfInfo.IsDir())
		assert.Equal("now a file", w2.Cat("linkf"))

		linkdInfo, err := w2.Workspace.FS.Stat("linkd")
		assert.NoError(err)
		assert.Equal(true, linkdInfo.IsDir())
		assert.Equal("inside", w2.Cat("linkd/inside.txt"))
	})

	t.Run("local file overwritten by directory and vice versa", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("file_to_dir", "f")
		w.Write("dir_to_file/inner.txt", "d")
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w.Rm("file_to_dir")
		w.Write("file_to_dir/inside.txt", "now a dir")
		w.Rm("dir_to_file")
		w.Write("dir_to_file", "now a file")
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		info, err := w2.Workspace.FS.Stat("file_to_dir")
		assert.NoError(err)
		assert.Equal(true, info.IsDir())
		assert.Equal("now a dir", w2.Cat("file_to_dir/inside.txt"))

		info, err = w2.Workspace.FS.Stat("dir_to_file")
		assert.NoError(err)
		assert.Equal(false, info.IsDir())
		assert.Equal("now a file", w2.Cat("dir_to_file"))
	})

	t.Run("local file and directory overwritten by symlink", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("target_file", "tf")
		w.Write("target_dir/inner.txt", "td")
		w.Write("realf", "f")
		w.Write("reald/inside.txt", "d")
		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w.Rm("realf")
		w.Symlink("target_file", "realf")
		w.Rm("reald")
		w.Symlink("target_dir", "reald")
		_, err = Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		assert.Equal("target_file", w2.ReadLink("realf"))
		assert.Equal("target_dir", w2.ReadLink("reald"))

		_, err = w2.Workspace.FS.Stat("reald/inside.txt")
		assert.Equal(true, errors.Is(err, fs.ErrNotExist))
	})

	t.Run("symlink mtime is preserved across commit and restore", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		w2 := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Symlink("a.txt", "link")
		linkMtime := time.Unix(1_700_000_000, 123_456_789)
		assert.NoError(w.Workspace.FS.Chmtime("link", linkMtime))

		_, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		_, err = Merge(t.Context(), w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		info, err := w2.Workspace.FS.Stat("link")
		assert.NoError(err)
		assert.Equal(linkMtime.UnixNano(), info.ModTime().UnixNano())
	})
}
