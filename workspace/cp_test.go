package workspace

import (
	"io/fs"
	"os/user"
	"runtime"
	"strconv"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestCp(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		out := td.NewTestFS(t, td.NewFS(t))
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/1.txt", "c")
		w.Write("c/d/2.txt", "cc")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w.Write("a.txt", "a")
		revId2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Copy all from rev1.
		err = Cp(r.Repository, out.FS, &CpOptions{revId1, wstd.CpMonitor(), nil, true}, td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 1, "c"},
			{"c/d", 0o700 | fs.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "cc"},
		}, out.Ls("."))

		// Trying to copy from rev2 should fail, because files already exist.
		err = Cp(r.Repository, out.FS, &CpOptions{revId2, wstd.CpMonitor(), nil, true}, td.NewFS(t))
		assert.Error(err, "failed to copy")
		assert.Error(err, "exists")
	})

	t.Run("Overwrite", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		out := td.NewTestFS(t, td.NewFS(t))
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "aaa")
		w.Write("b.txt", "b")
		w.Write("c/1.txt", "c")
		w.Write("c/d/2.txt", "cc")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// We make the file smaller to ensure it is truncated before overwriting.
		w.Write("a.txt", "a")
		// Removing a file should not affect anything.
		w.Rm("c/1.txt")
		w.Write("c/3.txt", "ccc")
		w.Chmod("b.txt", 0o777)
		revId2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Copy all from rev1.
		err = Cp(r.Repository, out.FS, &CpOptions{revId1, wstd.CpMonitor(), nil, true}, td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 3, "aaa"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 1, "c"},
			{"c/d", 0o700 | fs.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "cc"},
		}, out.Ls("."))

		// Copy all from the rev2.
		err = Cp(r.Repository, out.FS, &CpOptions{revId2, wstd.CpMonitorOverwrite(), nil, true}, td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o777, 1, "b"},
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 1, "c"},
			{"c/3.txt", 0o600, 3, "ccc"},
			{"c/d", 0o700 | fs.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "cc"},
		}, out.Ls("."))
	})

	t.Run("Chown", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		out := td.NewTestFS(t, td.NewFS(t))
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Create a revision "by hand" that changes the ownership of `a.txt`.
		snapshot := r.RevisionSnapshot(revId1, nil)
		assert.Equal(1, len(snapshot))
		entry := snapshot[0]
		assert.Equal("a.txt", entry.Path.String())
		assert.Equal(lib.ModeAndPerm(0o600), entry.Metadata.ModeAndPerm)

		commit, err := lib.NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		entry.Type = lib.RevisionEntryUpdate
		entry.Metadata.UID = 1234
		entry.Metadata.GID = 5678
		entry.Metadata.ModeAndPerm = 0o700
		err = commit.Add(entry)
		assert.NoError(err)
		revId2, err := commit.Commit(td.CommitInfo())
		assert.NoError(err)

		// Try to copy the file with `Chown` enabled.
		err = Cp(r.Repository, out.FS, &CpOptions{revId2, wstd.CpMonitorOverwrite(), nil, true}, td.NewFS(t))
		assert.Error(err, "failed to restore file owner 1234 and group 5678 for a.txt")

		// Try a second time without `Chown`.
		err = Cp(r.Repository, out.FS, &CpOptions{revId2, wstd.CpMonitorOverwrite(), nil, false}, td.NewFS(t))
		assert.NoError(err)
		assert.Equal(fs.FileMode(0o700).Perm(), out.Stat("a.txt").Mode().Perm())
	})

	t.Run("Parent directory without rx permission should still be created", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		out := td.NewFS(t)
		tout := td.NewTestFS(t, out)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("c/1.txt", "c1")
		w.Chmod("c", 0o500)

		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Copy all from the rev1.
		err = Cp(r.Repository, out, &CpOptions{revId1, wstd.CpMonitor(), nil, true}, td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"c", 0o500 | fs.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 2, "c1"},
		}, tout.Ls("."))
	})

	t.Run("PathFilter", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		out := td.NewTestFS(t, td.NewFS(t))

		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/1.txt", "c1")
		w.Write("c/d/2.txt", "c2")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		filter := lib.NewPathInclusionFilter([]string{"c/**/*"})
		err = Cp(r.Repository, out.FS, &CpOptions{revId1, wstd.CpMonitor(), filter, true}, td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]lib.TestFileInfo{
			{"c", 0o700 | fs.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 2, "c1"},
			{"c/d", 0o700 | fs.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "c2"},
		}, out.Ls("."))
	})

	t.Run("FileMode is restored (as much as possible)", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		out := td.NewTestFS(t, td.NewFS(t))

		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")

		// Set the gid of the file to a different group.
		currentUser, err := user.Current()
		assert.NoError(err)
		groups, err := currentUser.GroupIds()
		assert.NoError(err)
		gid := 0
		for _, g := range groups {
			if g == currentUser.Gid {
				continue
			}
			gi, err := strconv.Atoi(g)
			assert.NoError(err)
			gid = gi
		}
		assert.NotEqual(0, gid, "current user has no additional groups")
		w.Chown("a.txt", -1, gid)

		// Set `setuid`, `setgid`, and `sticky`.
		w.Chmod("a.txt", 0o777|fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky)

		// Commit.
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Copy all from the rev1.
		err = Cp(r.Repository, out.FS, &CpOptions{revId1, wstd.CpMonitor(), nil, true}, td.NewFS(t))
		assert.NoError(err)

		stat := w.Stat("a.txt")
		cpStat := out.Stat("a.txt")
		// We do not preserve setuid, setgid, and sticky.
		assert.Equal(stat.Mode()&fs.ModePerm, cpStat.Mode())
		assert.Equal(stat.Size(), cpStat.Size())
		assert.Equal(stat.ModTime(), cpStat.ModTime())
		// Verify extended metadata if supported. We use `EnhanceMetadata` to test this,
		// because it is provides platform specific implementations.
		md := lib.FileMetadata{GID: lib.UIDUnset, UID: lib.UIDUnset} //nolint:exhaustruct
		lib.EnhanceMetadata(&md, stat)
		if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
			assert.NotEqual(lib.UIDUnset, md.UID, "uid should be set on darwin and linux")
			assert.NotEqual(lib.UIDUnset, md.GID, "gid should be set on darwin and linux")
		}
		cpMd := lib.FileMetadata{GID: lib.UIDUnset, UID: lib.UIDUnset} //nolint:exhaustruct
		lib.EnhanceMetadata(&cpMd, cpStat)
		// We don't want to compare `BirthtimeSec` and `BirthtimeNSec` because `Birthtime` is not
		// restored and cannot be restored.
		md.BirthtimeSec = 0
		md.BirthtimeNSec = 0
		cpMd.BirthtimeSec = 0
		cpMd.BirthtimeNSec = 0
		assert.Equal(md, cpMd)
	})
}
