package workspace

import (
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

func TestStatus(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Empty workspace.
		status, err := Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Add files and directories.
		w.Write("a.txt", ".")
		w.Write("b.txt", "..")
		w.Write("c/1.txt", "...")
		w.Write("c/d/2.txt", "....")

		// "Dirty" workspace.
		status, err = Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{
			"A a.txt",
			"A b.txt",
			"A c/",
			"A c/1.txt",
			"A c/d/",
			"A c/d/2.txt",
		}, statusFilesString(status))

		// Commit, workspace should be "clean" again.
		_, err = Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		status, err = Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal(0, len(status))
		assert.NoError(err)

		// Add, remove, and update files.
		w.Rm("b.txt")
		w.Write("e.txt", ".....")
		w.Touch("c/1.txt", time.Now())

		// "Dirty" workspace.
		status, err = Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{
			"D b.txt",
			"A e.txt",
			"M c/1.txt",
		}, statusFilesString(status))
	})

	t.Run("Status always runs against the workspace head", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")

		// Commit, workspace should be "clean".
		_, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		status, err := Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Create a second workspace tied to the same repository.
		// Status should be clean, because the workspace points to the initial (empty) revision.
		w2 := wstd.NewTestWorkspace(t, r.Repository)
		status, err = Status(w2.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Merge repository into the second workspace.
		_, err = Merge(w2.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		status, err = Status(w2.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Add a commit in the first workspace.
		w.Write("b.txt", "b")
		_, err = Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		status, err = Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Add a file in the second workspace.
		// The status should reflect the change in the second workspace relative to the workspace
		// head and not the repository head.
		w2.Write("c.txt", "c")
		status, err = Status(w2.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{
			"A c.txt",
		}, statusFilesString(status))
	})

	t.Run("Removing a directory", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Empty workspace.
		status, err := Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Add files and directories.
		w.Write("a.txt", ".")
		w.Write("b.txt", "..")
		w.Write("c/1.txt", "...")
		w.Write("c/d/2.txt", "....")

		// Commit, workspace should be "clean".
		_, err = Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		status, err = Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Remove directory.
		w.Rm("c")

		// "Dirty" workspace.
		status, err = Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{
			"D c/",
			"D c/1.txt",
			"D c/d/",
			"D c/d/2.txt",
		}, statusFilesString(status))

		// Commit, workspace should be "clean" again.
		_, err = Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		status, err = Status(w.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal(0, len(status))
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

		// Run `status` with no changes.
		status, err := Status(prefixW.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{}, statusFilesString(status))

		// Add a file and run `status`.
		prefixW.Write("new.txt", "new")
		status, err = Status(prefixW.Workspace, r.Repository, wstd.StatusOptions(), td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{
			"A new.txt",
		}, statusFilesString(status))
	})

	t.Run("File ownership can be ignored", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		// out := td.NewTestFS(t, td.NewFS(t))
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

		// Status taking ownership into account.
		opts := wstd.StatusOptions()
		opts.Chown = true
		status, err := Status(w2.Workspace, r.Repository, opts, td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{
			"M a.txt",
		}, statusFilesString(status))

		// Status not taking ownership into account.
		opts.Chown = false
		status, err = Status(w2.Workspace, r.Repository, opts, td.NewFS(t))
		assert.NoError(err)
		assert.Equal([]string{}, statusFilesString(status))
	})
}

func statusFilesString(files []StatusFile) []string {
	s := []string{}
	for _, file := range files {
		s = append(s, file.Format())
	}
	return s
}
