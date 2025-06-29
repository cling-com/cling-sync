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
		rt := NewRepositoryTest(t)

		// Empty workspace.
		status, err := Status(rt.Workspace, rt.Repository, fakeStatusOptions(), t.TempDir())
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Add files and directories.
		rt.AddLocal("a.txt", ".")
		rt.AddLocal("b.txt", "..")
		rt.AddLocal("c/1.txt", "...")
		rt.AddLocal("c/d/2.txt", "....")

		// "Dirty" workspace.
		status, err = Status(rt.Workspace, rt.Repository, fakeStatusOptions(), t.TempDir())
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
		_, err = Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		status, err = Status(rt.Workspace, rt.Repository, fakeStatusOptions(), t.TempDir())
		assert.NoError(err)
		assert.Equal(0, len(status))
		assert.NoError(err)

		// Add, remove, and update files.
		rt.RemoveLocal("b.txt")
		rt.AddLocal("e.txt", ".....")
		rt.UpdateLocalMTime("c/1.txt", time.Now())

		// "Dirty" workspace.
		status, err = Status(rt.Workspace, rt.Repository, fakeStatusOptions(), t.TempDir())
		assert.NoError(err)
		assert.Equal([]string{
			"D b.txt",
			"A e.txt",
			"M c/1.txt",
		}, statusFilesString(status))
	})

	t.Run("Removing a directory", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)

		// Empty workspace.
		status, err := Status(rt.Workspace, rt.Repository, fakeStatusOptions(), t.TempDir())
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Add files and directories.
		rt.AddLocal("a.txt", ".")
		rt.AddLocal("b.txt", "..")
		rt.AddLocal("c/1.txt", "...")
		rt.AddLocal("c/d/2.txt", "....")

		// Commit, workspace should be "clean".
		_, err = Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		status, err = Status(rt.Workspace, rt.Repository, fakeStatusOptions(), t.TempDir())
		assert.NoError(err)
		assert.Equal(0, len(status))

		// Remove directory.
		rt.RemoveLocal("c")

		// "Dirty" workspace.
		status, err = Status(rt.Workspace, rt.Repository, fakeStatusOptions(), t.TempDir())
		assert.NoError(err)
		assert.Equal([]string{
			"D c/",
			"D c/1.txt",
			"D c/d/",
			"D c/d/2.txt",
		}, statusFilesString(status))

		// Commit, workspace should be "clean" again.
		_, err = Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		status, err = Status(rt.Workspace, rt.Repository, fakeStatusOptions(), t.TempDir())
		assert.NoError(err)
		assert.Equal(0, len(status))
	})
}

func statusFilesString(files []StatusFile) []string {
	s := []string{}
	for _, file := range files {
		s = append(s, file.Format())
	}
	return s
}

func fakeStatusOptions() *StatusOptions {
	return &StatusOptions{nil, NewTestStagingMonitor()}
}
