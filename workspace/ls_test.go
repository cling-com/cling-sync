package workspace

import (
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestLs(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Empty workspace.
		ls, err := Ls(r.Repository, td.NewFS(t), wstd.LsOptions(r.Head()))
		assert.NoError(err)
		assert.Equal(0, len(ls))

		// Add a commit.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/1.txt", "c1")
		w.Write("c/d/2.txt", "cd2")
		rev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add a second commit.
		w.Write("c/d/3.txt", "cd3")
		w.Write("a.txt", "aa")
		w.Rm("b.txt")
		rev2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		ls, err = Ls(r.Repository, td.NewFS(t), wstd.LsOptions(rev1))
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"a.txt", 0o600, 1},
			{"b.txt", 0o600, 1},
			{"c", 0o700 | lib.ModeDir, 0},
			{"c/1.txt", 0o600, 2},
			{"c/d", 0o700 | lib.ModeDir, 0},
			{"c/d/2.txt", 0o600, 3},
		}, lsFiles(ls))

		ls, err = Ls(r.Repository, td.NewFS(t), wstd.LsOptions(rev2))
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"a.txt", 0o600, 2},
			{"c", 0o700 | lib.ModeDir, 0},
			{"c/1.txt", 0o600, 2},
			{"c/d", 0o700 | lib.ModeDir, 0},
			{"c/d/2.txt", 0o600, 3},
			{"c/d/3.txt", 0o600, 3},
		}, lsFiles(ls))
	})

	t.Run("PathFilter", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add a commit.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/1.txt", "c1")
		w.Write("c/d/2.txt", "cd2")
		rev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		filter, err := lib.NewPathInclusionFilter([]string{"c"})
		assert.NoError(err)
		ls, err := Ls(r.Repository, td.NewFS(t), &LsOptions{rev1, filter, lib.Path{}})
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"c", 0o700 | lib.ModeDir, 0},
			{"c/1.txt", 0o600, 2},
			{"c/d", 0o700 | lib.ModeDir, 0},
			{"c/d/2.txt", 0o600, 3},
		}, lsFiles(ls))
	})

	t.Run("PathPrefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add a commit.
		w.Write("a.txt", "a")
		w.Write("bb", "b")
		w.Write("b/b1.txt", "b1")
		w.Write("b/b2.txt", "b2")
		w.Write("c/c1.txt", "c1")
		rev1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		prefix, err := lib.NewPath("b")
		assert.NoError(err)
		ls, err := Ls(r.Repository, td.NewFS(t), &LsOptions{rev1, nil, prefix})
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"b1.txt", 0o600, 2},
			{"b2.txt", 0o600, 2},
		}, lsFiles(ls))
	})
}

type lsFileInfo struct {
	Path string
	Mode lib.ModeAndPerm
	Size int
}

func lsFiles(f []LsFile) []lsFileInfo {
	result := []lsFileInfo{}
	for _, file := range f {
		result = append(result, lsFileInfo{
			Path: file.Path.String(),
			Mode: file.Metadata.ModeAndPerm,
			Size: int(file.Metadata.Size),
		})
	}
	return result
}
