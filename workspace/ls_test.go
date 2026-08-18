package workspace

import (
	"testing"

	"github.com/cling-com/cling-sync/lib"
)

func TestLs(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Empty workspace.
		ls, err := Ls(t.Context(), r.Repository, td.NewFS(t), wstd.LsOptions(r.Head()))
		assert.NoError(err)
		assert.Equal(0, len(ls))

		// Add a commit.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/1.txt", "c1")
		w.Write("c/d/2.txt", "cd2")
		rev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add a second commit.
		w.Write("c/d/3.txt", "cd3")
		w.Write("a.txt", "aa")
		w.Rm("b.txt")
		rev2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		ls, err = Ls(t.Context(), r.Repository, td.NewFS(t), wstd.LsOptions(rev1))
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"a.txt", 0o600, 1},
			{"b.txt", 0o600, 1},
			{"c", 0o700 | lib.FileModeDir, 0},
			{"c/1.txt", 0o600, 2},
			{"c/d", 0o700 | lib.FileModeDir, 0},
			{"c/d/2.txt", 0o600, 3},
		}, lsFiles(ls))

		ls, err = Ls(t.Context(), r.Repository, td.NewFS(t), wstd.LsOptions(rev2))
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"a.txt", 0o600, 2},
			{"c", 0o700 | lib.FileModeDir, 0},
			{"c/1.txt", 0o600, 2},
			{"c/d", 0o700 | lib.FileModeDir, 0},
			{"c/d/2.txt", 0o600, 3},
			{"c/d/3.txt", 0o600, 3},
		}, lsFiles(ls))
	})

	t.Run("A directory is listed directly before its contents", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("sub.txt", "s")
		w.Write("sub/a.txt", "a")
		rev, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		ls, err := Ls(t.Context(), r.Repository, td.NewFS(t), wstd.LsOptions(rev))
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"sub.txt", 0o600, 1},
			{"sub", 0o700 | lib.FileModeDir, 0},
			{"sub/a.txt", 0o600, 1},
		}, lsFiles(ls))
	})

	t.Run("Include", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add a commit.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/1.txt", "c1")
		w.Write("c/d/2.txt", "cd2")
		rev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		filter := lib.NewPathInclusionFilter([]string{"c"})
		ls, err := Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), filter, nil, lib.Path{}, 0},
		)
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"c", 0o700 | lib.FileModeDir, 0},
			{"c/1.txt", 0o600, 2},
			{"c/d", 0o700 | lib.FileModeDir, 0},
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
		rev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		prefix, err := lib.NewPath("b")
		assert.NoError(err)
		ls, err := Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), nil, nil, prefix, 0},
		)
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"b1.txt", 0o600, 2},
			{"b2.txt", 0o600, 2},
		}, lsFiles(ls))
	})

	t.Run("PathPrefix scopes the pattern to the prefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("A/B/1.txt", "x1")
		w.Write("A/B/2.txt", "x2")
		w.Write("A/C/3.txt", "x3")
		w.Write("A/top.txt", "t")
		rev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// The pattern is relative to the prefix: `B/*` under prefix `A` matches
		// `A/B/*`, not the literal repository path `B/*`.
		prefixA, err := lib.NewPath("A")
		assert.NoError(err)
		ls, err := Ls(t.Context(), r.Repository, td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), lib.NewPathInclusionFilter([]string{"B/*"}), nil, prefixA, 0})
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"B/1.txt", 0o600, 2},
			{"B/2.txt", 0o600, 2},
		}, lsFiles(ls))

		// Pushing the prefix all the way to `A/B` and matching `*` lists the subtree.
		prefixAB, err := lib.NewPath("A/B")
		assert.NoError(err)
		ls, err = Ls(t.Context(), r.Repository, td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), lib.NewPathInclusionFilter([]string{"*"}), nil, prefixAB, 0})
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"1.txt", 0o600, 2},
			{"2.txt", 0o600, 2},
		}, lsFiles(ls))
	})

	t.Run("Depth", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Write("b/b1.txt", "b1")
		w.Write("b/c/c1.txt", "c1")
		w.Write("b/c/d/d1.txt", "d1")
		rev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		ls, err := Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), nil, nil, lib.Path{}, 1},
		)
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"a.txt", 0o600, 1},
			{"b", 0o700 | lib.FileModeDir, 0},
		}, lsFiles(ls))

		ls, err = Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), nil, nil, lib.Path{}, 2},
		)
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"a.txt", 0o600, 1},
			{"b", 0o700 | lib.FileModeDir, 0},
			{"b/b1.txt", 0o600, 2},
			{"b/c", 0o700 | lib.FileModeDir, 0},
		}, lsFiles(ls))

		// 0 means unlimited.
		ls, err = Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), nil, nil, lib.Path{}, 0},
		)
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"a.txt", 0o600, 1},
			{"b", 0o700 | lib.FileModeDir, 0},
			{"b/b1.txt", 0o600, 2},
			{"b/c", 0o700 | lib.FileModeDir, 0},
			{"b/c/c1.txt", 0o600, 2},
			{"b/c/d", 0o700 | lib.FileModeDir, 0},
			{"b/c/d/d1.txt", 0o600, 2},
		}, lsFiles(ls))

		// A depth beyond the deepest path lists everything.
		ls, err = Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), nil, nil, lib.Path{}, 100},
		)
		assert.NoError(err)
		assert.Equal(7, len(ls))
	})

	t.Run("Depth counts from the path prefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Write("b/b1.txt", "b1")
		w.Write("b/c/c1.txt", "c1")
		w.Write("b/c/d/d1.txt", "d1")
		rev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		prefix, err := lib.NewPath("b")
		assert.NoError(err)
		ls, err := Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), nil, nil, prefix, 1},
		)
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"b1.txt", 0o600, 2},
			{"c", 0o700 | lib.FileModeDir, 0},
		}, lsFiles(ls))
	})

	t.Run("Depth and pattern", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Write("b/b1.txt", "b1")
		w.Write("b/c/c1.txt", "c1")
		rev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		filter := lib.NewPathInclusionFilter([]string{"b"})
		ls, err := Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), filter, nil, lib.Path{}, 2},
		)
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"b", 0o700 | lib.FileModeDir, 0},
			{"b/b1.txt", 0o600, 2},
			{"b/c", 0o700 | lib.FileModeDir, 0},
		}, lsFiles(ls))
	})

	t.Run(".clingignore does not affect existing revisions", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add a commit.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c.md", "c")
		rev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		ls, err := Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), nil, nil, lib.Path{}, 0},
		)
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"a.txt", 0o600, 1},
			{"b.txt", 0o600, 1},
			{"c.md", 0o600, 1},
		}, lsFiles(ls))

		// Adding a .clingignore file should not affect existing revisions.
		w.Write(".clingignore", "*.md")
		ls, err = Ls(
			t.Context(),
			r.Repository,
			td.NewFS(t),
			&LsOptions{rev1, wstd.SnapshotMonitor(), nil, nil, lib.Path{}, 0},
		)
		assert.NoError(err)
		assert.Equal([]lsFileInfo{
			{"a.txt", 0o600, 1},
			{"b.txt", 0o600, 1},
			{"c.md", 0o600, 1},
		}, lsFiles(ls))
	})
}

type lsFileInfo struct {
	Path string
	Mode lib.FileMode
	Size int
}

func lsFiles(f []LsFile) []lsFileInfo {
	result := make([]lsFileInfo, len(f))
	for i, file := range f {
		result[i] = lsFileInfo{
			Path: file.Path.String(),
			Mode: file.Metadata.FileMode,
			Size: int(file.Metadata.Size),
		}
	}
	return result
}
