package workspace

import (
	"bytes"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestCat(t *testing.T) {
	t.Parallel()

	setupCat := func(t *testing.T) (func(lib.RevisionId, string) (string, error), lib.RevisionId, lib.RevisionId) {
		t.Helper()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/1.txt", "c1")
		w.Symlink("a.txt", "link")
		rev1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		w.Write("a.txt", "aa")
		w.Rm("b.txt")
		rev2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		cat := func(rev lib.RevisionId, path string) (string, error) {
			p, err := lib.NewPath(path)
			assert.NoError(err)
			var buf bytes.Buffer
			err = Cat(t.Context(), r.Repository, &buf, &CatOptions{RevisionId: rev, Path: p}, td.NewFS(t))
			return buf.String(), err
		}
		return cat, rev1, rev2
	}

	t.Run("Reads a file at HEAD", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cat, _, rev2 := setupCat(t)
		got, err := cat(rev2, "a.txt")
		assert.NoError(err)
		assert.Equal("aa", got)
	})

	t.Run("Reads a nested file", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cat, _, rev2 := setupCat(t)
		got, err := cat(rev2, "c/1.txt")
		assert.NoError(err)
		assert.Equal("c1", got)
	})

	t.Run("Reads a file from an older revision", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cat, rev1, _ := setupCat(t)
		got, err := cat(rev1, "a.txt")
		assert.NoError(err)
		assert.Equal("a", got)
	})

	t.Run("Reads a file deleted at HEAD from an older revision", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cat, rev1, _ := setupCat(t)
		got, err := cat(rev1, "b.txt")
		assert.NoError(err)
		assert.Equal("b", got)
	})

	t.Run("A file absent from the revision should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cat, _, rev2 := setupCat(t)
		_, err := cat(rev2, "b.txt")
		assert.Error(err, "file not found")
	})

	t.Run("A directory should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cat, _, rev2 := setupCat(t)
		_, err := cat(rev2, "c")
		assert.Error(err, "is a directory")
	})

	t.Run("A symlink should fail naming its target", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cat, _, rev2 := setupCat(t)
		_, err := cat(rev2, "link")
		assert.Error(err, "link is a symlink to a.txt")
	})
}
