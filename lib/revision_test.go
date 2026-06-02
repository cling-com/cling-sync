package lib

import (
	"testing"
)

func TestReadRevisionChain(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo := td.NewTestRepository(t, td.NewFS(t))

		entry1, _ := testEntry(t, repo, "a.txt", "abc")
		rev1, err := testCommit(t, repo.Repository, entry1)
		assert.NoError(err)
		entry2, _ := testEntry(t, repo, "b.txt", "def")
		rev2, err := testCommit(t, repo.Repository, entry2)
		assert.NoError(err)
		entry3, _ := testEntry(t, repo, "c.txt", "ghi")
		rev3, err := testCommit(t, repo.Repository, entry3)
		assert.NoError(err)

		chain, err := ReadRevisionChain(t.Context(), repo.Repository)
		assert.NoError(err)
		assert.Equal(RevisionChain{rev3, rev2, rev1}, chain)
	})

	t.Run("Empty repository returns empty chain", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo := td.NewTestRepository(t, td.NewFS(t))

		chain, err := ReadRevisionChain(t.Context(), repo.Repository)
		assert.NoError(err)
		assert.Equal(RevisionChain{}, chain)
	})
}
