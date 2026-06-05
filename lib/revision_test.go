package lib

import (
	"testing"
)

func TestNewRevisionIdFromString(t *testing.T) {
	t.Parallel()
	assert := NewAssert(t)
	id := RevisionId{0xab, 0xcd}
	parsed, err := NewRevisionIdFromString(id.String())
	assert.NoError(err)
	assert.Equal(id, parsed)

	t.Run("Malformed revision id should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewRevisionIdFromString("not-hex")
		assert.Error(err, "invalid revision id")
		_, err = NewRevisionIdFromString("abcd") // valid hex but too short
		assert.Error(err, "invalid revision id")
		_, err = NewRevisionIdFromString("")
		assert.Error(err, "invalid revision id")
	})
}

func TestRevisionRange(t *testing.T) {
	t.Parallel()
	a := RevisionId{0xaa}
	b := RevisionId{0xbb}

	t.Run("NewRevisionRangeFromString and String round-trip", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		cases := []struct {
			in   string
			want RevisionRange
		}{
			{"", RevisionRange{nil, nil}},
			{a.String(), RevisionRange{nil, &a}},
			{a.String() + ".." + b.String(), RevisionRange{&a, &b}},
			{a.String() + "..", RevisionRange{&a, nil}},
			{".." + b.String(), RevisionRange{nil, &b}},
		}
		for _, c := range cases {
			r, err := NewRevisionRangeFromString(c.in)
			assert.NoError(err)
			assert.Equal(c.want, r, c.in)
		}
		// String renders the canonical form, which NewRevisionRangeFromString accepts again.
		assert.Equal("", RevisionRange{nil, nil}.String())
		assert.Equal(b.String(), RevisionRange{nil, &b}.String())
		assert.Equal(a.String()+".."+b.String(), RevisionRange{&a, &b}.String())
		assert.Equal(a.String()+"..", RevisionRange{&a, nil}.String())
	})

	t.Run("Malformed revision id should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewRevisionRangeFromString("not-hex")
		assert.Error(err, "invalid range")
		_, err = NewRevisionRangeFromString("dead..beef")
		assert.Error(err, "invalid range")
		_, err = NewRevisionRangeFromString(a.String() + "..nothex")
		assert.Error(err, "invalid range")
	})

	t.Run("IsInChain", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		chain := RevisionChain{a}
		assert.Equal(true, a.IsInChain(chain))
		assert.Equal(false, b.IsInChain(chain))
		assert.Equal(true, RevisionRange{nil, nil}.IsInChain(chain), "nil bounds are always valid")
		assert.Equal(true, RevisionRange{nil, &a}.IsInChain(chain))
		assert.Equal(false, RevisionRange{nil, &b}.IsInChain(chain), "Until not in chain")
		assert.Equal(false, RevisionRange{&b, &a}.IsInChain(chain), "Since not in chain")
	})
}

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
