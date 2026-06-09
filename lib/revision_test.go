package lib

import (
	"testing"
)

func TestParseRevisionId(t *testing.T) {
	t.Parallel()
	a := RevisionId{0xaa}
	b := RevisionId{0xbb}
	c := RevisionId{0xcc}
	chain := RevisionChain{c, b, a} // head first: c is head, b is head~1, a is head~2.

	t.Run("head and ids resolve, ~n walks toward the root", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		for spec, want := range map[string]RevisionId{
			"head":            c,
			"HEAD":            c,
			"head~0":          c,
			"head~1":          b,
			"head~2":          a,
			"head~":           b,
			b.String():        b,
			c.String() + "~2": a,
			b.String() + "~1": a,
		} {
			got, err := chain.ParseRevisionId(spec)
			assert.NoError(err, spec)
			assert.Equal(want, got, spec)
		}
	})

	t.Run("Out-of-range and malformed specs should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := chain.ParseRevisionId("head~3") // only three revisions
		assert.Error(err, "older than the oldest")
		_, err = chain.ParseRevisionId(RevisionId{0xff}.String()) // valid hex, not in chain
		assert.Error(err, "revision not found")
		_, err = chain.ParseRevisionId("not-hex")
		assert.Error(err, "invalid revision id")
		_, err = chain.ParseRevisionId("head~-1")
		assert.Error(err, "non-negative")
		_, err = chain.ParseRevisionId("head~x")
		assert.Error(err, "non-negative")
	})

	t.Run("head on an empty chain is the root", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		got, err := RevisionChain{}.ParseRevisionId("head")
		assert.NoError(err)
		assert.Equal(true, got.IsRoot())
		_, err = RevisionChain{}.ParseRevisionId("head~1")
		assert.Error(err, "older than the oldest")
	})
}

func TestRevisionRange(t *testing.T) {
	t.Parallel()
	a := RevisionId{0xaa}
	b := RevisionId{0xbb}
	chain := RevisionChain{b, a} // head first: b is head, a is head~1.

	t.Run("ParseRevisionRange and String round-trip", func(t *testing.T) {
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
			{"head~1..head", RevisionRange{&a, &b}}, // git-style bounds resolve
		}
		for _, c := range cases {
			r, err := chain.ParseRevisionRange(c.in)
			assert.NoError(err, c.in)
			assert.Equal(c.want, r, c.in)
		}
		// String renders the canonical form, which ParseRevisionRange accepts again.
		assert.Equal("", RevisionRange{nil, nil}.String())
		assert.Equal(b.String(), RevisionRange{nil, &b}.String())
		assert.Equal(a.String()+".."+b.String(), RevisionRange{&a, &b}.String())
		assert.Equal(a.String()+"..", RevisionRange{&a, nil}.String())
	})

	t.Run("Malformed or unknown bounds should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := chain.ParseRevisionRange("not-hex")
		assert.Error(err, "invalid range")
		_, err = chain.ParseRevisionRange(RevisionId{0xcc}.String()) // valid hex, not in chain
		assert.Error(err, "invalid range")
		_, err = chain.ParseRevisionRange(a.String() + "..nothex")
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
