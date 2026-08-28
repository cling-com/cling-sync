package lib

import (
	"errors"
	"io"
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
			// A bare revision is that revision alone: the range starts at its
			// parent, or at the root when it is the oldest revision.
			{b.String(), RevisionRange{&a, &b}},
			{"head", RevisionRange{&a, &b}},
			{a.String(), RevisionRange{nil, &a}},
			{"head~1", RevisionRange{nil, &a}},
			{a.String() + ".." + b.String(), RevisionRange{&a, &b}},
			{a.String() + "..", RevisionRange{&a, nil}},
			{".." + b.String(), RevisionRange{nil, &b}},
			{"..head", RevisionRange{nil, &b}},
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

func TestRevisionReader(t *testing.T) {
	t.Parallel()
	t.Run("Unordered entries should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// This is the sort order used prior to 2026-09, where a directory's
		// files came before its subdirectories.
		// `AddRevision` sorts internally, so we have to write the block by hand.
		chunk := RevisionEntryChunk{Entries: []*RevisionEntry{
			td.RevisionEntry("a/z.txt", RevisionEntryKindAdd),
			td.RevisionEntryExt("a/b", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntry("a/b/c.txt", RevisionEntryKindAdd),
		}}
		buf := make([]byte, chunk.MarshallSize())
		w := NewProtobufWriter(buf)
		assert.NoError(chunk.Marshall(w))
		blockId, _, err := r.WriteBlock(t.Context(), w.Bytes(), NewBlockBuf(), WriteBlockOpts{})
		assert.NoError(err)
		revisionId, err := r.WriteRevision(t.Context(), &Revision{ //nolint:exhaustruct
			Timestamp:        NewTimestampNow(),
			ParentRevisionId: RevisionId{},
			BlockIds:         []BlockId{blockId},
		})
		assert.NoError(err)

		revision, err := r.ReadRevision(t.Context(), revisionId, NewBlockBuf())
		assert.NoError(err)
		reader := NewRevisionReader(r.Repository, &revision)
		readBuf := NewBlockBuf()
		var readErr error
		for {
			_, err := reader.Read(t.Context(), readBuf)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				readErr = err
				break
			}
		}
		assert.Error(readErr, "not strictly sorted")
		assert.Error(readErr, "a/z.txt >= a/b/")
	})

	t.Run("Entry blocks should be read with the revision hint", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		revisionId := r.AddRevision(RevisionId{}, []*RevisionEntry{td.RevisionEntry("a.txt", RevisionEntryKindAdd)})
		revision, err := r.ReadRevision(t.Context(), revisionId, NewBlockBuf())
		assert.NoError(err)
		reader := NewRevisionReader(r.Repository, &revision)
		r.StorageMonitor.Reset()
		_, err = reader.Read(t.Context(), NewBlockBuf())
		assert.NoError(err)
		assert.Equal([]string{"ReadBlock(revision)"}, r.StorageMonitor.DistinctBlockOps())
	})
}

func TestDisplayOrderReader(t *testing.T) {
	t.Parallel()

	file := func(path string) *RevisionEntry {
		return td.RevisionEntry(path, RevisionEntryKindAdd)
	}
	dir := func(path string) *RevisionEntry {
		return td.RevisionEntryExt(path, RevisionEntryKindAdd, FileModeDir, "")
	}
	source := func(entries []*RevisionEntry) func(BlockBuf) (*RevisionEntry, error) {
		i := 0
		return func(BlockBuf) (*RevisionEntry, error) {
			if i == len(entries) {
				return nil, io.EOF
			}
			i++
			return entries[i-1], nil
		}
	}
	readAll := func(assert Assert, entries []*RevisionEntry) []string {
		sut := NewDisplayOrderReader(source(entries))
		buf := NewBlockBuf()
		got := []string{}
		for {
			entry, err := sut.Read(buf)
			if errors.Is(err, io.EOF) {
				return got
			}
			assert.NoError(err)
			path := entry.Path.String()
			if entry.Metadata.FileMode.IsDir() {
				path += PathDelim
			}
			got = append(got, path)
		}
	}

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		entries := []*RevisionEntry{
			dir("sub"),
			file("sub.txt"),
			file("sub.txt.bak"),
			file("sub/a.txt"),
			file("sub/z.txt"),
			file("t.txt"),
		}
		got := readAll(assert, entries)
		assert.Equal(
			[]string{"sub.txt", "sub.txt.bak", "sub/", "sub/a.txt", "sub/z.txt", "t.txt"},
			got)
	})

	t.Run("Nested directories", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		entries := []*RevisionEntry{
			dir("a"),
			dir("a.d"),
			file("a.d.txt"),
			file("a.d/x.txt"),
			file("a/y.txt"),
		}
		got := readAll(assert, entries)
		assert.Equal([]string{"a.d.txt", "a.d/", "a.d/x.txt", "a/", "a/y.txt"}, got)
	})

	t.Run("Same path as a file and directory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		entries := []*RevisionEntry{
			file("a"),
			dir("a"),
			file("a/b.txt"),
		}
		got := readAll(assert, entries)
		assert.Equal([]string{"a", "a/", "a/b.txt"}, got)
	})

	t.Run("Empty directory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		assert.Equal([]string{"a/"}, readAll(assert, []*RevisionEntry{dir("a")}))
		assert.Equal([]string{"a.txt", "a/"}, readAll(assert, []*RevisionEntry{dir("a"), file("a.txt")}))
		assert.Equal([]string{"a/", "b.txt"}, readAll(assert, []*RevisionEntry{dir("a"), file("b.txt")}))
	})

	t.Run("Nested empty directories", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		entries := []*RevisionEntry{
			dir("a"),
			dir("a.d"),
		}
		got := readAll(assert, entries)
		assert.Equal([]string{"a.d/", "a/"}, got)
	})

	t.Run("Source error is passed on", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := NewDisplayOrderReader(func(BlockBuf) (*RevisionEntry, error) {
			return nil, Errorf("block is gone")
		})
		_, err := sut.Read(NewBlockBuf())
		assert.Error(err, "block is gone")
	})

	t.Run("Too many nested directories should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		// `a`, `a.`, `a..` and so on, each name containing the one before it.
		entries := []*RevisionEntry{}
		size := 0
		for path := "a"; size <= maxDisplayOrderHoldBack; path += "." {
			entry := dir(path)
			size += entry.MarshallSize()
			entries = append(entries, entry)
		}
		atLimit := entries[:len(entries)-1]
		assert.Equal(len(atLimit), len(readAll(assert, atLimit)), "one entry short of the limit still fits")
		sut := NewDisplayOrderReader(source(entries))
		var err error
		for err == nil {
			_, err = sut.Read(NewBlockBuf())
		}
		assert.Error(err, "too many nested directories to list")
		assert.Error(err, "starting at "+entries[0].Path.String())
	})
}
