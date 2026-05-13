package lib

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestRevisionEntry(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		test := func(kind RevisionEntryKind) {
			t.Helper()
			var buf bytes.Buffer
			sut := td.RevisionEntry("a.txt", kind)
			err := MarshalRevisionEntry(sut, &buf)
			assert.NoError(err)
			read, err := UnmarshalRevisionEntry(&buf)
			assert.NoError(err)
			assert.Equal(sut, read)
		}
		test(RevisionEntryKindAdd)
		test(RevisionEntryKindUpdate)
		test(RevisionEntryKindDelete)
	})

	t.Run("MarshalledSize is an upper bound", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := td.RevisionEntry("a.txt", RevisionEntryKindAdd)
		err := MarshalRevisionEntry(sut, &buf)
		assert.NoError(err)
		assert.Equal(true, buf.Len() <= RevisionEntryMarshalledSize(sut))

		sut = td.RevisionEntry("a.txt", RevisionEntryKindDelete)
		buf.Reset()
		err = MarshalRevisionEntry(sut, &buf)
		assert.NoError(err)
		assert.Equal(true, buf.Len() <= RevisionEntryMarshalledSize(sut))
	})

	t.Run("RevisionPathCompare", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dirEntry := func(path string) *RevisionEntry {
			t.Helper()
			entry := td.RevisionEntry(path, RevisionEntryKindAdd)
			entry.Metadata = *td.PathMetadata(FileModeDir)
			return entry
		}
		fileEntry := func(path string) *RevisionEntry {
			t.Helper()
			return td.RevisionEntry(path, RevisionEntryKindAdd)
		}
		entries := []*RevisionEntry{
			fileEntry("a.zip"),
			fileEntry("abcd.txt"),
			dirEntry("a"),
			fileEntry("a/1.md"),
			fileEntry("a/2.md"),
			dirEntry("abc"),
			fileEntry("abc/1.md"),
		}
		// Randomize the order of the entries.
		rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
		actual := make([]*RevisionEntry, len(entries))
		copy(actual, entries)
		slices.SortFunc(actual, RevisionEntryPathCompare)
		actualPaths := make([]string, len(actual))
		for i, entry := range actual {
			actualPaths[i] = entry.Path.String()
		}
		assert.Equal([]string{
			"a.zip",
			"abcd.txt",
			"a",
			"a/1.md",
			"a/2.md",
			"abc",
			"abc/1.md",
		}, actualPaths)
	})

	t.Run("RevisionEntryPathCompare with different kinds", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := td.RevisionEntryExt("a", RevisionEntryKindDelete, FileModeDir, "")
		assert.Equal(
			0,
			RevisionEntryPathCompare(sut, td.RevisionEntryExt("a", RevisionEntryKindDelete, FileModeDir, "")),
		)
		assert.Equal(0, RevisionEntryPathCompare(sut, td.RevisionEntryExt("a", RevisionEntryKindAdd, FileModeDir, "")))
		assert.Equal(
			0,
			RevisionEntryPathCompare(sut, td.RevisionEntryExt("a", RevisionEntryKindUpdate, FileModeDir, "")),
		)

		// Files are greater than directories.
		assert.Equal(1, RevisionEntryPathCompare(sut, td.RevisionEntryExt("a", RevisionEntryKindUpdate, 0, "")))
	})
}

func TestRevisionEntryTemp(t *testing.T) {
	t.Parallel()

	t.Run("Sort order is files, directories, and subdirectories", func(t *testing.T) {
		// This basically makes sure that we always use `RevisionEntryPathCompare`.
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		// Use a small chunk size to force rotation.
		sut := NewRevisionEntryTempWriter(fs, 700)

		add := func(path string, mode FileMode) {
			err := sut.Add(
				&RevisionEntry{Kind: RevisionEntryKindAdd, Path: Path{path}, Metadata: *td.PathMetadata(mode)},
			)
			assert.NoError(err)
		}

		add("sub", FileModeDir)
		add("sub/sub", FileModeDir)
		add(".a.txt", 0)
		add("a.txt", 0)
		add("z.txt", 0)
		add("sub/.a.txt", 0)
		add("sub/a.txt", 0)
		add("sub/z.txt", 0)
		add("sub/sub/.a.txt", 0)
		add("sub/sub/a.txt", 0)
		add("sub/sub/z.txt", 0)

		temp, err := sut.Finalize()
		assert.NoError(err)
		assert.Equal(true, sut.chunks > 1, "should be multiple chunks")
		merged := readAllRevsisionTemp(t, temp, nil)
		actualPaths := make([]string, len(merged))
		for i, entry := range merged {
			actualPaths[i] = entry.Path.String()
		}
		assert.Equal([]string{
			".a.txt",
			"a.txt",
			"z.txt",
			"sub",
			"sub/.a.txt",
			"sub/a.txt",
			"sub/z.txt",
			"sub/sub",
			"sub/sub/.a.txt",
			"sub/sub/a.txt",
			"sub/sub/z.txt",
		}, actualPaths)
	})
}
