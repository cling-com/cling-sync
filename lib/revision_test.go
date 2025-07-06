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
		test := func(entryType RevisionEntryType) {
			t.Helper()
			var buf bytes.Buffer
			sut := td.RevisionEntry("a.txt", entryType)
			err := MarshalRevisionEntry(sut, &buf)
			assert.NoError(err)
			read, err := UnmarshalRevisionEntry(&buf)
			assert.NoError(err)
			assert.Equal(sut, read)
		}
		test(RevisionEntryAdd)
		test(RevisionEntryUpdate)
		test(RevisionEntryDelete)
	})

	t.Run("MarshalledSize", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := td.RevisionEntry("a.txt", RevisionEntryAdd)
		err := MarshalRevisionEntry(sut, &buf)
		assert.NoError(err)
		assert.Equal(sut.MarshalledSize(), buf.Len())

		sut = td.RevisionEntry("a.txt", RevisionEntryDelete)
		buf.Reset()
		err = MarshalRevisionEntry(sut, &buf)
		assert.NoError(err)
		assert.Equal(sut.MarshalledSize(), buf.Len())
	})

	t.Run("RevisionPathCompare", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dirEntry := func(path string) *RevisionEntry {
			t.Helper()
			entry := td.RevisionEntry(path, RevisionEntryAdd)
			entry.Metadata = td.FileMetadata(ModeDir)
			return entry
		}
		fileEntry := func(path string) *RevisionEntry {
			t.Helper()
			return td.RevisionEntry(path, RevisionEntryAdd)
		}
		entries := []*RevisionEntry{
			fileEntry("/a.zip"),
			fileEntry("/abcd.txt"),
			dirEntry("/a"),
			fileEntry("/a/1.md"),
			fileEntry("/a/2.md"),
			dirEntry("/abc"),
			fileEntry("/abc/1.md"),
		}
		// Randomize the order of the entries.
		rand.Shuffle(len(entries), func(i, j int) { entries[i], entries[j] = entries[j], entries[i] })
		actual := make([]*RevisionEntry, len(entries))
		copy(actual, entries)
		slices.SortFunc(actual, RevisionEntryPathCompare)
		actualPaths := make([]string, len(actual))
		for i, entry := range actual {
			actualPaths[i] = string(entry.Path)
		}
		assert.Equal([]string{
			"/a.zip",
			"/abcd.txt",
			"/a",
			"/a/1.md",
			"/a/2.md",
			"/abc",
			"/abc/1.md",
		}, actualPaths)
	})

	t.Run("RevisionEntryPathCompare with different types", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := td.RevisionEntryExt("a", RevisionEntryDelete, ModeDir, "")
		assert.Equal(0, RevisionEntryPathCompare(sut, td.RevisionEntryExt("a", RevisionEntryDelete, ModeDir, "")))
		assert.Equal(0, RevisionEntryPathCompare(sut, td.RevisionEntryExt("a", RevisionEntryAdd, ModeDir, "")))
		assert.Equal(0, RevisionEntryPathCompare(sut, td.RevisionEntryExt("a", RevisionEntryUpdate, ModeDir, "")))

		// Files are greater than directories.
		assert.Equal(1, RevisionEntryPathCompare(sut, td.RevisionEntryExt("a", RevisionEntryUpdate, 0, "")))
	})
}

func TestRevision(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := td.Revision(RevisionId{})
		err := MarshalRevision(sut, &buf)
		assert.NoError(err)
		read, err := UnmarshalRevision(&buf)
		assert.NoError(err)
		assert.Equal(sut, read)
	})
}
