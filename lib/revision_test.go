package lib

import (
	"bytes"
	"math/rand/v2"
	"slices"
	"strings"
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
			sut := fakeRevisionEntry("a.txt", entryType)
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
		sut := fakeRevisionEntry("a.txt", RevisionEntryAdd)
		err := MarshalRevisionEntry(sut, &buf)
		assert.NoError(err)
		assert.Equal(sut.MarshalledSize(), buf.Len())

		sut = fakeRevisionEntry("a.txt", RevisionEntryDelete)
		sut.Metadata = nil
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
			entry := fakeRevisionEntry(path, RevisionEntryAdd)
			entry.Metadata = fakeFileMetadata(ModeDir)
			return entry
		}
		fileEntry := func(path string) *RevisionEntry {
			t.Helper()
			return fakeRevisionEntry(path, RevisionEntryAdd)
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
}

func TestRevision(t *testing.T) {
	t.Parallel()
	t.Run("Marshal and Unmarshal", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		var buf bytes.Buffer
		sut := fakeRevision(RevisionId{})
		err := MarshalRevision(sut, &buf)
		assert.NoError(err)
		read, err := UnmarshalRevision(&buf)
		assert.NoError(err)
		assert.Equal(sut, read)
	})
}

func fakeRevision(parent RevisionId) *Revision {
	return &Revision{
		TimestampSec:  123456789,
		TimestampNSec: 12345,
		Message:       "test message",
		Author:        "test author",
		Parent:        parent,
		Blocks:        []BlockId{fakeBlockId("1")},
	}
}

func fakeRevisionEntry(path string, entryType RevisionEntryType) *RevisionEntry {
	return fakeRevisionEntryMode(path, entryType, 0)
}

func fakeRevisionEntryMode(path string, entryType RevisionEntryType, mode ModeAndPerm) *RevisionEntry {
	var metadata *FileMetadata
	if entryType != RevisionEntryDelete {
		metadata = fakeFileMetadata(mode)
	}
	return &RevisionEntry{
		Path:     NewPath(strings.Split(path, "/")...),
		Type:     entryType,
		Metadata: metadata,
	}
}
