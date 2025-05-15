package lib

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
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

func TestRevisionEntryChunks(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionEntryChunks(dir, "chunk", 500)

		add := func(path string, mode ModeAndPerm) {
			err := sut.Add(&RevisionEntry{Path(path), RevisionEntryAdd, fakeFileMetadata(mode)})
			assert.NoError(err)
		}

		add("/some/dir1", ModeDir)
		add("/some/dir1/fileb", 0)
		files, _ := os.ReadDir(dir)
		assert.Equal(0, len(files))
		// Now it should rotate.
		add("/some/dir1/filea", 0)
		files, _ = os.ReadDir(dir)
		assert.Equal(1, len(files))
		add("/some/dir2/filec", 0)
		add("/some/dir1/a", 0)
		add("/some/dir1/b", 0)
		files, _ = os.ReadDir(dir)
		assert.Equal(2, len(files))
		add("/some/dir2/filea", 0)
		add("/some/dir2", ModeDir)
		add("/some", ModeDir)

		merged := []*RevisionEntry{}
		err := sut.MergeChunks(func(re *RevisionEntry) error {
			merged = append(merged, re)
			return nil
		})
		assert.NoError(err)
		assert.Equal(9, len(merged))
		assert.Equal("/some", string(merged[0].Path))
		assert.Equal("/some/dir1", string(merged[1].Path))
		assert.Equal("/some/dir1/a", string(merged[2].Path))
		assert.Equal("/some/dir1/b", string(merged[3].Path))
		assert.Equal("/some/dir1/filea", string(merged[4].Path))
		assert.Equal("/some/dir1/fileb", string(merged[5].Path))
		assert.Equal("/some/dir2", string(merged[6].Path))
		assert.Equal("/some/dir2/filea", string(merged[7].Path))
		assert.Equal("/some/dir2/filec", string(merged[8].Path))
	})
	t.Run("Single chuck", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionEntryChunks(dir, "chunk", defaultChunkSize)

		err := sut.Add(fakeRevisionEntry("/some/dir/fileb", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.Add(fakeRevisionEntry("/some/dir/filea", RevisionEntryAdd))
		assert.NoError(err)
		merged := []*RevisionEntry{}
		err = sut.MergeChunks(func(re *RevisionEntry) error {
			merged = append(merged, re)
			return nil
		})
		assert.NoError(err)
		assert.Equal(2, len(merged))
		assert.Equal("/some/dir/filea", string(merged[0].Path))
		assert.Equal("/some/dir/fileb", string(merged[1].Path))
	})
	t.Run("Duplicate paths in the same chunk are rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionEntryChunks(dir, "chunk", defaultChunkSize)

		err := sut.Add(fakeRevisionEntry("/some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.Add(fakeRevisionEntry("/some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.rotateChunk()
		assert.Error(err, "duplicate revision entry path: /some/dir/file")
	})
	t.Run("Duplicate paths in different chunks are rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionEntryChunks(dir, "chunk", 1)

		err := sut.Add(fakeRevisionEntry("/some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.Add(fakeRevisionEntry("/some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.rotateChunk()
		assert.NoError(err)
		assert.Equal(2, sut.chunkIndex)
		err = sut.MergeChunks(func(re *RevisionEntry) error { return nil })
		assert.Error(err, "duplicate revision entry path: /some/dir/file")
	})
	t.Run("Fuzzing", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		// Generate all path permutations.
		parts := []string{"one", "two", "three", "four", "five"}
		depth := 5
		var paths []string
		var build func([]string, int)
		build = func(curr []string, d int) {
			if d == 0 {
				paths = append(paths, "/"+strings.Join(curr, "/"))
				return
			}
			for _, p := range parts {
				build(append(curr, p), d-1)
			}
		}
		build(nil, depth)
		// Shuffle the paths to simulate unordered input.
		rand.Shuffle(len(paths), func(i, j int) { paths[i], paths[j] = paths[j], paths[i] })
		// Use small chunk size to force rotation.
		sut := NewRevisionEntryChunks(t.TempDir(), "chunk", 32*1024)
		for _, p := range paths {
			assert.NoError(sut.Add(fakeRevisionEntry(p, RevisionEntryAdd)))
		}
		assert.Greater(sut.chunkIndex, 10)
		merged := []*RevisionEntry{}
		err := sut.MergeChunks(func(re *RevisionEntry) error {
			merged = append(merged, re)
			return nil
		})
		assert.NoError(err)
		assert.Equal(len(paths), len(merged))
		last := ""
		for _, entry := range merged {
			curr := string(entry.Path)
			assert.Equal(true, curr > last, "unsorted: %q > %q", curr, last)
			last = curr
		}
	})
}

func BenchmarkRevisionEntryChunks(b *testing.B) {
	assert := NewAssert(b)
	dir := b.TempDir()
	sut := NewRevisionEntryChunks(dir, "chunk", 16*1024) // Force chunk rotation.
	for range b.N {
		path := fmt.Sprintf("/%d/%d/%d", rand.Int(), rand.Int(), rand.Int()) //nolint:gosec
		_ = sut.Add(fakeRevisionEntry(path, RevisionEntryAdd))
	}
	if b.N > 1000 {
		files, _ := os.ReadDir(dir)
		// Make sure we wrote multiple files.
		assert.Greater(len(files), 1)
	}
	err := sut.MergeChunks(func(re *RevisionEntry) error { return nil })
	assert.NoError(err)
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
	var metadata *FileMetadata
	if entryType != RevisionEntryDelete {
		metadata = fakeFileMetadata(0)
	}
	return &RevisionEntry{
		Path:     NewPath(strings.Split(path, "/")...),
		Type:     entryType,
		Metadata: metadata,
	}
}
