package lib

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStaging(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewStaging(fakeRevisionId("head"), dir)
		assert.NoError(err)
		sut.chunkWriter.maxChunkSize = 500
		add := func(path string, mode ModeAndPerm) {
			err = sut.Add(Path(path), fakeFileMetadata(mode))
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

		err = sut.MergeChunks()
		assert.NoError(err)
		sortedFile, err := os.Open(filepath.Join(dir, "staging"))
		assert.NoError(err)
		defer sortedFile.Close() //nolint:errcheck
		re := []RevisionEntry{}
		for {
			e, err := UnmarshalRevisionEntry(sortedFile)
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(err)
			re = append(re, *e)
		}
		assert.Equal(9, len(re))
		assert.Equal("/some", string(re[0].Path))
		assert.Equal("/some/dir1", string(re[1].Path))
		assert.Equal("/some/dir1/a", string(re[2].Path))
		assert.Equal("/some/dir1/b", string(re[3].Path))
		assert.Equal("/some/dir1/filea", string(re[4].Path))
		assert.Equal("/some/dir1/fileb", string(re[5].Path))
		assert.Equal("/some/dir2", string(re[6].Path))
		assert.Equal("/some/dir2/filea", string(re[7].Path))
		assert.Equal("/some/dir2/filec", string(re[8].Path))
	})
	t.Run("Single chuck", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewStaging(fakeRevisionId("head"), dir)
		assert.NoError(err)
		err = sut.Add(Path("/some/dir/fileb"), fakeFileMetadata(0))
		assert.NoError(err)
		err = sut.Add(Path("/some/dir/filea"), fakeFileMetadata(0))
		assert.NoError(err)
		err = sut.MergeChunks()
		assert.NoError(err)
		sortedFile, err := os.Open(filepath.Join(dir, "staging"))
		assert.NoError(err)
		defer sortedFile.Close() //nolint:errcheck
		re := []RevisionEntry{}
		for {
			f, err := UnmarshalRevisionEntry(sortedFile)
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(err)
			re = append(re, *f)
		}
		assert.Equal(2, len(re))
		assert.Equal("/some/dir/filea", string(re[0].Path))
		assert.Equal("/some/dir/fileb", string(re[1].Path))
	})
	t.Run("Duplicate paths in the same chunk are rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewStaging(fakeRevisionId("head"), dir)
		assert.NoError(err)
		err = sut.Add(Path("/some/dir/file"), fakeFileMetadata(0))
		assert.NoError(err)
		err = sut.Add(Path("/some/dir/file"), fakeFileMetadata(0))
		assert.NoError(err)
		err = sut.chunkWriter.rotateChunk()
		assert.Error(err, "duplicate revision entry path: /some/dir/file")
	})
	t.Run("Duplicate paths in different chunks are rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewStaging(fakeRevisionId("head"), dir)
		assert.NoError(err)
		sut.chunkWriter.maxChunkSize = 1
		err = sut.Add(Path("/some/dir/file"), fakeFileMetadata(0))
		assert.NoError(err)
		err = sut.Add(Path("/some/dir/file"), fakeFileMetadata(0))
		assert.NoError(err)
		err = sut.chunkWriter.rotateChunk()
		assert.NoError(err)
		assert.Equal(2, sut.chunkWriter.chunkIndex)
		err = sut.MergeChunks()
		assert.Error(err, "duplicate revision entry path: /some/dir/file")
	})
	t.Run("Fuzzing", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		// Generate all path permutations of fixed depth from parts.
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
		// Set up Changes with small chunk size to force rotation.
		dir := t.TempDir()
		sut, err := NewStaging(fakeRevisionId("head"), dir)
		assert.NoError(err)
		sut.chunkWriter.maxChunkSize = 32 * 1024
		// Add all entries.
		for _, p := range paths {
			assert.NoError(sut.Add(Path(p), fakeFileMetadata(0)))
		}
		assert.Greater(sut.chunkWriter.chunkIndex, 10)
		assert.NoError(sut.MergeChunks())
		// Verify that the resulting file is sorted.
		f, err := os.Open(filepath.Join(dir, "staging"))
		assert.NoError(err)
		defer f.Close() //nolint:errcheck
		last := ""
		for {
			entry, err := UnmarshalRevisionEntry(f)
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(err)
			curr := string(entry.Path)
			assert.Equal(true, curr > last, "unsorted: %q > %q", curr, last)
			last = curr
		}
	})
}

func BenchmarkStaging(b *testing.B) {
	assert := NewAssert(b)
	dir := b.TempDir()
	sut, err := NewStaging(fakeRevisionId("head"), dir)
	assert.NoError(err)
	sut.chunkWriter.maxChunkSize = 16 * 1024 // Force chunk rotation.
	for range b.N {
		path := fmt.Sprintf("/%d/%d/%d", rand.Int(), rand.Int(), rand.Int()) //nolint:gosec
		_ = sut.Add(Path(path), fakeFileMetadata(0))
	}
	if b.N > 1000 {
		files, _ := os.ReadDir(dir)
		// Make sure we wrote multiple files.
		assert.Greater(len(files), 1)
	}
	err = sut.MergeChunks()
	assert.NoError(err)
}
