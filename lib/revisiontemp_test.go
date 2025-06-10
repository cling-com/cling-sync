package lib

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
)

func TestRevisionTemp(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionTempWriter(dir, 500)

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

		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, nil)
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

	t.Run("Chunk size is not exceeded", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()

		// First, try with a chunk size that *exactly* fits 3 entries.
		entry := fakeRevisionEntry("1.txt", RevisionEntryAdd)
		sut := NewRevisionTempWriter(dir, entry.MarshalledSize()*3)
		for i := range 3 {
			err := sut.Add(fakeRevisionEntry(fmt.Sprintf("%d.txt", i), RevisionEntryAdd))
			assert.NoError(err)
			assert.Equal(0, sut.chunks, "chunk should not have been rotated")
		}
		err := sut.Add(fakeRevisionEntry("4.txt", RevisionEntryAdd))
		assert.NoError(err)
		assert.Equal(1, sut.chunks, "chunk should have been rotated")

		// Now, try with a chunk size that is on byte smaller than 3 entries.
		sut = NewRevisionTempWriter(dir, entry.MarshalledSize()*3-1)
		for i := range 2 {
			err := sut.Add(fakeRevisionEntry(fmt.Sprintf("%d.txt", i), RevisionEntryAdd))
			assert.NoError(err)
			assert.Equal(0, sut.chunks, "chunk should not have been rotated")
		}
		err = sut.Add(fakeRevisionEntry("3.txt", RevisionEntryAdd))
		assert.NoError(err)
		assert.Equal(1, sut.chunks, "chunk should have been rotated")
	})

	t.Run("Sort order is files, directories, and subdirectories", func(t *testing.T) {
		// This basically makes sure that we always use `RevisionEntryPathCompare`.
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		// Use a small chunk size to force rotation.
		sut := NewRevisionTempWriter(dir, 700)

		add := func(path string, mode ModeAndPerm) {
			err := sut.Add(&RevisionEntry{Path(path), RevisionEntryAdd, fakeFileMetadata(mode)})
			assert.NoError(err)
		}

		add("sub", ModeDir)
		add("sub/sub", ModeDir)
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
		assert.Equal(3, sut.chunks, "should be multiple chunks")
		merged := readAllRevsisionTemp(t, temp, nil)
		actualPaths := make([]string, len(merged))
		for i, entry := range merged {
			actualPaths[i] = string(entry.Path)
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

	t.Run("Single chuck", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionTempWriter(dir, DefaultRevisionTempChunkSize)

		err := sut.Add(fakeRevisionEntry("/some/dir/fileb", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.Add(fakeRevisionEntry("/some/dir/filea", RevisionEntryAdd))
		assert.NoError(err)
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, nil)
		assert.Equal(2, len(merged))
		assert.Equal("/some/dir/filea", string(merged[0].Path))
		assert.Equal("/some/dir/fileb", string(merged[1].Path))
	})

	t.Run("Duplicate paths in the same chunk are rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionTempWriter(dir, DefaultRevisionTempChunkSize)

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
		sut := NewRevisionTempWriter(dir, 1)

		err := sut.Add(fakeRevisionEntry("/some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.Add(fakeRevisionEntry("/some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.rotateChunk()
		assert.NoError(err)
		assert.Equal(2, sut.chunks)
		_, err = sut.Finalize()
		assert.Error(err, "duplicate revision entry path: /some/dir/file")
	})

	t.Run("PathFilter", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionTempWriter(dir, DefaultRevisionTempChunkSize)

		for _, path := range []string{"a.txt", "sub/a.txt", "b.txt"} {
			err := sut.Add(fakeRevisionEntry(path, RevisionEntryAdd))
			assert.NoError(err)
		}

		filtered, err := NewPathExclusionFilter([]string{"**/a.txt"}, []string{})
		assert.NoError(err)
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, filtered)
		assert.Equal(1, len(merged))
		assert.Equal("b.txt", string(merged[0].Path))
	})

	t.Run("PathFilter filtering everything", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionTempWriter(dir, DefaultRevisionTempChunkSize)

		for _, path := range []string{"a.txt", "sub/a.txt", "b.txt"} {
			err := sut.Add(fakeRevisionEntry(path, RevisionEntryAdd))
			assert.NoError(err)
		}

		filtered, err := NewPathExclusionFilter([]string{"**/*"}, []string{})
		assert.NoError(err)
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, filtered)
		assert.Equal(0, len(merged))
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
		sut := NewRevisionTempWriter(t.TempDir(), 32*1024)
		for _, p := range paths {
			assert.NoError(sut.Add(fakeRevisionEntry(p, RevisionEntryAdd)))
		}
		assert.Greater(sut.chunks, 10)
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, nil)
		assert.Equal(len(paths), len(merged))
		last := ""
		for _, entry := range merged {
			curr := string(entry.Path)
			assert.Equal(true, curr > last, "unsorted: %q > %q", curr, last)
			last = curr
		}
	})
}

func TestRevisionTempCache(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut := NewRevisionTempWriter(dir, 500)
		add := func(path string, mode ModeAndPerm) {
			err := sut.Add(&RevisionEntry{Path(path), RevisionEntryAdd, fakeFileMetadata(mode)})
			assert.NoError(err)
		}

		add("b.txt", 0)
		add("y.txt", 0)
		add("sub", ModeDir)
		add("sub/a.txt", 0)
		add("sub/y.txt", 0)
		add("sub/sub", ModeDir)
		add("sub/sub/a.txt", 0)
		add("sub/sub/y.txt", 0)

		temp, err := sut.Finalize()
		assert.NoError(err)
		assert.Equal(4, temp.Chunks())

		cache, err := NewRevisionTempCache(temp, 2)
		assert.NoError(err)

		// First, check that we find all entries.
		for _, path := range []string{"b.txt", "y.txt", "sub", "sub/a.txt", "sub/y.txt", "sub/sub", "sub/sub/a.txt", "sub/sub/y.txt"} {
			isDir := path == "sub" || path == "sub/sub"
			entry, ok, err := cache.Get(Path(path), isDir)
			assert.NoError(err, path)
			assert.Equal(true, ok, path)
			assert.Equal(path, string(entry.Path), path)
		}

		// We read all entries in order so we should never evict a chunk we
		// need later.
		assert.Equal(4, cache.CacheMisses)

		// Check that we don't find entries.
		for _, path := range []string{"a.txt", "z.txt", "sub/z.txt", "sub/sub/z.txt"} {
			isDir := path == "sub" || path == "sub/sub"
			entry, ok, err := cache.Get(Path(path), isDir)
			assert.NoError(err, path)
			assert.Equal(false, ok, path)
			assert.Nil(entry, path)
		}
		assert.Equal(4, cache.CacheMisses)
	})
}

func BenchmarkRevisionTemp(b *testing.B) {
	assert := NewAssert(b)
	dir := b.TempDir()
	sut := NewRevisionTempWriter(dir, 16*1024) // Force chunk rotation.
	for b.Loop() {
		path := fmt.Sprintf("/%d/%d/%d", rand.Int(), rand.Int(), rand.Int()) //nolint:gosec
		_ = sut.Add(fakeRevisionEntry(path, RevisionEntryAdd))
	}
	if b.N > 1000 {
		files, _ := os.ReadDir(dir)
		// Make sure we wrote multiple files.
		assert.Greater(len(files), 1)
	}
	_, err := sut.Finalize()
	assert.NoError(err)
}

func readAllRevsisionTemp(t *testing.T, sut *RevisionTemp, pathFilter PathFilter) []*RevisionEntry {
	t.Helper()
	assert := NewAssert(t)
	merged := []*RevisionEntry{}
	tempReader := sut.Reader(pathFilter)
	for {
		entry, err := tempReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(err)
		merged = append(merged, entry)
	}
	return merged
}
