package lib

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"strings"
	"testing"
)

func TestRevisionTemp(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut, err := NewRevisionTempWriter(RevisionId{}, fs, 500)
		assert.NoError(err)

		add := func(path string, mode ModeAndPerm) {
			err := sut.Add(&RevisionEntry{Path{path}, RevisionEntryAdd, td.FileMetadata(mode)})
			assert.NoError(err)
		}

		add("some/dir1", ModeDir)
		add("some/dir1/fileb", 0)
		files, _ := fs.ReadDir(".")
		assert.Equal(0, len(files))
		// Now it should rotate.
		add("some/dir1/filea", 0)
		files, _ = fs.ReadDir(".")
		assert.Equal(1, len(files))
		add("some/dir2/filec", 0)
		add("some/dir1/a", 0)
		add("some/dir1/b", 0)
		files, _ = fs.ReadDir(".")
		assert.Equal(2, len(files))
		add("some/dir2/filea", 0)
		add("some/dir2", ModeDir)
		add("some", ModeDir)

		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, nil)
		mergedPaths := make([]string, len(merged))
		for i, entry := range merged {
			mergedPaths[i] = entry.Path.String()
		}
		assert.Equal([]string{
			"some",
			"some/dir1",
			"some/dir1/a",
			"some/dir1/b",
			"some/dir1/filea",
			"some/dir1/fileb",
			"some/dir2",
			"some/dir2/filea",
			"some/dir2/filec",
		}, mergedPaths)
	})

	t.Run("Chunk size is not exceeded", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)

		// First, try with a chunk size that *exactly* fits 3 entries.
		entry := td.RevisionEntry("1.txt", RevisionEntryAdd)
		sut, err := NewRevisionTempWriter(RevisionId{}, fs, entry.MarshalledSize()*3)
		assert.NoError(err)
		for i := range 3 {
			err := sut.Add(td.RevisionEntry(fmt.Sprintf("%d.txt", i), RevisionEntryAdd))
			assert.NoError(err)
			assert.Equal(0, sut.chunks, "chunk should not have been rotated")
		}
		err = sut.Add(td.RevisionEntry("4.txt", RevisionEntryAdd))
		assert.NoError(err)
		assert.Equal(1, sut.chunks, "chunk should have been rotated")

		// Now, try with a chunk size that is on byte smaller than 3 entries.
		sut, err = NewRevisionTempWriter(RevisionId{}, fs, entry.MarshalledSize()*3-1)
		assert.NoError(err)
		for i := range 2 {
			err := sut.Add(td.RevisionEntry(fmt.Sprintf("%d.txt", i), RevisionEntryAdd))
			assert.NoError(err)
			assert.Equal(0, sut.chunks, "chunk should not have been rotated")
		}
		err = sut.Add(td.RevisionEntry("3.txt", RevisionEntryAdd))
		assert.NoError(err)
		assert.Equal(1, sut.chunks, "chunk should have been rotated")
	})

	t.Run("Sort order is files, directories, and subdirectories", func(t *testing.T) {
		// This basically makes sure that we always use `RevisionEntryPathCompare`.
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		// Use a small chunk size to force rotation.
		sut, err := NewRevisionTempWriter(RevisionId{}, fs, 700)
		assert.NoError(err)

		add := func(path string, mode ModeAndPerm) {
			err := sut.Add(&RevisionEntry{Path{path}, RevisionEntryAdd, td.FileMetadata(mode)})
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

	t.Run("Single chuck", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut, err := NewRevisionTempWriter(RevisionId{}, fs, DefaultRevisionTempChunkSize)
		assert.NoError(err)

		err = sut.Add(td.RevisionEntry("some/dir/fileb", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.Add(td.RevisionEntry("some/dir/filea", RevisionEntryAdd))
		assert.NoError(err)
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, nil)
		assert.Equal(2, len(merged))
		names := []string{}
		for _, entry := range merged {
			names = append(names, entry.Path.String())
		}
		slices.Sort(names)
		assert.Equal([]string{"some/dir/filea", "some/dir/fileb"}, names)
	})

	t.Run("Duplicate paths in the same chunk are rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut, err := NewRevisionTempWriter(RevisionId{}, fs, DefaultRevisionTempChunkSize)
		assert.NoError(err)

		err = sut.Add(td.RevisionEntry("some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.Add(td.RevisionEntry("some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.rotateChunk()
		assert.Error(err, "duplicate revision entry path: some/dir/file")
	})

	t.Run("Duplicate paths in different chunks are rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut, err := NewRevisionTempWriter(RevisionId{}, fs, 1)
		assert.NoError(err)

		err = sut.Add(td.RevisionEntry("some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.Add(td.RevisionEntry("some/dir/file", RevisionEntryAdd))
		assert.NoError(err)
		err = sut.rotateChunk()
		assert.NoError(err)
		assert.Equal(2, sut.chunks)
		_, err = sut.Finalize()
		assert.Error(err, "duplicate revision entry path: some/dir/file")
	})

	t.Run("PathFilter", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut, err := NewRevisionTempWriter(RevisionId{}, fs, DefaultRevisionTempChunkSize)
		assert.NoError(err)

		for _, path := range []string{"a.txt", "sub/a.txt", "b.txt"} {
			err := sut.Add(td.RevisionEntry(path, RevisionEntryAdd))
			assert.NoError(err)
		}

		filter := NewPathExclusionFilter([]string{"**/a.txt"})
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, filter)
		assert.Equal(1, len(merged))
		assert.Equal("b.txt", merged[0].Path.String())
	})

	t.Run("PathFilter filtering everything", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut, err := NewRevisionTempWriter(RevisionId{}, fs, DefaultRevisionTempChunkSize)
		assert.NoError(err)

		for _, path := range []string{"a.txt", "sub/a.txt", "b.txt"} {
			err := sut.Add(td.RevisionEntry(path, RevisionEntryAdd))
			assert.NoError(err)
		}

		filter := NewPathExclusionFilter([]string{"**/*"})
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, filter)
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
				paths = append(paths, strings.Join(curr, "/"))
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
		sut, err := NewRevisionTempWriter(RevisionId{}, td.NewFS(t), 32*1024)
		assert.NoError(err)
		for _, p := range paths {
			assert.NoError(sut.Add(td.RevisionEntry(p, RevisionEntryAdd)))
		}
		assert.Greater(sut.chunks, 10)
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, nil)
		assert.Equal(len(paths), len(merged))
		last := ""
		for _, entry := range merged {
			curr := entry.Path.String()
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
		fs := td.NewFS(t)
		sut, err := NewRevisionTempWriter(RevisionId{}, fs, 500)
		assert.NoError(err)
		add := func(path string, mode ModeAndPerm) {
			err := sut.Add(&RevisionEntry{Path{path}, RevisionEntryAdd, td.FileMetadata(mode)})
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
		// Empty the cache for testing purposes.
		cache.cache = make([]map[string]*RevisionEntry, temp.Chunks())
		cache.chunksInCache = 0
		assert.NoError(err)

		// First, check that we find all entries.
		for _, path := range []string{"b.txt", "y.txt", "sub", "sub/a.txt", "sub/y.txt", "sub/sub", "sub/sub/a.txt", "sub/sub/y.txt"} {
			isDir := path == "sub" || path == "sub/sub"
			entry, ok, err := cache.Get(Path{path}, isDir)
			assert.NoError(err, path)
			assert.Equal(true, ok, path)
			assert.Equal(path, entry.Path.String(), path)
		}

		// We read all entries in order so we should never evict a chunk we
		// need later.
		assert.Equal(4, cache.CacheMisses)

		// Check that we don't find entries.
		for _, path := range []string{"a.txt", "z.txt", "sub/z.txt", "sub/sub/z.txt"} {
			isDir := path == "sub" || path == "sub/sub"
			entry, ok, err := cache.Get(Path{path}, isDir)
			assert.NoError(err, path)
			assert.Equal(false, ok, path)
			assert.Nil(entry, path)
		}
		assert.Equal(4, cache.CacheMisses)
	})
}

func BenchmarkRevisionTemp(b *testing.B) {
	assert := NewAssert(b)
	fs := td.NewFS(b)
	sut, err := NewRevisionTempWriter(RevisionId{}, fs, 16*1024) // Force chunk rotation.
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		path := fmt.Sprintf("/%d/%d/%d", rand.Int(), rand.Int(), rand.Int()) //nolint:gosec
		_ = sut.Add(td.RevisionEntry(path, RevisionEntryAdd))
	}
	if b.N > 1000 {
		files, _ := fs.ReadDir(".")
		// Make sure we wrote multiple files.
		assert.Greater(len(files), 1)
	}
	_, err = sut.Finalize()
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
