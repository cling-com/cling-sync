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

// We test with `RevisionEntry` because it is the most common use case.
func TestTemp(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut := NewRevisionEntryTempWriter(fs, 400+chunkFramingOverhead)

		add := func(path string, mode FileMode) {
			err := sut.Add(
				&RevisionEntry{Kind: RevisionEntryKindAdd, Path: Path{path}, Metadata: *td.PathMetadata(mode)},
			)
			assert.NoError(err)
		}

		add("some/dir1", FileModeDir)
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
		add("some/dir2", FileModeDir)
		add("some", FileModeDir)

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
		entry := td.RevisionEntry("1.txt", RevisionEntryKindAdd)
		sut := NewRevisionEntryTempWriter(fs, marshallSize(entry)*3+chunkFramingOverhead)
		for i := range 3 {
			err := sut.Add(td.RevisionEntry(fmt.Sprintf("%d.txt", i), RevisionEntryKindAdd))
			assert.NoError(err)
			assert.Equal(0, sut.chunks, "chunk should not have been rotated")
		}
		err := sut.Add(td.RevisionEntry("4.txt", RevisionEntryKindAdd))
		assert.NoError(err)
		assert.Equal(1, sut.chunks, "chunk should have been rotated")

		// Now, try with a chunk size that is one byte smaller than 3 entries.
		sut = NewRevisionEntryTempWriter(fs, marshallSize(entry)*3-1+chunkFramingOverhead)
		for i := range 2 {
			err := sut.Add(td.RevisionEntry(fmt.Sprintf("%d.txt", i), RevisionEntryKindAdd))
			assert.NoError(err)
			assert.Equal(0, sut.chunks, "chunk should not have been rotated")
		}
		err = sut.Add(td.RevisionEntry("3.txt", RevisionEntryKindAdd))
		assert.NoError(err)
		assert.Equal(1, sut.chunks, "chunk should have been rotated")
	})

	t.Run("Single chuck", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut := NewRevisionEntryTempWriter(fs, DefaultTempChunkSize)

		err := sut.Add(td.RevisionEntry("some/dir/fileb", RevisionEntryKindAdd))
		assert.NoError(err)
		err = sut.Add(td.RevisionEntry("some/dir/filea", RevisionEntryKindAdd))
		assert.NoError(err)
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, nil)
		assert.Equal(2, len(merged))
		names := make([]string, len(merged))
		for i, entry := range merged {
			names[i] = entry.Path.String()
		}
		slices.Sort(names)
		assert.Equal([]string{"some/dir/filea", "some/dir/fileb"}, names)
	})

	t.Run("Duplicate entries in the same chunk are rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut := NewRevisionEntryTempWriter(fs, DefaultTempChunkSize)

		err := sut.Add(td.RevisionEntry("some/dir/file", RevisionEntryKindAdd))
		assert.NoError(err)
		err = sut.Add(td.RevisionEntry("some/dir/file", RevisionEntryKindAdd))
		assert.NoError(err)
		err = sut.rotateChunk()
		assert.Error(err, "duplicate entry")
		assert.Error(err, "some/dir/file")
	})

	t.Run("Duplicate entries in different chunks are rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut := NewRevisionEntryTempWriter(fs, 1)

		err := sut.Add(td.RevisionEntry("some/dir/file", RevisionEntryKindAdd))
		assert.NoError(err)
		err = sut.Add(td.RevisionEntry("some/dir/file", RevisionEntryKindAdd))
		assert.NoError(err)
		err = sut.rotateChunk()
		assert.NoError(err)
		assert.Equal(2, sut.chunks)
		_, err = sut.Finalize()
		assert.Error(err, "duplicate entry")
		assert.Error(err, "some/dir/file")
	})

	t.Run("PathFilter", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut := NewRevisionEntryTempWriter(fs, DefaultTempChunkSize)

		for _, path := range []string{"a.txt", "sub/a.txt", "b.txt"} {
			err := sut.Add(td.RevisionEntry(path, RevisionEntryKindAdd))
			assert.NoError(err)
		}

		filter := NewPathExclusionFilter([]string{"**/a.txt"})
		temp, err := sut.Finalize()
		assert.NoError(err)
		merged := readAllRevsisionTemp(t, temp, filter)
		assert.Equal(1, len(merged))
		assert.Equal("b.txt", merged[0].Path.String())
	})

	t.Run("Filter filtering everything", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut := NewRevisionEntryTempWriter(fs, DefaultTempChunkSize)

		for _, path := range []string{"a.txt", "sub/a.txt", "b.txt"} {
			err := sut.Add(td.RevisionEntry(path, RevisionEntryKindAdd))
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
		sut := NewRevisionEntryTempWriter(td.NewFS(t), 32*1024)
		for _, p := range paths {
			assert.NoError(sut.Add(td.RevisionEntry(p, RevisionEntryKindAdd)))
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

	t.Run("Multi-frame chunk files", func(t *testing.T) {
		t.Parallel()
		// Push enough entries through that the writer produces multiple chunk
		// files, each containing multiple frames. Then check Finalize streams
		// the merge correctly: count matches, output is sorted, every chunk
		// file is multi-frame.
		assert := NewAssert(t)
		sut := NewRevisionEntryTempWriter(td.NewFS(t), 256*1024)
		const n = 4000
		for range n {
			p := fmt.Sprintf("p-%d.txt", rand.Int())
			assert.NoError(sut.Add(td.RevisionEntry(p, RevisionEntryKindAdd)))
		}
		temp, err := sut.Finalize()
		assert.NoError(err)
		assert.Greater(temp.Chunks(), 1)
		for i := range temp.Chunks() {
			f, err := countFramesInChunkFile(temp, i)
			assert.NoError(err)
			assert.Greater(f, 1, "chunk %d has %d frame(s)", i, f)
		}
		merged := readAllRevsisionTemp(t, temp, nil)
		assert.Equal(n, len(merged))
		for i := 1; i < len(merged); i++ {
			a, b := merged[i-1].Path.String(), merged[i].Path.String()
			assert.Equal(true, a < b, "unsorted at %d: %q >= %q", i, a, b)
		}
	})
}

func countFramesInChunkFile[T Marshallable](temp *Temp[T], i int) (int, error) {
	f, err := temp.fs.OpenRead(fmt.Sprintf("%d.sorted", i))
	if err != nil {
		return 0, err
	}
	defer f.Close() //nolint:errcheck
	data, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}
	r := NewProtobufReader(data)
	n := 0
	for !r.AtEnd() {
		_, _, err := r.ReadTag()
		if err != nil {
			return 0, err
		}
		if _, err := r.ReadBytes(); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}

func TestTempCache(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut := NewRevisionEntryTempWriter(fs, 400+chunkFramingOverhead)
		add := func(path string, mode FileMode) {
			err := sut.Add(
				&RevisionEntry{Kind: RevisionEntryKindAdd, Path: Path{path}, Metadata: *td.PathMetadata(mode)},
			)
			assert.NoError(err)
		}

		add("b.txt", 0)
		add("y.txt", 0)
		add("sub", FileModeDir)
		add("sub/a.txt", 0)
		add("sub/y.txt", 0)
		add("sub/sub", FileModeDir)
		add("sub/sub/a.txt", 0)
		add("sub/sub/y.txt", 0)

		temp, err := sut.Finalize()
		assert.NoError(err)
		assert.Equal(4, temp.Chunks())

		cache, err := NewRevisionEntryTempCache(temp, 2)
		assert.NoError(err)

		// First, check that we find all entries.
		for _, path := range []string{"b.txt", "y.txt", "sub", "sub/a.txt", "sub/y.txt", "sub/sub", "sub/sub/a.txt", "sub/sub/y.txt"} {
			isDir := path == "sub" || path == "sub/sub"
			entry, ok, err := cache.Get(PathCompareString(Path{path}, isDir))
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
			entry, ok, err := cache.Get(PathCompareString(Path{path}, isDir))
			assert.NoError(err, path)
			assert.Equal(false, ok, path)
			assert.Nil(entry, path)
		}
		// The cache size is only two, so some more cache misses happened.
		assert.Equal(7, cache.CacheMisses)
	})

	t.Run("LRU eviction respects maxChunksInCache", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut := NewRevisionEntryTempWriter(fs, 400+chunkFramingOverhead)
		add := func(path string, mode FileMode) {
			err := sut.Add(
				&RevisionEntry{Kind: RevisionEntryKindAdd, Path: Path{path}, Metadata: *td.PathMetadata(mode)},
			)
			assert.NoError(err)
		}

		add("b.txt", 0)
		add("y.txt", 0)
		add("sub", FileModeDir)
		add("sub/a.txt", 0)
		add("sub/y.txt", 0)
		add("sub/sub", FileModeDir)
		add("sub/sub/a.txt", 0)
		add("sub/sub/y.txt", 0)

		temp, err := sut.Finalize()
		assert.NoError(err)
		assert.Equal(4, temp.Chunks())

		loadedChunks := func(c *TempCache[*RevisionEntry]) []int {
			loaded := []int{}
			for i, chunk := range c.cache {
				if chunk != nil {
					loaded = append(loaded, i)
				}
			}
			return loaded
		}

		// maxChunksInCache = 2, so at most 2 chunks should be in memory.
		cache, err := NewRevisionEntryTempCache(temp, 2)
		assert.NoError(err)
		assert.Equal([]int{}, loadedChunks(cache))

		// Figure out which chunk each entry lives in by looking up three
		// entries that must reside in different chunks.
		chunkOf := func(path string, isDir bool) int {
			key := PathCompareString(Path{path}, isDir)
			for i := len(cache.firstEntries) - 1; i >= 0; i-- {
				if strings.Compare(key, cache.firstEntries[i]) >= 0 {
					return i
				}
			}
			return -1
		}

		chunkA := chunkOf("b.txt", false)
		chunkB := chunkOf("sub/a.txt", false)
		chunkC := chunkOf("sub/sub/a.txt", false)
		// Sanity: all three must be in distinct chunks.
		assert.Equal(true, chunkA != chunkB && chunkB != chunkC && chunkA != chunkC,
			"entries must be in 3 distinct chunks: %d, %d, %d", chunkA, chunkB, chunkC)

		// Load first entry - one chunk loaded.
		_, ok, err := cache.Get(PathCompareString(Path{"sub/sub/a.txt"}, false))
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal([]int{chunkC}, loadedChunks(cache))

		// Load second entry in a different chunk - two chunks loaded (at the limit).
		_, ok, err = cache.Get(PathCompareString(Path{"b.txt"}, false))
		assert.NoError(err)
		assert.Equal(true, ok)
		loaded := loadedChunks(cache)
		assert.Equal(2, len(loaded))

		// Load third entry in yet another chunk - must evict one, keeping exactly 2.
		_, ok, err = cache.Get(PathCompareString(Path{"sub/a.txt"}, false))
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal(2, len(loadedChunks(cache)),
			"at most maxChunksInCache (2) chunks should be loaded, got: %v",
			loadedChunks(cache))
	})
}

func BenchmarkRevisionTemp(b *testing.B) {
	assert := NewAssert(b)
	const n = 5000
	paths := make([]string, n)
	for i := range paths {
		paths[i] = fmt.Sprintf("%d/%d/%d", rand.Int(), rand.Int(), rand.Int())
	}
	b.ResetTimer()
	for b.Loop() {
		fs := td.NewFS(b)
		sut := NewRevisionEntryTempWriter(fs, 16*1024) // Force chunk rotation.
		for _, p := range paths {
			_ = sut.Add(td.RevisionEntry(p, RevisionEntryKindAdd))
		}
		_, err := sut.Finalize()
		assert.NoError(err)
	}
}

func readAllRevsisionTemp(t *testing.T, sut *Temp[*RevisionEntry], pathFilter PathFilter) []*RevisionEntry {
	t.Helper()
	assert := NewAssert(t)
	merged := []*RevisionEntry{}
	tempReader := sut.Reader(RevisionEntryPathFilter(pathFilter))
	buf := NewBlockBuf()
	for {
		entry, err := tempReader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(err)
		merged = append(merged, entry)
	}
	return merged
}
