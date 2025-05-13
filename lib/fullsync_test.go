package lib

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommitNWayMergeSort(t *testing.T) {
	t.Parallel()
	type Item struct {
		Value int
	}
	marshal := func(i Item, w io.Writer) error {
		return binary.Write(w, binary.LittleEndian, int64(i.Value))
	}
	unmarshal := func(r io.Reader) (Item, error) {
		var v int64
		err := binary.Read(r, binary.LittleEndian, &v)
		return Item{int(v)}, err
	}
	compare := func(a, b Item) (int, error) {
		return a.Value - b.Value, nil
	}
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		chunks := [][]int{
			{1, 4, 7},
			{2, 5, 8},
			{3, 6, 9},
		}
		var readers []io.Reader
		for _, chunk := range chunks {
			buf := &bytes.Buffer{}
			for _, val := range chunk {
				_ = marshal(Item{val}, buf)
			}
			readers = append(readers, buf)
		}
		out := &bytes.Buffer{}
		err := nWayMergeSort[Item](readers, out, unmarshal, marshal, compare)
		assert.NoError(err)
		var values []int
		for {
			it, err := unmarshal(out)
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(err)
			values = append(values, it.Value)
		}
		assert.Equal([]int{1, 2, 3, 4, 5, 6, 7, 8, 9}, values)
	})
	t.Run("Fuzzing", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		chunks := make([]bytes.Buffer, 5)
		numValues := len(chunks) * 100
		for i := range numValues {
			// Bias towards lower indexes. This creates an uneven distribution
			// of entries across chunks.
			chunkIndex := int(rand.ExpFloat64()) % len(chunks)
			err := marshal(Item{Value: i}, &chunks[chunkIndex])
			assert.NoError(err)
		}
		readers := make([]io.Reader, len(chunks))
		for i, c := range chunks {
			readers[i] = &c
		}
		out := &bytes.Buffer{}
		err := nWayMergeSort(readers, out, unmarshal, marshal, compare)
		assert.NoError(err)
		for i := range numValues {
			it, err := unmarshal(out)
			assert.NoError(err)
			assert.Equal(i, it.Value)
		}
		_, err = unmarshal(out)
		assert.ErrorIs(err, io.EOF)
	})
	t.Run("Compare function error is propagated", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}
		_ = marshal(Item{1}, buf1)
		_ = marshal(Item{2}, buf2)
		badCompare := func(a, b Item) (int, error) {
			return 0, Errorf("Boom")
		}
		err := nWayMergeSort([]io.Reader{buf1, buf2}, &bytes.Buffer{}, unmarshal, marshal, badCompare)
		assert.Error(err, "Boom")
	})
}

func TestStagingMetadataCollection(t *testing.T) {
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

		err = sut.Close()
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
		err = sut.Close()
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
		err = sut.Close()
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
		assert.NoError(sut.Close())
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

func BenchmarkStagingMetadataCollection(b *testing.B) {
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
	err = sut.Close()
	assert.NoError(err)
}
