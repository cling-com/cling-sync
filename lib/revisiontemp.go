// A sorted file-based storage for revision entries.
package lib

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"
)

const DefaultRevisionTempChunkSize = 4 * 1024 * 1024

// todo: encrypt the temporary files
type RevisionTemp struct {
	RevisionId RevisionId
	fs         FS
	chunks     int
}

func (rt *RevisionTemp) Reader(pathFilter PathFilter) *RevisionTempReader {
	return &RevisionTempReader{fs: rt.fs, chunks: rt.chunks, pathFilter: pathFilter} //nolint:exhaustruct
}

func (rt *RevisionTemp) Chunks() int {
	return rt.chunks
}

func (rt *RevisionTemp) Remove() error {
	if err := rt.fs.RemoveAll("."); err != nil {
		return WrapErrorf(err, "failed to remove temporary fs %s", rt.fs)
	}
	return nil
}

type RevisionTempReader struct {
	fs           FS
	pathFilter   PathFilter
	chunks       int
	chunkIndex   int
	current      []*RevisionEntry
	currentIndex int
}

func (rtr *RevisionTempReader) Read() (*RevisionEntry, error) {
	for {
		if rtr.current == nil || rtr.currentIndex == len(rtr.current) {
			if rtr.chunkIndex == rtr.chunks {
				return nil, io.EOF
			}
			entries, err := rtr.ReadChunk(rtr.chunkIndex)
			if err != nil {
				return nil, err
			}
			rtr.current = entries
			rtr.currentIndex = 0
			rtr.chunkIndex++
			if len(entries) == 0 {
				// All entries have been filtered out.
				continue
			}
		}
		re := rtr.current[rtr.currentIndex]
		rtr.currentIndex++
		if rtr.pathFilter == nil || rtr.pathFilter.Include(re.Path) {
			return re, nil
		}
	}
}

func (rtr *RevisionTempReader) ReadChunk(i int) ([]*RevisionEntry, error) {
	if i < 0 || i >= rtr.chunks {
		return nil, Errorf("chunk index out of range")
	}
	f, err := rtr.fs.OpenRead(rtr.chunkFilename(i))
	if err != nil {
		return nil, WrapErrorf(err, "failed to open chunk file %d", i)
	}
	defer f.Close() //nolint:errcheck
	r := bufio.NewReader(f)
	entries := []*RevisionEntry{}
	for {
		re, err := UnmarshalRevisionEntry(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, WrapErrorf(err, "failed to unmarshal revision entry from chunk file %d", i)
		}
		if rtr.pathFilter != nil && !rtr.pathFilter.Include(re.Path) {
			continue
		}
		entries = append(entries, re)
	}
	return entries, nil
}

// This ignores the `pathFilter` and reads all entries (obviously).
func (rtr *RevisionTempReader) ReadChunkRaw(i int) ([]byte, error) {
	if i < 0 || i >= rtr.chunks {
		return nil, Errorf("chunk index out of range")
	}
	f, err := rtr.fs.OpenRead(rtr.chunkFilename(i))
	if err != nil {
		return nil, WrapErrorf(err, "failed to open chunk file %d", i)
	}
	defer f.Close() //nolint:errcheck
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read chunk file %d", i)
	}
	return data, nil
}

func (rtr *RevisionTempReader) chunkFilename(index int) string {
	return fmt.Sprintf("%d.sorted", index)
}

type RevisionTempWriter struct {
	RevisionId   RevisionId
	fs           FS
	chunk        []*RevisionEntry
	chunkSize    int
	maxChunkSize int
	chunks       int
	fileExt      string
}

func NewRevisionTempWriter(revisionId RevisionId, fs FS, maxChunkSize int) *RevisionTempWriter {
	return &RevisionTempWriter{ //nolint:exhaustruct
		RevisionId:   revisionId,
		fs:           fs,
		maxChunkSize: maxChunkSize,
		fileExt:      "raw",
	}
}

func (rtw *RevisionTempWriter) Add(re *RevisionEntry) error {
	size := re.MarshalledSize()
	if rtw.chunkSize > 0 && rtw.chunkSize+size > rtw.maxChunkSize {
		if err := rtw.rotateChunk(); err != nil {
			return err
		}
	}
	rtw.chunk = append(rtw.chunk, re)
	rtw.chunkSize += size
	return nil
}

// Rotate the current chunk and then sort all chunks and return the merged result.
func (rtw *RevisionTempWriter) Finalize() (*RevisionTemp, error) { //nolint:funlen
	if err := rtw.rotateChunk(); err != nil {
		return nil, WrapErrorf(err, "failed to rotate final chunk")
	}
	// Create a new RevisionTempWriter to store the sorted chunks.
	sorted := NewRevisionTempWriter(rtw.RevisionId, rtw.fs, rtw.maxChunkSize)
	sorted.fileExt = "sorted"
	// Open all chunks with a buffered reader.
	readerFiles := make([]io.ReadCloser, rtw.chunks)
	readers := make([]io.Reader, rtw.chunks)
	for i := range rtw.chunks {
		f, err := rtw.fs.OpenRead(rtw.chunkFilename(i))
		if err != nil {
			return nil, WrapErrorf(err, "failed to open chunk file")
		}
		defer f.Close() //nolint:errcheck
		readerFiles[i] = f
		readers[i] = bufio.NewReader(f)
	}
	type entry struct {
		value      *RevisionEntry
		chunkIndex int
	}
	// First, read the first entry of each file.
	entries := make([]*entry, 0, len(readers))
	for i, r := range readers {
		// todo(perf): We should not need to unmarshal and the marshal all entries.
		value, err := UnmarshalRevisionEntry(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return nil, WrapErrorf(err, "failed to read from chunk %d", i)
		}
		entries = append(entries, &entry{value, i})
	}
	compare := func(a, b *RevisionEntry) (int, error) {
		c := RevisionEntryPathCompare(a, b)
		if c == 0 {
			return 0, Errorf("duplicate revision entry path: %s", a.Path)
		}
		return c, nil
	}
	for len(entries) > 0 {
		// Find the "smallest" FileMetadata.
		minIndex := 0
		for i := 1; i < len(entries); i++ {
			c, err := compare(entries[i].value, entries[minIndex].value)
			if err != nil {
				return nil, err
			}
			if c < 0 {
				minIndex = i
			}
		}
		// Write the "smallest" value.
		if err := sorted.Add(entries[minIndex].value); err != nil {
			return nil, WrapErrorf(err, "failed to write to target file")
		}
		// Read next value from the same chunk.
		chunkIdx := entries[minIndex].chunkIndex
		value, err := UnmarshalRevisionEntry(readers[chunkIdx])
		if err != nil {
			if errors.Is(err, io.EOF) {
				entries = slices.Delete(entries, minIndex, minIndex+1)
				continue
			}
			return nil, WrapErrorf(err, "failed to read next from chunk %d", chunkIdx)
		}
		entries[minIndex] = &entry{value, chunkIdx}
	}
	if err := sorted.rotateChunk(); err != nil {
		return nil, WrapErrorf(err, "failed to rotate final chunk")
	}
	// Close and delete all chunk files.
	for i := range rtw.chunks {
		if err := readerFiles[i].Close(); err != nil {
			return nil, WrapErrorf(err, "failed to close chunk file")
		}
		if err := rtw.fs.Remove(rtw.chunkFilename(i)); err != nil {
			return nil, WrapErrorf(err, "failed to remove chunk file")
		}
	}
	return &RevisionTemp{rtw.RevisionId, sorted.fs, sorted.chunks}, nil
}

func (rtw *RevisionTempWriter) chunkFilename(index int) string {
	return fmt.Sprintf("%d.%s", index, rtw.fileExt)
}

// Sort the current chunk and write it to disk.
func (rtw *RevisionTempWriter) rotateChunk() error {
	if len(rtw.chunk) == 0 {
		return nil
	}
	var err error
	slices.SortFunc(rtw.chunk, func(a, b *RevisionEntry) int {
		c := RevisionEntryPathCompare(a, b)
		if c == 0 {
			err = Errorf("duplicate revision entry path: %s", a.Path)
		}
		return c
	})
	if err != nil {
		return err
	}
	// todo: encrypt the data before writing to disk.
	file, err := rtw.fs.OpenWrite(rtw.chunkFilename(rtw.chunks))
	if err != nil {
		return WrapErrorf(err, "failed to open chunk file")
	}
	defer file.Close() //nolint:errcheck
	w := bufio.NewWriter(file)
	for _, entry := range rtw.chunk {
		if err := MarshalRevisionEntry(entry, w); err != nil {
			return WrapErrorf(err, "failed to write to chunk file")
		}
	}
	if err := w.Flush(); err != nil {
		return WrapErrorf(err, "failed to flush chunk file")
	}
	if err := file.Close(); err != nil {
		return WrapErrorf(err, "failed to close chunk file")
	}
	rtw.chunk = nil
	rtw.chunkSize = 0
	rtw.chunks += 1
	return nil
}

type RevisionTempCache struct {
	Source           *RevisionTemp
	maxChunksInCache int
	reader           *RevisionTempReader
	cache            []map[string]*RevisionEntry
	firstEntries     []*RevisionEntry
	lastAccessed     []int64
	chunksInCache    int
	CacheMisses      int
}

func NewRevisionTempCache(temp *RevisionTemp, maxChunksInCache int) (*RevisionTempCache, error) {
	firstEntries := make([]*RevisionEntry, temp.Chunks())
	reader := temp.Reader(nil)
	for i := range temp.Chunks() {
		f, err := reader.fs.OpenRead(reader.chunkFilename(i))
		if err != nil {
			return nil, WrapErrorf(err, "failed to open chunk file %d", i)
		}
		defer f.Close() //nolint:errcheck
		first, err := UnmarshalRevisionEntry(f)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read chunk file %d", i)
		}
		firstEntries[i] = first
	}
	return &RevisionTempCache{
		Source:           temp,
		maxChunksInCache: maxChunksInCache,
		reader:           reader,
		cache:            make([]map[string]*RevisionEntry, temp.Chunks()),
		firstEntries:     firstEntries,
		lastAccessed:     make([]int64, temp.Chunks()),
		CacheMisses:      0,
		chunksInCache:    0,
	}, nil
}

func (rtc *RevisionTempCache) Get(path Path, isDir bool) (*RevisionEntry, bool, error) {
	entry := &RevisionEntry{Path: path, Type: RevisionEntryAdd, Metadata: &FileMetadata{}} //nolint:exhaustruct
	if isDir {
		entry.Metadata.ModeAndPerm = ModeDir
	}
	// Find the chunk that contains the entry.
	chunkIndex := rtc.Source.Chunks() - 1
	for i, firstEntry := range rtc.firstEntries {
		c := RevisionEntryPathCompare(entry, firstEntry)
		if c < 0 {
			if i == 0 {
				return nil, false, nil
			}
			chunkIndex = i - 1
			break
		}
	}
	if chunkIndex < 0 {
		return nil, false, nil
	}
	cache := rtc.cache[chunkIndex]
	if cache == nil {
		if rtc.chunksInCache >= rtc.maxChunksInCache {
			// Evict the oldest chunk.
			oldest := -1
			for i, lastAccessed := range rtc.lastAccessed {
				if oldest < 0 || lastAccessed < rtc.lastAccessed[oldest] {
					oldest = i
				}
			}
			rtc.cache[oldest] = nil
		} else {
			rtc.chunksInCache++
		}
		cache = make(map[string]*RevisionEntry)
		rtc.cache[chunkIndex] = cache
		rtc.CacheMisses++
		entries, err := rtc.reader.ReadChunk(chunkIndex)
		if err != nil {
			return nil, false, WrapErrorf(err, "failed to read chunk %d", chunkIndex)
		}
		for _, entry := range entries {
			cache[entry.Path.String()] = entry
		}
	}
	rtc.lastAccessed[chunkIndex] = time.Now().UnixNano()
	re, ok := cache[entry.Path.String()]
	return re, ok, nil
}
