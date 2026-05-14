// A sorted, chunked, encrypted temporary storage of entries.
package lib

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

const DefaultTempChunkSize = 4 * 1024 * 1024

// chunkMarshallingOverhead is the slack a chunk's marshal buffer needs on
// top of the running budget (10 bytes per ProtobufBytesWriter.WriteMessage
// nesting level — our deepest is RevisionEntryChunk → RevisionEntry →
// PathMetadata → Timestamp, plus a little headroom).
const chunkMarshallingOverhead = 64

// Marshallable is the constraint for entry types stored by TempWriter/Temp.
type Marshallable interface {
	Marshall(ProtobufWriter) error
	MarshallSize() int
}

// chunkMarshaller is the per-entry-type serializer for whole chunks. It is
// intentionally unexported — implementations live in the package that owns
// the entry type (e.g. `revisionEntryChunkMarshaller` in revisionentry.go,
// `stagingEntryChunkMarshaller` in workspace) and are passed by value into
// NewTempWriter / OpenTemp / NewRevisionReader. Go's structural interfaces
// let external packages satisfy this without naming the type.
type chunkMarshaller[T any] interface {
	MarshallAll(entries []T, w ProtobufWriter) error
	UnmarshallAll(r *ProtobufReader) ([]T, error)
}

type Temp[T Marshallable] struct {
	fs         FS
	chunks     int
	marshaller chunkMarshaller[T]
}

func OpenTemp[T Marshallable](fs FS, marshaller chunkMarshaller[T]) (*Temp[T], error) {
	chunks, err := fs.ReadDir(".")
	if err != nil {
		return nil, WrapErrorf(err, "failed to read temp files")
	}
	return &Temp[T]{fs, len(chunks), marshaller}, nil
}

func (t *Temp[T]) Chunks() int {
	return t.chunks
}

func (t *Temp[T]) Reader(filter func(T) bool) *TempReader[T] {
	return &TempReader[T]{
		fs:           t.fs,
		chunks:       t.chunks,
		chunkIndex:   0,
		current:      nil,
		currentIndex: 0,
		filter:       filter,
		marshaller:   t.marshaller,
	}
}

func (t *Temp[T]) Remove() error {
	if err := t.fs.RemoveAll("."); err != nil {
		return WrapErrorf(err, "failed to remove temporary fs %s", t.fs)
	}
	return nil
}

type TempReader[T Marshallable] struct {
	fs           FS
	chunks       int
	chunkIndex   int
	current      []T
	currentIndex int
	filter       func(T) bool
	marshaller   chunkMarshaller[T]
}

func (tr *TempReader[T]) Read(buf BlockBuf) (T, error) {
	var zero T
	for {
		if tr.current == nil || tr.currentIndex == len(tr.current) {
			if tr.chunkIndex == tr.chunks {
				return zero, io.EOF
			}
			entries, err := tr.ReadChunk(tr.chunkIndex)
			if err != nil {
				return zero, err
			}
			tr.current = entries
			tr.currentIndex = 0
			tr.chunkIndex++
			if len(entries) == 0 {
				// All entries have been filtered out.
				continue
			}
		}
		entry := tr.current[tr.currentIndex]
		tr.currentIndex++
		if tr.filter == nil || tr.filter(entry) {
			return entry, nil
		}
	}
}

func (tr *TempReader[T]) ReadChunk(i int) ([]T, error) {
	if i < 0 || i >= tr.chunks {
		return nil, Errorf("chunk index out of range")
	}
	data, err := tr.ReadChunkRaw(i)
	if err != nil {
		return nil, err
	}
	entries, err := tr.marshaller.UnmarshallAll(NewProtobufReader(data))
	if err != nil {
		return nil, WrapErrorf(err, "failed to unmarshall chunk %d", i)
	}
	if tr.filter == nil {
		return entries, nil
	}
	filtered := entries[:0]
	for _, e := range entries {
		if tr.filter(e) {
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

// ReadChunkRaw returns the chunk's raw on-disk bytes (a marshalled chunk
// message — same wire format as a repository revision block).
func (tr *TempReader[T]) ReadChunkRaw(i int) ([]byte, error) {
	if i < 0 || i >= tr.chunks {
		return nil, Errorf("chunk index out of range")
	}
	data, err := ReadFile(tr.fs, tr.chunkFilename(i))
	if err != nil {
		return nil, WrapErrorf(err, "failed to read chunk file %d", i)
	}
	return data, nil
}

func (tr *TempReader[T]) chunkFilename(index int) string {
	return fmt.Sprintf("%d.sorted", index)
}

// marshallSize returns the size of the marshalled representation of `t`
// including their protobuf overhead when used in a slice (tag + len).
func marshallSize[T Marshallable](t T) int {
	n := t.MarshallSize()
	return TagLen(1, 2) + VarintLen(int64(n)) + n
}

type TempWriter[T Marshallable] struct {
	fs           FS
	chunk        []T
	chunkSize    int
	maxChunkSize int
	chunks       int
	fileExt      string
	compare      func(a, b T) int
	marshaller   chunkMarshaller[T]
}

// Create a new TempWriter.
// Parameters:
// - compare: A function that compares two entries. Two entries must never be equal.
// - marshaller: Serializes a sorted batch of entries to a chunk file.
func NewTempWriter[T Marshallable](
	compare func(a, b T) int,
	marshaller chunkMarshaller[T],
	fs FS,
	maxChunkSize int,
) *TempWriter[T] {
	return &TempWriter[T]{fs, nil, 0, maxChunkSize, 0, "raw", compare, marshaller}
}

func (tw *TempWriter[T]) Add(t T) error {
	size := marshallSize(t)
	if tw.chunkSize > 0 && tw.chunkSize+size > tw.maxChunkSize {
		if err := tw.rotateChunk(); err != nil {
			return err
		}
	}
	tw.chunk = append(tw.chunk, t)
	tw.chunkSize += size
	return nil
}

// Rotate the current chunk and then sort all chunks and return the merged result.
func (tw *TempWriter[T]) Finalize() (*Temp[T], error) {
	if err := tw.rotateChunk(); err != nil {
		return nil, WrapErrorf(err, "failed to rotate final chunk")
	}
	sorted := NewTempWriter(tw.compare, tw.marshaller, tw.fs, tw.maxChunkSize)
	sorted.fileExt = "sorted"
	// Load each input chunk fully (chunks are bounded by maxChunkSize).
	chunks := make([][]T, tw.chunks)
	for i := range tw.chunks {
		data, err := ReadFile(tw.fs, tw.chunkFilename(i))
		if err != nil {
			return nil, WrapErrorf(err, "failed to read chunk file %d", i)
		}
		entries, err := tw.marshaller.UnmarshallAll(NewProtobufReader(data))
		if err != nil {
			return nil, WrapErrorf(err, "failed to unmarshall chunk file %d", i)
		}
		chunks[i] = entries
	}
	// k-way merge.
	cursors := make([]int, tw.chunks)
	for {
		minChunk := -1
		for i, idx := range cursors {
			if idx >= len(chunks[i]) {
				continue
			}
			if minChunk == -1 {
				minChunk = i
				continue
			}
			c := tw.compare(chunks[i][idx], chunks[minChunk][cursors[minChunk]])
			if c == 0 {
				return nil, Errorf("duplicate entry: %v", chunks[i][idx])
			}
			if c < 0 {
				minChunk = i
			}
		}
		if minChunk == -1 {
			break
		}
		if err := sorted.Add(chunks[minChunk][cursors[minChunk]]); err != nil {
			return nil, WrapErrorf(err, "failed to write to target file")
		}
		cursors[minChunk]++
	}
	if err := sorted.rotateChunk(); err != nil {
		return nil, WrapErrorf(err, "failed to rotate final chunk")
	}
	// Delete all input chunk files.
	for i := range tw.chunks {
		if err := tw.fs.Remove(tw.chunkFilename(i)); err != nil {
			return nil, WrapErrorf(err, "failed to remove chunk file")
		}
	}
	return &Temp[T]{sorted.fs, sorted.chunks, sorted.marshaller}, nil
}

// Sort the current chunk and serialize it as a typed chunk message.
func (tw *TempWriter[T]) rotateChunk() error {
	if len(tw.chunk) == 0 {
		return nil
	}
	var sortErr error
	slices.SortFunc(tw.chunk, func(a, b T) int {
		c := tw.compare(a, b)
		if c == 0 {
			sortErr = Errorf("duplicate entry: %v", a)
		}
		return c
	})
	if sortErr != nil {
		return sortErr
	}
	buf := make([]byte, tw.chunkSize+chunkMarshallingOverhead)
	pw := NewProtobufWriter(buf)
	if err := tw.marshaller.MarshallAll(tw.chunk, pw); err != nil {
		return WrapErrorf(err, "failed to marshall chunk")
	}
	if err := WriteFile(tw.fs, tw.chunkFilename(tw.chunks), pw.Bytes()); err != nil {
		return WrapErrorf(err, "failed to write chunk file")
	}
	tw.chunk = nil
	tw.chunkSize = 0
	tw.chunks += 1
	return nil
}

func (tw *TempWriter[T]) chunkFilename(index int) string {
	return fmt.Sprintf("%d.%s", index, tw.fileExt)
}

type TempCache[T Marshallable] struct {
	Source           *Temp[T]
	maxChunksInCache int
	reader           *TempReader[T]
	cache            []map[string]T
	firstEntries     []string
	lastAccessed     []int64
	chunksInCache    int
	cacheKey         func(T) string
	CacheMisses      int
}

func NewTempCache[T Marshallable](
	temp *Temp[T],
	cacheKey func(T) string,
	maxChunksInCache int,
) (*TempCache[T], error) {
	firstEntries := make([]string, temp.Chunks())
	cache := make([]map[string]T, temp.Chunks())
	chunksInCache := 0
	reader := temp.Reader(nil)
	for i := range temp.Chunks() {
		entries, err := reader.ReadChunk(i)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read chunk file %d", i)
		}
		if len(entries) == 0 {
			return nil, Errorf("empty chunk file %d", i)
		}
		firstEntries[i] = cacheKey(entries[0])
	}
	return &TempCache[T]{
		Source:           temp,
		maxChunksInCache: maxChunksInCache,
		reader:           reader,
		cache:            cache,
		firstEntries:     firstEntries,
		lastAccessed:     make([]int64, temp.Chunks()),
		CacheMisses:      0,
		cacheKey:         cacheKey,
		chunksInCache:    chunksInCache,
	}, nil
}

func (tc *TempCache[T]) Get(key string) (T, bool, error) {
	var zero T
	if tc == nil {
		return zero, false, nil
	}
	// Find the chunk that contains the entry.
	chunkIndex := tc.Source.Chunks() - 1
	for i, firstEntry := range tc.firstEntries {
		c := strings.Compare(key, firstEntry)
		if c < 0 {
			if i == 0 {
				return zero, false, nil
			}
			chunkIndex = i - 1
			break
		}
	}
	if chunkIndex < 0 {
		return zero, false, nil
	}
	cache := tc.cache[chunkIndex]
	if cache == nil {
		if tc.chunksInCache >= tc.maxChunksInCache {
			// Evict the oldest chunk.
			oldest := -1
			for i, lastAccessed := range tc.lastAccessed {
				if tc.cache[i] == nil {
					continue
				}
				if oldest < 0 || lastAccessed < tc.lastAccessed[oldest] {
					oldest = i
				}
			}
			tc.cache[oldest] = nil
		} else {
			tc.chunksInCache++
		}
		cache = make(map[string]T)
		tc.cache[chunkIndex] = cache
		tc.CacheMisses++
		entries, err := tc.reader.ReadChunk(chunkIndex)
		if err != nil {
			return zero, false, WrapErrorf(err, "failed to read chunk %d", chunkIndex)
		}
		for _, entry := range entries {
			cache[tc.cacheKey(entry)] = entry
		}
	}
	tc.lastAccessed[chunkIndex] = time.Now().UnixNano()
	re, ok := cache[key]
	return re, ok, nil
}
