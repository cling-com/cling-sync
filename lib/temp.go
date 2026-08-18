// A sorted, chunked, on-disk temporary storage of entries.
package lib

import (
	"errors"
	"fmt"
	"io"
	"slices"
)

const DefaultTempChunkSize = 4 * 1024 * 1024

// Returned by `TempWriter.Add` and `TempWriter.CloseAndSort` when a writer that
// does not ignore duplicates sees the same entry twice.
var ErrDuplicateTempEntry = Errorf("duplicate entry")

// Splitting a sorted chunk into this many frames.
const framesPerChunk = 16

// Worst-case `TempFrame` envelope overhead (tag + varint length) per frame
// times `framesPerChunk`. Reserved against `maxChunkSize` so the on-disk
// chunk file stays under budget even after framing.
const chunkFramingOverhead = framesPerChunk * 8

// Marshallable is the proto-message contract: serialize to a writer and
// report the size of what was written.
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
//
// `EntrySize` returns the wire size of one entry as written by `MarshallAll`
// — used by `TempWriter` for chunk-budget accounting. It belongs on the
// marshaller (not on `T`) because the per-entry framing depends on the
// chunk format, not just on `T`.
type chunkMarshaller[T any] interface {
	MarshallAll(entries []T, w ProtobufWriter) error
	UnmarshallAll(r *ProtobufReader) ([]T, error)
	EntrySize(entry T) int
}

// A chunkMarshaller for any 32 byte value.
type bytes32ChunkMarshaller[T ~[32]byte] struct{}

const bytes32Size = 32

func (bytes32ChunkMarshaller[T]) MarshallAll(entries []T, w ProtobufWriter) error {
	for _, entry := range entries {
		if err := w.WriteBytes(1, entry[:]); err != nil {
			return WrapErrorf(err, "failed to write entry")
		}
	}
	return nil
}

func (bytes32ChunkMarshaller[T]) UnmarshallAll(r *ProtobufReader) ([]T, error) {
	var entries []T
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, WrapErrorf(err, "failed to read entry tag")
		}
		if tag != 1 || wireType != 2 {
			return nil, Errorf("unexpected tag %d / wire type %d for entry", tag, wireType)
		}
		b, err := r.ReadBytes()
		if err != nil {
			return nil, WrapErrorf(err, "failed to read entry")
		}
		if len(b) != bytes32Size {
			return nil, Errorf("entry must have length %d, got %d", bytes32Size, len(b))
		}
		entries = append(entries, T(b))
	}
	return entries, nil
}

func (bytes32ChunkMarshaller[T]) EntrySize(_ T) int {
	return TagLen(1, 2) + VarintLen(bytes32Size) + bytes32Size
}

type Temp[T any] struct {
	fs         FS
	chunks     int
	marshaller chunkMarshaller[T]
}

func OpenTemp[T any](fs FS, marshaller chunkMarshaller[T]) (*Temp[T], error) {
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

type TempReader[T any] struct {
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
			entries, err := tr.ReadChunk(tr.chunkIndex, buf)
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

func (tr *TempReader[T]) ReadChunk(i int, buf BlockBuf) ([]T, error) {
	if i < 0 || i >= tr.chunks {
		return nil, Errorf("chunk index out of range")
	}
	fr, err := newFrameReader(tr.fs, sortedChunkFilename(i), tr.marshaller, buf)
	if err != nil {
		return nil, err
	}
	defer fr.Close() //nolint:errcheck
	var entries []T
	for {
		e, err := fr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if tr.filter == nil || tr.filter(e) {
			entries = append(entries, e)
		}
	}
	return entries, nil
}

// The name a chunk file has once it is part of a `Temp`.
func sortedChunkFilename(index int) string {
	return fmt.Sprintf("%d.sorted", index)
}

type TempWriter[T any] struct {
	fs               FS
	chunk            []T
	chunkSize        int
	maxChunkSize     int
	chunks           int
	fileExt          string
	compare          func(a, b T) int
	marshaller       chunkMarshaller[T]
	ignoreDuplicates bool
	// Lazily allocated on first rotateChunk and reused for every frame.
	frameBuf BlockBuf
}

// Create a new TempWriter.
// Parameters:
//   - compare: A function that compares two entries. Two entries must never be
//     equal — use NewTempWriterWithIgnoreDuplicates to silently drop duplicates.
//   - marshaller: Serializes a sorted batch of entries to a chunk file.
func NewTempWriter[T any](
	compare func(a, b T) int,
	marshaller chunkMarshaller[T],
	fs FS,
	maxChunkSize int,
) *TempWriter[T] {
	return &TempWriter[T]{ //nolint:exhaustruct
		fs:           fs,
		maxChunkSize: maxChunkSize,
		fileExt:      "raw",
		compare:      compare,
		marshaller:   marshaller,
	}
}

// Like NewTempWriter, but duplicate entries (compare == 0) are silently
// dropped in rotateChunk and the CloseAndSort k-way merge instead of erroring.
func NewTempWriterWithIgnoreDuplicates[T any](
	compare func(a, b T) int,
	marshaller chunkMarshaller[T],
	fs FS,
	maxChunkSize int,
) *TempWriter[T] {
	tw := NewTempWriter(compare, marshaller, fs, maxChunkSize)
	tw.ignoreDuplicates = true
	return tw
}

func (tw *TempWriter[T]) Add(t T) error {
	size := tw.marshaller.EntrySize(t)
	budget := tw.maxChunkSize - chunkFramingOverhead
	if tw.chunkSize > 0 && tw.chunkSize+size > budget {
		if err := tw.rotateChunk(); err != nil {
			return err
		}
	}
	tw.chunk = append(tw.chunk, t)
	tw.chunkSize += size
	return nil
}

// Close the writer and return the chunks as they are, for a caller that added
// its entries in order. Nothing is sorted or merged, so the caller has to be
// sure of that.
func (tw *TempWriter[T]) CloseWithoutSort() (*Temp[T], error) {
	if err := tw.rotateChunk(); err != nil {
		return nil, WrapErrorf(err, "failed to rotate final chunk")
	}
	for i := range tw.chunks {
		if err := tw.fs.Rename(tw.chunkFilename(i), sortedChunkFilename(i)); err != nil {
			return nil, WrapErrorf(err, "failed to rename chunk file %d", i)
		}
	}
	return &Temp[T]{tw.fs, tw.chunks, tw.marshaller}, nil
}

// Close the writer, sorting and merging every chunk into the result.
func (tw *TempWriter[T]) CloseAndSort() (*Temp[T], error) { //nolint:funlen
	if err := tw.rotateChunk(); err != nil {
		return nil, WrapErrorf(err, "failed to rotate final chunk")
	}
	sorted := NewTempWriter(tw.compare, tw.marshaller, tw.fs, tw.maxChunkSize)
	sorted.fileExt = "sorted"
	readers := make([]*frameReader[T], 0, tw.chunks)
	heads := make([]T, 0, tw.chunks)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	for i := range tw.chunks {
		r, err := newFrameReader(tw.fs, tw.chunkFilename(i), tw.marshaller, NewBlockBuf())
		if err != nil {
			return nil, err
		}
		e, err := r.Read()
		if errors.Is(err, io.EOF) {
			_ = r.Close()
			continue
		}
		if err != nil {
			_ = r.Close()
			return nil, err
		}
		readers = append(readers, r)
		heads = append(heads, e)
	}
	queue := NewHeap(func(a, b int) int { return tw.compare(heads[a], heads[b]) }, len(readers))
	for i := range readers {
		queue.Push(i)
	}
	// Refill the queue's smallest chunk from its own reader. An exhausted chunk
	// leaves the queue and is closed by the deferred cleanup.
	advance := func() error {
		i := queue.Pop()
		e, err := readers[i].Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		heads[i] = e
		queue.Push(i)
		return nil
	}
	for queue.Len() > 0 {
		minHead := heads[queue.Peek()]
		if err := sorted.Add(minHead); err != nil {
			return nil, WrapErrorf(err, "failed to write to target file")
		}
		if err := advance(); err != nil {
			return nil, err
		}
		// Equal entries sort next to each other, so a duplicate of `minHead` is
		// now at the top of the queue.
		for queue.Len() > 0 && tw.compare(heads[queue.Peek()], minHead) == 0 {
			if !tw.ignoreDuplicates {
				return nil, WrapErrorf(ErrDuplicateTempEntry, "duplicate entry: %v", minHead)
			}
			if err := advance(); err != nil {
				return nil, err
			}
		}
	}
	if err := sorted.rotateChunk(); err != nil {
		return nil, WrapErrorf(err, "failed to rotate final chunk")
	}
	for i := range tw.chunks {
		if err := tw.fs.Remove(tw.chunkFilename(i)); err != nil {
			return nil, WrapErrorf(err, "failed to remove chunk file")
		}
	}
	return &Temp[T]{sorted.fs, sorted.chunks, sorted.marshaller}, nil
}

func (tw *TempWriter[T]) rotateChunk() error {
	if len(tw.chunk) == 0 {
		return nil
	}
	var sortErr error
	slices.SortFunc(tw.chunk, func(a, b T) int {
		c := tw.compare(a, b)
		if c == 0 && !tw.ignoreDuplicates {
			sortErr = WrapErrorf(ErrDuplicateTempEntry, "duplicate entry: %v", a)
		}
		return c
	})
	if sortErr != nil {
		return sortErr
	}
	if tw.ignoreDuplicates {
		// Compact adjacent duplicates in the now-sorted chunk.
		w := 1
		for i := 1; i < len(tw.chunk); i++ {
			if tw.compare(tw.chunk[i], tw.chunk[w-1]) == 0 {
				continue
			}
			tw.chunk[w] = tw.chunk[i]
			w++
		}
		tw.chunk = tw.chunk[:w]
	}
	f, err := tw.fs.OpenWrite(tw.chunkFilename(tw.chunks))
	if err != nil {
		return WrapErrorf(err, "failed to open chunk file")
	}
	if err := tw.writeFrames(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return WrapErrorf(err, "failed to close chunk file")
	}
	tw.chunk = nil
	tw.chunkSize = 0
	tw.chunks++
	return nil
}

// Frames are contiguous windows of the already-sorted `tw.chunk`, written
// in order, so reading them sequentially produces a globally sorted stream.
func (tw *TempWriter[T]) writeFrames(w io.Writer) error {
	if tw.frameBuf.buf == nil {
		tw.frameBuf = NewBlockBuf()
	}
	entriesPerFrame := max((len(tw.chunk)+framesPerChunk-1)/framesPerChunk, 1)
	var envelopeScratch [11]byte
	for start := 0; start < len(tw.chunk); start += entriesPerFrame {
		end := min(start+entriesPerFrame, len(tw.chunk))
		slice := tw.chunk[start:end]
		pw := NewProtobufWriter(tw.frameBuf.Bytes())
		if err := tw.marshaller.MarshallAll(slice, pw); err != nil {
			return WrapErrorf(err, "failed to marshall frame")
		}
		// Marshall the frame "by hand" so we don't have to allocate a
		// buffer.
		data := pw.Bytes()
		ew := NewProtobufWriter(envelopeScratch[:])
		if err := ew.WriteTag(1, 2); err != nil {
			return WrapErrorf(err, "failed to write frame tag")
		}
		if err := ew.WriteVarint(int64(len(data))); err != nil {
			return WrapErrorf(err, "failed to write frame length")
		}
		if _, err := w.Write(ew.Bytes()); err != nil {
			return WrapErrorf(err, "failed to write frame envelope")
		}
		if _, err := w.Write(data); err != nil {
			return WrapErrorf(err, "failed to write frame data")
		}
	}
	return nil
}

func (tw *TempWriter[T]) chunkFilename(index int) string {
	return fmt.Sprintf("%d.%s", index, tw.fileExt)
}

// `buf` must remain unused by the caller until Close() returns.
// The reader holds its bytes for the lifetime of the iteration.
type frameReader[T any] struct {
	closer     io.Closer
	pb         *ProtobufReader
	marshaller chunkMarshaller[T]
	current    []T
	cursor     int
}

func newFrameReader[T any](
	fs FS,
	name string,
	m chunkMarshaller[T],
	buf BlockBuf,
) (*frameReader[T], error) {
	f, err := fs.OpenRead(name)
	if err != nil {
		return nil, WrapErrorf(err, "failed to open chunk file %s", name)
	}
	data, err := buf.Read(f)
	if err != nil {
		_ = f.Close()
		return nil, WrapErrorf(err, "failed to read chunk file %s", name)
	}
	return &frameReader[T]{ //nolint:exhaustruct
		closer: f, pb: NewProtobufReader(data), marshaller: m,
	}, nil
}

// Read returns the next entry or io.EOF when the chunk file is exhausted.
func (r *frameReader[T]) Read() (T, error) {
	var zero T
	for r.cursor >= len(r.current) {
		if r.pb.AtEnd() {
			return zero, io.EOF
		}
		tag, wireType, err := r.pb.ReadTag()
		if err != nil {
			return zero, WrapErrorf(err, "failed to read frame tag")
		}
		if tag != 1 || wireType != 2 {
			return zero, Errorf("unexpected frame tag %d/wire %d", tag, wireType)
		}
		frameData, err := r.pb.ReadBytes()
		if err != nil {
			return zero, WrapErrorf(err, "failed to read frame data")
		}
		entries, err := r.marshaller.UnmarshallAll(NewProtobufReader(frameData))
		if err != nil {
			return zero, WrapErrorf(err, "failed to unmarshall frame")
		}
		r.current = entries
		r.cursor = 0
	}
	e := r.current[r.cursor]
	r.cursor++
	return e, nil
}

func (r *frameReader[T]) Close() error {
	return r.closer.Close() //nolint:wrapcheck
}

// `K` keys the per-chunk maps and orders the chunks, so it must be comparable
// and cheap to build. A string key would allocate on every lookup.
type TempCache[T any, K comparable] struct {
	Source           *Temp[T]
	maxChunksInCache int
	reader           *TempReader[T]
	buf              BlockBuf
	cache            []map[K]T
	firstEntries     []K
	// Access order, not wall clock: two lookups can land in the same nanosecond
	// and then the eviction picks whichever chunk it happens to scan first.
	lastAccessed        []int64
	lastAccessedCounter int64
	chunksInCache       int
	cacheKey            func(T) K
	compareKey          func(a, b K) int
	CacheMisses         int
}

func NewTempCache[T any, K comparable](
	temp *Temp[T],
	cacheKey func(T) K,
	compareKey func(a, b K) int,
	maxChunksInCache int,
) (*TempCache[T, K], error) {
	firstEntries := make([]K, temp.Chunks())
	cache := make([]map[K]T, temp.Chunks())
	chunksInCache := 0
	reader := temp.Reader(nil)
	buf := NewBlockBuf()
	for i := range temp.Chunks() {
		entries, err := reader.ReadChunk(i, buf)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read chunk file %d", i)
		}
		if len(entries) == 0 {
			return nil, Errorf("empty chunk file %d", i)
		}
		firstEntries[i] = cacheKey(entries[0])
	}
	return &TempCache[T, K]{
		Source:              temp,
		maxChunksInCache:    maxChunksInCache,
		reader:              reader,
		buf:                 buf,
		cache:               cache,
		firstEntries:        firstEntries,
		lastAccessed:        make([]int64, temp.Chunks()),
		lastAccessedCounter: 0,
		CacheMisses:         0,
		cacheKey:            cacheKey,
		compareKey:          compareKey,
		chunksInCache:       chunksInCache,
	}, nil
}

func (tc *TempCache[T, K]) Get(key K) (T, bool, error) {
	var zero T
	if tc == nil {
		return zero, false, nil
	}
	// Find the chunk that contains the entry.
	chunkIndex := tc.Source.Chunks() - 1
	for i, firstEntry := range tc.firstEntries {
		c := tc.compareKey(key, firstEntry)
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
		cache = make(map[K]T)
		tc.cache[chunkIndex] = cache
		tc.CacheMisses++
		// Cached entries outlive the reads of later chunks into the same
		// `tc.buf`, so an entry must copy its fields instead of pointing into
		// it, pinned by `TestRevisionEntry/Unmarshalling does not alias the buffer`.
		entries, err := tc.reader.ReadChunk(chunkIndex, tc.buf)
		if err != nil {
			return zero, false, WrapErrorf(err, "failed to read chunk %d", chunkIndex)
		}
		for _, entry := range entries {
			cache[tc.cacheKey(entry)] = entry
		}
	}
	tc.lastAccessedCounter++
	tc.lastAccessed[chunkIndex] = tc.lastAccessedCounter
	re, ok := cache[key]
	return re, ok, nil
}
