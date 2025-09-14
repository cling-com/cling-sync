// A sorted, chunked, encrypted temporary storage of entries.
package lib

import (
	"bytes"
	cryptoCipher "crypto/cipher"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

const DefaultTempChunkSize = 4 * 1024 * 1024

type Temp[T any] struct {
	fs        FS
	chunks    int
	cipher    cryptoCipher.AEAD
	unmarshal func(r io.Reader) (*T, error)
}

func (t *Temp[T]) Chunks() int {
	return t.chunks
}

func (t *Temp[T]) Reader(filter func(*T) bool) *TempReader[T] {
	return &TempReader[T]{
		fs:           t.fs,
		chunks:       t.chunks,
		chunkIndex:   0,
		current:      nil,
		currentIndex: 0,
		filter:       filter,
		cipher:       t.cipher,
		unmarshal:    t.unmarshal,
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
	current      []*T
	currentIndex int
	cipher       cryptoCipher.AEAD
	filter       func(*T) bool
	unmarshal    func(r io.Reader) (*T, error)
}

func (tr *TempReader[T]) Read() (*T, error) {
	for {
		if tr.current == nil || tr.currentIndex == len(tr.current) {
			if tr.chunkIndex == tr.chunks {
				return nil, io.EOF
			}
			entries, err := tr.ReadChunk(tr.chunkIndex)
			if err != nil {
				return nil, err
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

func (tr *TempReader[T]) ReadChunk(i int) ([]*T, error) {
	if i < 0 || i >= tr.chunks {
		return nil, Errorf("chunk index out of range")
	}
	data, err := tr.ReadChunkRaw(i)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data)
	entries := []*T{}
	for {
		re, err := tr.unmarshal(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, WrapErrorf(err, "failed to unmarshal entry from chunk file %d", i)
		}
		if tr.filter != nil && !tr.filter(re) {
			continue
		}
		entries = append(entries, re)
	}
	return entries, nil
}

// This ignores the `filter` and reads all entries (obviously).
func (tr *TempReader[T]) ReadChunkRaw(i int) ([]byte, error) {
	if i < 0 || i >= tr.chunks {
		return nil, Errorf("chunk index out of range")
	}
	encrypted, err := ReadFile(tr.fs, tr.chunkFilename(i))
	if err != nil {
		return nil, WrapErrorf(err, "failed to read chunk file %d", i)
	}
	data, err := Decrypt(encrypted, tr.cipher, nil, make([]byte, len(encrypted)-TotalCipherOverhead))
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt chunk file %d", i)
	}
	return data, nil
}

func (tr *TempReader[T]) chunkFilename(index int) string {
	return fmt.Sprintf("%d.sorted", index)
}

type TempWriter[T any] struct {
	fs             FS
	chunk          []*T
	chunkSize      int
	maxChunkSize   int
	chunks         int
	cipher         cryptoCipher.AEAD
	fileExt        string
	compare        func(a, b *T) int
	marshal        func(t *T, w io.Writer) error
	marshalledSize func(t *T) int
	unmarshal      func(r io.Reader) (*T, error)
}

// Create a new TempWriter.
// Parameters:
// - compare: A function that compares two entries. Two entries must never be equal.
func NewTempWriter[T any](
	compare func(a, b *T) int,
	marshal func(t *T, w io.Writer) error,
	marshalledSize func(t *T) int,
	unmarshal func(r io.Reader) (*T, error),
	fs FS,
	maxChunkSize int,
) (*TempWriter[T], error) {
	key, err := NewRawKey()
	if err != nil {
		return nil, WrapErrorf(err, "failed to generate random key for encryption")
	}
	cipher, err := NewCipher(key)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create a cipher from key for encryption")
	}
	return &TempWriter[T]{fs, nil, 0, maxChunkSize, 0, cipher, "raw", compare, marshal, marshalledSize, unmarshal}, nil
}

func (tw *TempWriter[T]) Add(t *T) error {
	size := tw.marshalledSize(t)
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
func (tw *TempWriter[T]) Finalize() (*Temp[T], error) { //nolint:funlen
	if err := tw.rotateChunk(); err != nil {
		return nil, WrapErrorf(err, "failed to rotate final chunk")
	}
	// Create a new RevisionTempWriter to store the sorted chunks.
	sorted, err := NewTempWriter(tw.compare, tw.marshal, tw.marshalledSize, tw.unmarshal, tw.fs, tw.maxChunkSize)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create new TempWriter")
	}
	sorted.fileExt = "sorted"
	readers := make([]io.Reader, tw.chunks)
	for i := range tw.chunks {
		data, err := ReadFile(tw.fs, tw.chunkFilename(i))
		if err != nil {
			return nil, WrapErrorf(err, "failed to read chunk file")
		}
		decrypted, err := Decrypt(data, tw.cipher, nil, make([]byte, len(data)-TotalCipherOverhead))
		if err != nil {
			return nil, WrapErrorf(err, "failed to decrypt chunk file")
		}
		readers[i] = bytes.NewReader(decrypted)
	}
	type entry struct {
		value      *T
		chunkIndex int
	}
	// First, read the first entry of each file.
	entries := make([]*entry, 0, len(readers))
	for i, r := range readers {
		// todo(perf): We should not need to unmarshal and the marshal all entries.
		value, err := tw.unmarshal(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return nil, WrapErrorf(err, "failed to read from chunk %d", i)
		}
		entries = append(entries, &entry{value, i})
	}
	for len(entries) > 0 {
		// Find the "smallest" entry.
		minIndex := 0
		for i := 1; i < len(entries); i++ {
			c := tw.compare(entries[i].value, entries[minIndex].value)
			if c == 0 {
				return nil, Errorf("duplicate entry: %s", entries[i].value)
			}
			if c < 0 {
				minIndex = i
			}
		}
		// Write the "smallest" entry..
		if err := sorted.Add(entries[minIndex].value); err != nil {
			return nil, WrapErrorf(err, "failed to write to target file")
		}
		// Read next entry from the same chunk.
		chunkIdx := entries[minIndex].chunkIndex
		value, err := tw.unmarshal(readers[chunkIdx])
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
	// Delete all chunk files.
	for i := range tw.chunks {
		if err := tw.fs.Remove(tw.chunkFilename(i)); err != nil {
			return nil, WrapErrorf(err, "failed to remove chunk file")
		}
	}
	return &Temp[T]{sorted.fs, sorted.chunks, sorted.cipher, sorted.unmarshal}, nil
}

// Sort the current chunk, encrypt it, and write it to disk.
func (tw *TempWriter[T]) rotateChunk() error {
	if len(tw.chunk) == 0 {
		return nil
	}
	var err error
	slices.SortFunc(tw.chunk, func(a, b *T) int {
		c := tw.compare(a, b)
		if c == 0 {
			err = Errorf("duplicate entry: %s", a)
		}
		return c
	})
	if err != nil {
		return err
	}
	file, err := tw.fs.OpenWrite(tw.chunkFilename(tw.chunks))
	if err != nil {
		return WrapErrorf(err, "failed to open chunk file")
	}
	defer file.Close() //nolint:errcheck
	buf := bytes.NewBuffer(nil)
	for _, entry := range tw.chunk {
		if err := tw.marshal(entry, buf); err != nil {
			return WrapErrorf(err, "failed to write to chunk file")
		}
	}
	encrypted, err := Encrypt(buf.Bytes(), tw.cipher, nil, make([]byte, len(buf.Bytes())+TotalCipherOverhead))
	if err != nil {
		return WrapErrorf(err, "failed to encrypt chunk")
	}
	if err := WriteFile(tw.fs, tw.chunkFilename(tw.chunks), encrypted); err != nil {
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

type TempCache[T any] struct {
	Source           *Temp[T]
	maxChunksInCache int
	reader           *TempReader[T]
	cache            []map[string]*T
	firstEntries     []string
	lastAccessed     []int64
	chunksInCache    int
	cacheKey         func(*T) string
	CacheMisses      int
}

func NewTempCache[T any](temp *Temp[T], cacheKey func(*T) string, maxChunksInCache int) (*TempCache[T], error) {
	firstEntries := make([]string, temp.Chunks())
	cache := make([]map[string]*T, temp.Chunks())
	chunksInCache := 0
	reader := temp.Reader(nil)
	for i := range temp.Chunks() {
		chunk, err := reader.ReadChunk(i)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read chunk file %d", i)
		}
		firstEntries[i] = cacheKey(chunk[0])
		if chunksInCache < maxChunksInCache {
			c := make(map[string]*T)
			cache[i] = c
			for _, entry := range chunk {
				c[cacheKey(entry)] = entry
			}
		}
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

func (tc *TempCache[T]) Get(key string) (*T, bool, error) {
	// Find the chunk that contains the entry.
	chunkIndex := tc.Source.Chunks() - 1
	for i, firstEntry := range tc.firstEntries {
		c := strings.Compare(key, firstEntry)
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
	cache := tc.cache[chunkIndex]
	if cache == nil {
		if tc.chunksInCache >= tc.maxChunksInCache {
			// Evict the oldest chunk.
			oldest := -1
			for i, lastAccessed := range tc.lastAccessed {
				if oldest < 0 || lastAccessed < tc.lastAccessed[oldest] {
					oldest = i
				}
			}
			tc.cache[oldest] = nil
		} else {
			tc.chunksInCache++
		}
		cache = make(map[string]*T)
		tc.cache[chunkIndex] = cache
		tc.CacheMisses++
		entries, err := tc.reader.ReadChunk(chunkIndex)
		if err != nil {
			return nil, false, WrapErrorf(err, "failed to read chunk %d", chunkIndex)
		}
		for _, entry := range entries {
			cache[tc.cacheKey(entry)] = entry
		}
	}
	tc.lastAccessed[chunkIndex] = time.Now().UnixNano()
	re, ok := cache[key]
	return re, ok, nil
}
