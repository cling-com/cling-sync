package lib

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const revisionMarshalMagick = "cling-rev"

type RevisionId BlockId

func (id RevisionId) String() string {
	return "rev:" + hex.EncodeToString(id[:])
}

func (id RevisionId) Long() string {
	return hex.EncodeToString(id[:])
}

func (id RevisionId) IsRoot() bool {
	return id == (RevisionId)(BlockId{})
}

type RevisionEntryType uint8

const (
	RevisionEntryAdd    RevisionEntryType = 0
	RevisionEntryUpdate RevisionEntryType = 1
	RevisionEntryDelete RevisionEntryType = 2
)

func (t RevisionEntryType) String() string {
	switch t {
	case RevisionEntryAdd:
		return "add"
	case RevisionEntryUpdate:
		return "update"
	case RevisionEntryDelete:
		return "delete"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

type Revision struct {
	TimestampSec  int64
	TimestampNSec int32
	Message       string
	Author        string
	Parent        RevisionId
	Blocks        []BlockId
}

func MarshalRevision(c *Revision, w io.Writer) error {
	if len(c.Blocks) == 0 {
		return Errorf("commit must have at least one block")
	}
	bw := NewBinaryWriter(w)
	// We use a "magic" value here to distinguish revision blocks
	// from data blocks and catch any accidental misuse.
	bw.WriteString(revisionMarshalMagick)
	bw.Write(c.TimestampSec)
	bw.Write(c.TimestampNSec)
	bw.WriteString(c.Message)
	bw.WriteString(c.Author)
	bw.Write(c.Parent)
	bw.WriteLen(len(c.Blocks))
	for _, blockId := range c.Blocks {
		bw.Write(blockId)
	}
	return bw.Err
}

func UnmarshalRevision(r io.Reader) (*Revision, error) {
	br := NewBinaryReader(r)
	var c Revision
	magick := br.ReadString()
	if br.Err == nil && magick != revisionMarshalMagick {
		return nil, Errorf("this is not a commit (invalid magick)")
	}
	br.Read(&c.TimestampSec)
	br.Read(&c.TimestampNSec)
	c.Message = br.ReadString()
	c.Author = br.ReadString()
	br.Read(&c.Parent)
	l := br.ReadLen()
	c.Blocks = make([]BlockId, l)
	for i := range l {
		br.Read(&c.Blocks[i])
	}
	return &c, br.Err
}

// Compare two revision entries by their full path.
//
// The sorting order is:
//   - directory
//   - files inside the directory
//   - sub-directory
//   - files inside the sub-directory
//   - ...
//
// Example:
//   - a.txt
//   - z.txt
//   - sub/
//   - sub/a.txt
//   - sub/z.txt
//   - sub/sub/
//   - sub/sub/a.txt
//   - sub/sub/z.txt
func RevisionEntryPathCompare(a, b *RevisionEntry) int {
	key := func(e *RevisionEntry) string {
		p := strings.ReplaceAll(string(e.Path), "/", "/1")
		if e.Metadata != nil && e.Metadata.ModeAndPerm.IsDir() {
			return p
		}
		lastSlash := strings.LastIndex(p, "/")
		if lastSlash == -1 || lastSlash == len(p)-1 {
			return "0" + p
		}
		return p[:lastSlash] + "/0" + p[lastSlash+2:]
	}
	return strings.Compare(key(a), key(b))
}

type RevisionEntry struct {
	Path     Path
	Type     RevisionEntryType
	Metadata *FileMetadata
}

func NewRevisionEntry(path Path, typ RevisionEntryType, md *FileMetadata) (RevisionEntry, error) {
	if typ == RevisionEntryDelete {
		if md != nil {
			return RevisionEntry{}, Errorf("cannot create delete revision with metadata")
		}
	} else if md == nil {
		return RevisionEntry{}, Errorf("cannot create add/update revision without metadata")
	}
	return RevisionEntry{Path: path, Type: typ, Metadata: md}, nil
}

func (se *RevisionEntry) EstimatedSize() int {
	size := len(se.Path) + 1
	if se.Metadata != nil {
		size += se.Metadata.EstimatedSize()
	}
	return size
}

func MarshalRevisionEntry(r *RevisionEntry, w io.Writer) error {
	bw := NewBinaryWriter(w)
	bw.WriteString(string(r.Path))
	bw.Write(r.Type)
	if r.Metadata != nil {
		if r.Type == RevisionEntryDelete {
			return Errorf("cannot marshal delete revision with metadata %s", r.Path)
		}
		return MarshalFileMetadata(r.Metadata, w)
	} else if r.Type != RevisionEntryDelete {
		return Errorf("cannot marshal add/update revision without metadata %s (%d)", r.Path, r.Type)
	}
	if bw.Err != nil {
		return WrapErrorf(bw.Err, "failed to marshal revision entry %s", r.Path)
	}
	return nil
}

// todo: All unmarshal functions should take a reference of an object to be filled.
// todo: Make sure to wrap all errors in marshal and unmarshal
func UnmarshalRevisionEntry(r io.Reader) (*RevisionEntry, error) {
	var re RevisionEntry
	br := NewBinaryReader(r)
	path := br.ReadString()
	re.Path = Path(path)
	br.Read(&re.Type)
	if re.Type != RevisionEntryDelete {
		metadata, err := UnmarshalFileMetadata(r)
		if err != nil {
			return nil, WrapErrorf(err, "failed to unmarshal file metadata for revision entry %s", re.Path)
		}
		re.Metadata = metadata
	}
	if br.Err != nil {
		return nil, WrapErrorf(br.Err, "failed to unmarshal revision entry")
	}
	return &re, nil
}

type RevisionReader struct {
	revision   *Revision
	repository *Repository
	blockIndex int
	current    io.Reader
	blockBuf   BlockBuf
}

func NewRevisionReader(repository *Repository, revision *Revision, blockBuf BlockBuf) *RevisionReader {
	return &RevisionReader{
		revision:   revision,
		repository: repository,
		blockIndex: 0,
		current:    nil,
		blockBuf:   blockBuf,
	}
}

// Return `io.EOF` if we are done.
func (rr *RevisionReader) Read() (*RevisionEntry, error) {
	if rr.current == nil {
		if rr.blockIndex >= len(rr.revision.Blocks) {
			return nil, io.EOF
		}
		blockId := rr.revision.Blocks[rr.blockIndex]
		data, _, err := rr.repository.ReadBlock(blockId, rr.blockBuf)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read block %s", blockId)
		}
		rr.blockIndex++
		rr.current = bytes.NewBuffer(data)
	}
	re, err := UnmarshalRevisionEntry(rr.current)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// Go to next block.
			rr.current = nil
			return rr.Read()
		}
		return nil, WrapErrorf(err, "failed to unmarshal revision entry")
	}
	return re, nil
}

// Create a sorted list (by `Path`) of revision entries using file based merge sort.
// It is guaranteed that directory entries are sorted together with their contents.
type RevisionEntryChunks struct {
	tmpDir       string
	filePrefix   string
	chunk        []*RevisionEntry
	chunkSize    int
	chunkIndex   int
	maxChunkSize int
}

func NewRevisionEntryChunks(tmpDir string, filePrefix string, maxChunkSize int) *RevisionEntryChunks {
	return &RevisionEntryChunks{tmpDir: tmpDir, filePrefix: filePrefix, maxChunkSize: maxChunkSize} //nolint:exhaustruct
}

func (c *RevisionEntryChunks) Add(re *RevisionEntry) error {
	size := re.EstimatedSize()
	if c.chunkSize > 0 && c.chunkSize+size > c.maxChunkSize {
		if err := c.rotateChunk(); err != nil {
			return err
		}
	}
	c.chunk = append(c.chunk, re)
	c.chunkSize += re.EstimatedSize()
	return nil
}

func (c *RevisionEntryChunks) Chunks() int {
	return c.chunkIndex
}

func (c *RevisionEntryChunks) ChunkReader(index int) (io.ReadCloser, error) {
	if index < 0 || index >= c.chunkIndex {
		return nil, Errorf("chunk index out of range")
	}
	f, err := os.Open(c.chunkFilename(index))
	if err != nil {
		return nil, WrapErrorf(err, "failed to open chunk file")
	}
	return f, nil
}

func (c *RevisionEntryChunks) Close() error {
	if len(c.chunk) != 0 {
		return c.rotateChunk()
	}
	return nil
}

func (c *RevisionEntryChunks) MergeChunks(write func(re *RevisionEntry) error) error {
	if err := c.Close(); err != nil {
		return WrapErrorf(err, "failed to close chunk writer")
	}
	// Open all chunks with a buffered reader.
	readers := make([]io.Reader, c.Chunks())
	for i := range c.Chunks() {
		f, err := c.ChunkReader(i)
		if err != nil {
			return WrapErrorf(err, "failed to open staging chunk file")
		}
		defer f.Close() //nolint:errcheck
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
			return WrapErrorf(err, "failed to read from chunk %d", i)
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
				return err
			}
			if c < 0 {
				minIndex = i
			}
		}
		// Write the "smallest" value.
		if err := write(entries[minIndex].value); err != nil {
			return WrapErrorf(err, "failed to write to target file")
		}
		// Read next value from the same chunk.
		chunkIdx := entries[minIndex].chunkIndex
		value, err := UnmarshalRevisionEntry(readers[chunkIdx])
		if err != nil {
			if errors.Is(err, io.EOF) {
				entries = slices.Delete(entries, minIndex, minIndex+1)
				continue
			}
			return WrapErrorf(err, "failed to read next from chunk %d", chunkIdx)
		}
		entries[minIndex] = &entry{value, chunkIdx}
	}
	return nil
}

func (c *RevisionEntryChunks) chunkFilename(index int) string {
	return filepath.Join(c.tmpDir, fmt.Sprintf("%s-%d", c.filePrefix, index))
}

// Sort the current chunk and write it to disk.
func (c *RevisionEntryChunks) rotateChunk() error {
	var err error
	slices.SortFunc(c.chunk, func(a, b *RevisionEntry) int {
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
	file, err := os.OpenFile(c.chunkFilename(c.chunkIndex), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return WrapErrorf(err, "failed to open chunk file")
	}
	defer file.Close() //nolint:errcheck
	w := bufio.NewWriter(file)
	for _, entry := range c.chunk {
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
	c.chunk = nil
	c.chunkSize = 0
	c.chunkIndex += 1
	return nil
}
