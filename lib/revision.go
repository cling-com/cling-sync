package lib

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

const revisionMarshalMagick = "cling-rev"

type RevisionId BlockId

func (id RevisionId) String() string {
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
func RevisionEntryPathCompare(a, b *RevisionEntry) int {
	return strings.Compare(
		PathCompareString(a.Path, a.Metadata.ModeAndPerm.IsDir()),
		PathCompareString(b.Path, b.Metadata.ModeAndPerm.IsDir()),
	)
}

func RevisionEntryPathCompareString(e *RevisionEntry) string {
	return PathCompareString(e.Path, e.Metadata.ModeAndPerm.IsDir())
}

type RevisionEntry struct {
	Path     Path
	Type     RevisionEntryType
	Metadata *FileMetadata
}

func NewRevisionEntry(path Path, typ RevisionEntryType, md *FileMetadata) (RevisionEntry, error) {
	return RevisionEntry{Path: path, Type: typ, Metadata: md}, nil
}

func MarshalledSize(r *RevisionEntry) int {
	return r.Path.Len() + 2 + 1 + // Path + len(Path) + Type
		r.Metadata.MarshalledSize()
}

func MarshalRevisionEntry(r *RevisionEntry, w io.Writer) error {
	bw := NewBinaryWriter(w)
	bw.WriteString(r.Path.String())
	bw.Write(r.Type)
	if err := MarshalFileMetadata(r.Metadata, w); err != nil {
		return WrapErrorf(err, "failed to marshal revision entry %s", r.Path)
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
	var err error
	re.Path, err = NewPath(path)
	if err != nil {
		return nil, WrapErrorf(err, "failed to unmarshal revision entry, invalid path %q", path)
	}
	br.Read(&re.Type)
	metadata, err := UnmarshalFileMetadata(r)
	if err != nil {
		return nil, WrapErrorf(err, "failed to unmarshal file metadata for revision entry %s", re.Path)
	}
	re.Metadata = metadata
	if br.Err != nil {
		return nil, WrapErrorf(br.Err, "failed to unmarshal revision entry")
	}
	return &re, nil
}

type RevisionEntryReader interface {
	Read() (*RevisionEntry, error)
}

type RevisionReader struct {
	revision   *Revision
	repository *Repository
	blockIndex int
	current    io.Reader
}

func NewRevisionReader(repository *Repository, revision *Revision) *RevisionReader {
	return &RevisionReader{
		revision:   revision,
		repository: repository,
		blockIndex: 0,
		current:    nil,
	}
}

// Return `io.EOF` if we are done.
func (rr *RevisionReader) Read() (*RevisionEntry, error) {
	if rr.current == nil {
		if rr.blockIndex >= len(rr.revision.Blocks) {
			return nil, io.EOF
		}
		blockId := rr.revision.Blocks[rr.blockIndex]
		data, _, err := rr.repository.ReadBlock(blockId)
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

func NewRevisionEntryTempWriter(fs FS, maxChunkSize int) (*TempWriter[RevisionEntry], error) {
	return NewTempWriter(
		RevisionEntryPathCompare,
		MarshalRevisionEntry,
		MarshalledSize,
		UnmarshalRevisionEntry,
		fs,
		maxChunkSize,
	)
}

func NewRevisionEntryTempCache(temp *Temp[RevisionEntry], maxChunksInCache int) (*TempCache[RevisionEntry], error) {
	return NewTempCache(temp, RevisionEntryPathCompareString, maxChunksInCache)
}

func RevisionEntryPathFilter(pathFilter PathFilter) func(e *RevisionEntry) bool {
	if pathFilter == nil {
		return nil
	}
	return func(e *RevisionEntry) bool {
		return pathFilter.Include(e.Path, e.Metadata.ModeAndPerm.IsDir())
	}
}
