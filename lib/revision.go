package lib

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"time"
)

const revisionMarshalMagick = "cling-rev"

type RevisionId BlockId

func (id RevisionId) String() string {
	return "rev:" + hex.EncodeToString(id[:])
}

func (id RevisionId) Short() string {
	return hex.EncodeToString(id[:])[:8]
}

func (id RevisionId) Long() string {
	return hex.EncodeToString(id[:])
}

func (id RevisionId) IsRoot() bool {
	// fimxe: test
	return id == (RevisionId)(BlockId{})
}

type RevisionEntryType = uint8

const (
	RevisionEntryAdd    = 0
	RevisionEntryUpdate = 1
	RevisionEntryDelete = 2
)

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

type RevisionBuilder struct {
	buf    bytes.Buffer
	parent RevisionId
}

func NewRevisionBuilder(repo *Repository) (*RevisionBuilder, error) {
	head, err := repo.Head()
	if err != nil {
		return nil, WrapErrorf(err, "failed to get head revision")
	}
	return &RevisionBuilder{parent: head}, nil //nolint:exhaustruct
}

func (rb *RevisionBuilder) Add(re *RevisionEntry) error {
	if rb.buf.Len()+(re.EstimatedSize()*2) > MaxBlockDataSize {
		return Errorf("revision entry is too large")
	}
	err := MarshalRevisionEntry(re, &rb.buf)
	if err != nil {
		return WrapErrorf(err, "failed to marshal revision entry")
	}
	return nil
}

type CommitInfo struct {
	Author  string
	Message string
}

func (rb *RevisionBuilder) Commit(repo *Repository, blockBuf BlockBuf, info *CommitInfo) (RevisionId, error) {
	if rb.buf.Len() == 0 {
		return RevisionId{}, Errorf("revision is empty")
	}
	_, blockHeader, err := repo.WriteBlock(rb.buf.Bytes(), blockBuf)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write revision block")
	}
	now := time.Now()
	commit := &Revision{
		TimestampSec:  now.Unix(),
		TimestampNSec: int32(now.Nanosecond()), //nolint:gosec
		Message:       info.Message,
		Author:        info.Author,
		Parent:        rb.parent,
		Blocks:        []BlockId{blockHeader.BlockId},
	}
	return repo.WriteRevision(commit, blockBuf)
}

type RevisionReader struct {
	revision   *Revision
	repository *Repository
	blockIndex int
	current    io.Reader
	blockBuf   BlockBuf
}

func NewRevisionReader(repository *Repository, commit *Revision, blockBuf BlockBuf) *RevisionReader {
	return &RevisionReader{
		revision:   commit,
		repository: repository,
		blockIndex: 0,
		current:    nil,
		blockBuf:   blockBuf,
	}
}

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
