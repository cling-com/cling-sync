package lib

import (
	"encoding/hex"
	"io"
)

const revisionMarshalMagick = "cling-rev"

type RevisionId BlockId

func (id RevisionId) String() string {
	return hex.EncodeToString(id[:])
}

func (id RevisionId) IsRoot() bool {
	return id == (RevisionId)(BlockId{})
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
	if l <= 0 {
		return nil, Errorf("invalid blocks length")
	}
	c.Blocks = make([]BlockId, l)
	for i := range l {
		br.Read(&c.Blocks[i])
	}
	return &c, br.Err
}

// RevisionReader streams `RevisionEntry`s out of the blocks of a `Revision`.
// Each block holds a `RevisionEntryChunk` — the same wire format as a
// TempWriter chunk file.
type RevisionReader struct {
	revision     *Revision
	repository   *Repository
	blockIndex   int
	current      []*RevisionEntry
	currentIndex int
	marshaller   revisionEntryChunkMarshaller
}

func NewRevisionReader(repository *Repository, revision *Revision) *RevisionReader {
	return &RevisionReader{
		revision:     revision,
		repository:   repository,
		blockIndex:   0,
		current:      nil,
		currentIndex: 0,
		marshaller:   revisionEntryChunkMarshaller{},
	}
}

// Return `io.EOF` if we are done.
func (rr *RevisionReader) Read(buf BlockBuf) (*RevisionEntry, error) {
	for rr.current == nil || rr.currentIndex == len(rr.current) {
		if rr.blockIndex >= len(rr.revision.Blocks) {
			return nil, io.EOF
		}
		blockId := rr.revision.Blocks[rr.blockIndex]
		data, err := rr.repository.ReadBlock(blockId, buf)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read block %s", blockId)
		}
		entries, err := rr.marshaller.UnmarshallAll(NewProtobufReader(data))
		if err != nil {
			return nil, WrapErrorf(err, "failed to unmarshall block %s", blockId)
		}
		rr.blockIndex++
		rr.current = entries
		rr.currentIndex = 0
	}
	entry := rr.current[rr.currentIndex]
	rr.currentIndex++
	return entry, nil
}
