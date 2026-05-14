package lib

import (
	"encoding/hex"
	"io"
)

type RevisionId BlockId

func (id RevisionId) String() string {
	return hex.EncodeToString(id[:])
}

func (id RevisionId) IsRoot() bool {
	return id == (RevisionId)(BlockId{})
}

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
		if rr.blockIndex >= len(rr.revision.BlockIds) {
			return nil, io.EOF
		}
		blockId := rr.revision.BlockIds[rr.blockIndex]
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
