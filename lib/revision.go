package lib

import (
	"context"
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

// RevisionChain is a list of revision ids, head first, ending at the revision
// whose parent is the root (so, the root revision is excluded).
type RevisionChain = []RevisionId

// ReadRevisionChain returns the repository's revision chain, head first.
func ReadRevisionChain(ctx context.Context, repository *Repository) (RevisionChain, error) {
	id, err := repository.Head(ctx)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read head")
	}
	chain := RevisionChain{}
	buf := NewBlockBuf()
	for !id.IsRoot() {
		chain = append(chain, id)
		revision, err := repository.ReadRevision(ctx, id, buf)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read revision %s", id)
		}
		id = revision.ParentRevisionId
	}
	return chain, nil
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
func (rr *RevisionReader) Read(ctx context.Context, buf BlockBuf) (*RevisionEntry, error) {
	for rr.current == nil || rr.currentIndex == len(rr.current) {
		if rr.blockIndex >= len(rr.revision.BlockIds) {
			return nil, io.EOF
		}
		blockId := rr.revision.BlockIds[rr.blockIndex]
		data, err := rr.repository.ReadBlock(ctx, blockId, buf)
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
