package lib

import (
	"context"
	"encoding/hex"
	"io"
	"slices"
	"strings"
)

type RevisionId BlockId

// NewRevisionIdFromString parses a hex-encoded RevisionId.
func NewRevisionIdFromString(s string) (RevisionId, error) {
	id, err := NewBlockIdFromString(s)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "invalid revision id")
	}
	return RevisionId(id), nil
}

func (id RevisionId) String() string {
	return hex.EncodeToString(id[:])
}

func (id RevisionId) IsRoot() bool {
	return id == (RevisionId)(BlockId{})
}

func (id RevisionId) IsInChain(chain RevisionChain) bool {
	return slices.Contains(chain, id)
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

// RevisionRange is a span of the revision chain. Until is the included revision
// and Since is the excluded one, like git's `Since..Until`. A nil Until means
// the head. A nil Since means the root.
type RevisionRange struct {
	Since *RevisionId
	Until *RevisionId
}

// NewRevisionRangeFromString parses the text format:
//
//	<until>           <until> back to the root (a single revision)
//	<since>..<until>  excludes <since>, like git's `since..until`
//	<since>..         after <since> up to the head
//	..<until>         the root up to <until> (same as `<until>`)
//	(empty)           the whole chain
func NewRevisionRangeFromString(s string) (RevisionRange, error) {
	var r RevisionRange
	since, until, isRange := strings.Cut(s, "..")
	if !isRange {
		since, until = "", since
	}
	if since != "" {
		id, err := NewRevisionIdFromString(since)
		if err != nil {
			return r, WrapErrorf(err, "invalid range since %q", since)
		}
		r.Since = &id
	}
	if until != "" {
		id, err := NewRevisionIdFromString(until)
		if err != nil {
			return r, WrapErrorf(err, "invalid range until %q", until)
		}
		r.Until = &id
	}
	return r, nil
}

func (r RevisionRange) String() string {
	switch {
	case r.Since == nil && r.Until == nil:
		return ""
	case r.Since == nil:
		return r.Until.String()
	case r.Until == nil:
		return r.Since.String() + ".."
	default:
		return r.Since.String() + ".." + r.Until.String()
	}
}

// IsInChain reports whether every bound of the range is part of the chain. A
// nil bound (the head or the root) is always considered valid.
func (r RevisionRange) IsInChain(chain RevisionChain) bool {
	if r.Since != nil && !r.Since.IsInChain(chain) {
		return false
	}
	if r.Until != nil && !r.Until.IsInChain(chain) {
		return false
	}
	return true
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
