package lib

import (
	"context"
	"encoding/hex"
	"io"
	"slices"
	"strconv"
	"strings"
)

type RevisionId BlockId

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
type RevisionChain []RevisionId

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

// ParseRevisionId resolves a revision spec against the chain. A spec is a hex
// revision id or `head`, optionally suffixed with `~<n>` to walk n revisions
// back toward the root, like git's `HEAD~2`. `head` and `head~0` are the head
// revision (the root revision on an empty repository).
func (chain RevisionChain) ParseRevisionId(spec string) (RevisionId, error) {
	base, steps, err := splitRevisionSteps(spec)
	if err != nil {
		return RevisionId{}, err
	}
	index := 0
	if !strings.EqualFold(base, "head") {
		id, err := NewBlockIdFromString(base)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "invalid revision id %q", base)
		}
		if index = slices.Index(chain, RevisionId(id)); index < 0 {
			return RevisionId{}, Errorf("revision not found in repository: %s", base)
		}
	}
	target := index + steps
	if target >= len(chain) {
		if len(chain) == 0 && steps == 0 {
			return RevisionId{}, nil // `head` on an empty repository is the root.
		}
		return RevisionId{}, Errorf("revision %q is older than the oldest revision in the repository", spec)
	}
	return chain[target], nil
}

// splitRevisionSteps splits a `<base>~<n>` spec. A bare `~` means one step.
func splitRevisionSteps(spec string) (string, int, error) {
	base, n, found := strings.Cut(spec, "~")
	if !found {
		return spec, 0, nil
	}
	if n == "" {
		return base, 1, nil
	}
	steps, err := strconv.Atoi(n)
	if err != nil || steps < 0 {
		return "", 0, Errorf("invalid revision %q: expected `<rev>~<n>` with a non-negative count", spec)
	}
	return base, steps, nil
}

// RevisionRange is a span of the revision chain. Until is the included revision
// and Since is the excluded one, like git's `Since..Until`. A nil Until means
// the head. A nil Since means the root.
type RevisionRange struct {
	Since *RevisionId
	Until *RevisionId
}

// ParseRevisionRange parses a revision range, resolving each bound against the
// chain. Formats:
//
//	<until>           <until> back to the root (a single revision)
//	<since>..<until>  excludes <since>, like git's `since..until`
//	<since>..         after <since> up to the head
//	..<until>         the root up to <until> (same as `<until>`)
//	(empty)           the whole chain
//
// Each bound is a spec accepted by ParseRevisionId (an id or `head`, with an
// optional `~<n>`).
func (chain RevisionChain) ParseRevisionRange(spec string) (RevisionRange, error) {
	var r RevisionRange
	since, until, isRange := strings.Cut(spec, "..")
	if !isRange {
		since, until = "", since
	}
	if since != "" {
		id, err := chain.ParseRevisionId(since)
		if err != nil {
			return r, WrapErrorf(err, "invalid range since %q", since)
		}
		r.Since = &id
	}
	if until != "" {
		id, err := chain.ParseRevisionId(until)
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
