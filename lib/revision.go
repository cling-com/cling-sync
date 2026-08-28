package lib

import (
	"context"
	"encoding/hex"
	"errors"
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
//	<rev>             only <rev>
//	<since>..<until>  excludes <since>, like git's `since..until`
//	<since>..         after <since> up to the head
//	..<until>         the root up to <until>
//	(empty)           the whole chain, the same as `..head`
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
	if !isRange && r.Until != nil {
		// A bare revision is that revision alone, so the range starts at its
		// parent. The oldest revision has no parent in the chain, and a nil
		// Since already stops at the root.
		if i := slices.Index(chain, *r.Until); i >= 0 && i+1 < len(chain) {
			r.Since = &chain[i+1]
		}
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
	last         *RevisionEntry
}

func NewRevisionReader(repository *Repository, revision *Revision) *RevisionReader {
	return &RevisionReader{
		revision:     revision,
		repository:   repository,
		blockIndex:   0,
		current:      nil,
		currentIndex: 0,
		marshaller:   revisionEntryChunkMarshaller{},
		last:         nil,
	}
}

// Return the next entry of the revision, or `io.EOF`.
//
// Entries come out strictly increasing by `RevisionEntry.PathCompare`. A
// revision with entries not in order will fail loudly.
func (rr *RevisionReader) Read(ctx context.Context, buf BlockBuf) (*RevisionEntry, error) {
	for rr.current == nil || rr.currentIndex == len(rr.current) {
		if rr.blockIndex >= len(rr.revision.BlockIds) {
			return nil, io.EOF
		}
		blockId := rr.revision.BlockIds[rr.blockIndex]
		data, err := rr.repository.ReadBlock(ctx, blockId, buf, ReadBlockOpts{RevisionBlockHint: true})
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
	if rr.last != nil && rr.last.PathCompare(entry) >= 0 {
		return nil, Errorf("entries are not strictly sorted: %s >= %s",
			rr.last.PathDesc(), entry.PathDesc())
	}
	rr.last = entry
	return entry, nil
}

// The most a `DisplayOrderReader` holds back before it gives up. Reaching this
// takes thousands of directories whose names nest inside one another.
const maxDisplayOrderHoldBack = 8 * 1024 * 1024

// Read entries in the order they should be printed, where a directory comes
// directly before its contents.
//
// Entries are stored ordered by path alone, so a sibling whose name continues
// below `/` falls between a directory and its contents: `sub`, `sub.txt`,
// `sub/a.txt`. Holding a directory back until its contents are reached gives
// `sub.txt`, `sub`, `sub/a.txt`, which is the same order as comparing a
// directory as its path with a trailing `/`.
type DisplayOrderReader struct {
	source func(BlockBuf) (*RevisionEntry, error)
	// Directories waiting for their contents, outermost first. One holds
	// another only when its name nests inside the gap of the one before it.
	//
	// These outlive later reads into the same `buf`, so an entry must copy its
	// fields instead of pointing into it, pinned by
	// `TestRevisionEntry/Unmarshalling does not alias the buffer`.
	held         []*RevisionEntry
	heldBytes    int
	pending      []*RevisionEntry
	pendingIndex int
}

func NewDisplayOrderReader(source func(BlockBuf) (*RevisionEntry, error)) *DisplayOrderReader {
	return &DisplayOrderReader{source, nil, 0, nil, 0}
}

func (dr *DisplayOrderReader) Read(buf BlockBuf) (*RevisionEntry, error) {
	for {
		if dr.pendingIndex < len(dr.pending) {
			entry := dr.pending[dr.pendingIndex]
			dr.pendingIndex++
			return entry, nil
		}
		dr.pending = dr.pending[:0]
		dr.pendingIndex = 0
		entry, err := dr.source(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			if len(dr.held) == 0 {
				return nil, io.EOF
			}
			for i := len(dr.held) - 1; i >= 0; i-- {
				dr.pending = append(dr.pending, dr.held[i])
			}
			dr.held = dr.held[:0]
			dr.heldBytes = 0
			continue
		}
		// A directory keeps waiting only while the entries arriving belong to
		// the gap between it and its own contents, which are the paths that
		// extend its name with a byte below `/`.
		for len(dr.held) > 0 {
			held := dr.held[len(dr.held)-1]
			p, d := entry.Path.p, held.Path.p
			if len(p) > len(d) && strings.HasPrefix(p, d) && p[len(d)] < '/' {
				break
			}
			dr.held = dr.held[:len(dr.held)-1]
			dr.heldBytes -= held.MarshallSize()
			dr.pending = append(dr.pending, held)
		}
		if !entry.Metadata.FileMode.IsDir() {
			dr.pending = append(dr.pending, entry)
			continue
		}
		dr.held = append(dr.held, entry)
		dr.heldBytes += entry.MarshallSize()
		if dr.heldBytes > maxDisplayOrderHoldBack {
			return nil, Errorf("too many nested directories to list, starting at %s", dr.held[0].Path)
		}
	}
}
