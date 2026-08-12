package lib

import (
	"fmt"
)

func (k RevisionEntryKind) String() string {
	switch k {
	case RevisionEntryKindAdd:
		return "add"
	case RevisionEntryKindUpdate:
		return "update"
	case RevisionEntryKindDelete:
		return "delete"
	default:
		return fmt.Sprintf("unknown(%d)", uint32(k))
	}
}

// Compare two revision entries by their full path.
func (e *RevisionEntry) PathCompare(other *RevisionEntry) int {
	return PathCompare(e.Path, e.Metadata.FileMode.IsDir(), other.Path, other.Metadata.FileMode.IsDir())
}

// The path, with a trailing separator for a directory, so that a file and a
// directory of one path can be told apart.
func (e *RevisionEntry) PathDesc() string {
	if e.Metadata.FileMode.IsDir() {
		return e.Path.String() + PathDelim
	}
	return e.Path.String()
}

func (e *RevisionEntry) PathKey() PathKey {
	return PathKey{e.Path, e.Metadata.FileMode.IsDir()}
}

type RevisionEntryCache = TempCache[*RevisionEntry, PathKey]

func RevisionEntryPathFilter(pathFilter PathFilter) func(e *RevisionEntry) bool {
	if pathFilter == nil {
		return nil
	}
	return func(e *RevisionEntry) bool {
		return pathFilter.Include(e.Path, e.Metadata.FileMode.IsDir())
	}
}

func NewRevisionEntryTempWriter(fs FS, maxChunkSize int) *TempWriter[*RevisionEntry] {
	return NewTempWriter[*RevisionEntry](
		(*RevisionEntry).PathCompare,
		revisionEntryChunkMarshaller{},
		fs,
		maxChunkSize,
	)
}

// revisionEntryChunkMarshaller serializes batches of `*RevisionEntry` via the
// `RevisionEntryChunk` wire format.
type revisionEntryChunkMarshaller struct{}

func (revisionEntryChunkMarshaller) MarshallAll(entries []*RevisionEntry, w ProtobufWriter) error {
	return (&RevisionEntryChunk{Entries: entries}).Marshall(w)
}

func (revisionEntryChunkMarshaller) UnmarshallAll(r *ProtobufReader) ([]*RevisionEntry, error) {
	chunk, err := UnmarshallRevisionEntryChunk(r)
	if err != nil {
		return nil, err
	}
	return chunk.Entries, nil
}

func (revisionEntryChunkMarshaller) EntrySize(entry *RevisionEntry) int {
	n := entry.MarshallSize()
	return TagLen(1, 2) + VarintLen(int64(n)) + n
}

func NewRevisionEntryTempCache(
	temp *Temp[*RevisionEntry],
	maxChunksInCache int,
) (*RevisionEntryCache, error) {
	return NewTempCache(temp, (*RevisionEntry).PathKey, ComparePathKey, maxChunksInCache)
}
