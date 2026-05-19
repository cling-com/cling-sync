package lib

import (
	"fmt"
	"strings"
)

// `ProtobufWriter.WriteMessage` reserves 10B of scratch per nesting level.
// A `RevisionEntryChunk` nests 3 deep (entry → metadata → timestamp). 64
// covers the worst case with slack.
const revisionEntryChunkMarshallScratch = 64

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
func RevisionEntryPathCompare(a, b *RevisionEntry) int {
	return strings.Compare(
		PathCompareString(a.Path, a.Metadata.FileMode.IsDir()),
		PathCompareString(b.Path, b.Metadata.FileMode.IsDir()),
	)
}

func RevisionEntryPathCompareString(e *RevisionEntry) string {
	return PathCompareString(e.Path, e.Metadata.FileMode.IsDir())
}

func RevisionEntryPathFilter(pathFilter PathFilter) func(e *RevisionEntry) bool {
	if pathFilter == nil {
		return nil
	}
	return func(e *RevisionEntry) bool {
		return pathFilter.Include(e.Path, e.Metadata.FileMode.IsDir())
	}
}

type RevisionEntryReader interface {
	Read(buf BlockBuf) (*RevisionEntry, error)
}

func NewRevisionEntryTempWriter(fs FS, maxChunkSize int) *TempWriter[*RevisionEntry] {
	return NewTempWriter[*RevisionEntry](
		RevisionEntryPathCompare,
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

func NewRevisionEntryTempCache(
	temp *Temp[*RevisionEntry],
	maxChunksInCache int,
) (*TempCache[*RevisionEntry], error) {
	return NewTempCache(temp, RevisionEntryPathCompareString, maxChunksInCache)
}
