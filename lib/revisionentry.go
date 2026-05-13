package lib

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
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

// RevisionEntryDiskSize returns the exact number of bytes that
// MarshalRevisionEntry would emit for r (the 4-byte length prefix plus
// the protobuf payload).
func RevisionEntryDiskSize(r *RevisionEntry) int {
	return 4 + r.MarshallSize()
}

// MarshalRevisionEntry writes a length-prefixed, protobuf-encoded
// RevisionEntry to w. This io.Writer wrapper bridges TempWriter; it will
// be removed when TempWriter is migrated to ProtobufWriter/Reader.
func MarshalRevisionEntry(r *RevisionEntry, w io.Writer) error {
	// +64 covers WriteMessage's 10-bytes-per-nesting-level scratch space.
	// Goes away with the hand-written wrapper.
	buf := make([]byte, r.MarshallSize()+64)
	pw := NewProtobufWriter(buf)
	if err := r.Marshall(pw); err != nil {
		return WrapErrorf(err, "failed to marshal revision entry %s", r.Path)
	}
	payload := pw.Bytes()
	if err := binary.Write(w, binary.LittleEndian, uint32(len(payload))); err != nil { //nolint:gosec
		return WrapErrorf(err, "failed to write revision entry length")
	}
	if _, err := w.Write(payload); err != nil {
		return WrapErrorf(err, "failed to write revision entry payload")
	}
	return nil
}

func UnmarshalRevisionEntry(r io.Reader) (*RevisionEntry, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return nil, WrapErrorf(err, "failed to read revision entry length")
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, WrapErrorf(err, "failed to read revision entry payload")
	}
	e, err := UnmarshallRevisionEntry(NewProtobufReader(buf))
	if err != nil {
		return nil, err
	}
	return &e, nil
}

type RevisionEntryReader interface {
	Read(buf BlockBuf) (*RevisionEntry, error)
}

func NewRevisionEntryTempWriter(fs FS, maxChunkSize int) *TempWriter[RevisionEntry] {
	return NewTempWriter(
		RevisionEntryPathCompare,
		MarshalRevisionEntry,
		RevisionEntryDiskSize,
		UnmarshalRevisionEntry,
		fs,
		maxChunkSize,
	)
}

func NewRevisionEntryTempCache(temp *Temp[RevisionEntry], maxChunksInCache int) (*TempCache[RevisionEntry], error) {
	return NewTempCache(temp, RevisionEntryPathCompareString, maxChunksInCache)
}
