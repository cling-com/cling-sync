package workspace

import (
	"encoding/binary"
	"io"
	"io/fs"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

func NewStagingEntry(
	path lib.Path,
	fileInfo fs.FileInfo,
	fileSize int64,
	fileHash lib.Sha256,
	blockIds []lib.BlockId,
) (*StagingEntry, error) {
	stat, err := lib.EnhancedStat(fileInfo)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get metadata for %s", path)
	}
	if fileInfo.IsDir() {
		if fileSize != 0 {
			return nil, lib.Errorf("file size mismatch: %d vs 0", fileSize)
		}
	} else {
		if fileInfo.Size() != fileSize {
			return nil, lib.Errorf("file size mismatch: %d vs %d", fileInfo.Size(), fileSize)
		}
	}
	md := lib.NewPathMetadataFromFileInfo(fileInfo, fileHash, blockIds)
	return &StagingEntry{
		RepoPath: path,
		Metadata: md,
		Ctime:    lib.Timestamp{Sec: stat.CTimeSec, Nsec: uint32(stat.CTimeNSec)}, //nolint:gosec
		Size:     fileSize,
		Inode:    stat.Inode,
	}, nil
}

func (e *StagingEntry) HasChanged(other *StagingEntry) bool {
	return e.Ctime != other.Ctime || e.Inode != other.Inode || e.Size != other.Size
}

// StagingEntryDiskSize returns the exact number of bytes that
// MarshalStagingEntry would emit (the 4-byte length prefix plus the
// protobuf payload).
func StagingEntryDiskSize(e *StagingEntry) int {
	return 4 + e.MarshallSize()
}

// MarshalStagingEntry writes a length-prefixed, protobuf-encoded
// StagingEntry to w. This io.Writer wrapper bridges TempWriter; it will
// be removed when TempWriter is migrated to ProtobufWriter/Reader.
func MarshalStagingEntry(e *StagingEntry, w io.Writer) error {
	// +64 covers WriteMessage's 10-bytes-per-nesting-level scratch space.
	// Goes away with the hand-written wrapper.
	buf := make([]byte, e.MarshallSize()+64)
	pw := lib.NewProtobufWriter(buf)
	if err := e.Marshall(pw); err != nil {
		return lib.WrapErrorf(err, "failed to marshal staging entry %s", e.RepoPath)
	}
	payload := pw.Bytes()
	if err := binary.Write(w, binary.LittleEndian, uint32(len(payload))); err != nil { //nolint:gosec
		return lib.WrapErrorf(err, "failed to write staging entry length")
	}
	if _, err := w.Write(payload); err != nil {
		return lib.WrapErrorf(err, "failed to write staging entry payload")
	}
	return nil
}

func UnmarshalStagingEntry(r io.Reader) (*StagingEntry, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return nil, lib.WrapErrorf(err, "failed to read staging entry length")
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, lib.WrapErrorf(err, "failed to read staging entry payload")
	}
	e, err := UnmarshallStagingEntry(lib.NewProtobufReader(buf))
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func StagingEntryPathFilter(pathFilter lib.PathFilter) func(e *StagingEntry) bool {
	if pathFilter == nil {
		return nil
	}
	return func(e *StagingEntry) bool {
		return pathFilter.Include(e.RepoPath, e.Metadata.FileMode.IsDir())
	}
}

func StagingEntryPathCompare(a, b *StagingEntry) int {
	return strings.Compare(
		lib.PathCompareString(a.RepoPath, a.Metadata.FileMode.IsDir()),
		lib.PathCompareString(b.RepoPath, b.Metadata.FileMode.IsDir()),
	)
}

func StagingCacheKey(stagingEntry *StagingEntry) string {
	return lib.PathCompareString(stagingEntry.RepoPath, stagingEntry.Metadata.FileMode.IsDir())
}

func NewStagingCacheWriter(fs lib.FS, maxChunkSize int) *lib.TempWriter[StagingEntry] {
	return lib.NewTempWriter(
		StagingEntryPathCompare,
		MarshalStagingEntry,
		StagingEntryDiskSize,
		UnmarshalStagingEntry,
		fs,
		maxChunkSize,
	)
}

func OpenStagingCache(fs lib.FS, maxChunksInCache int) (*lib.TempCache[StagingEntry], error) {
	temp, err := lib.OpenTemp(fs, UnmarshalStagingEntry)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open temp")
	}
	cache, err := lib.NewTempCache(temp, StagingCacheKey, maxChunksInCache)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create new TempCache")
	}
	return cache, nil
}
