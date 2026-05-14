package workspace

import (
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

func NewStagingCacheWriter(fs lib.FS, maxChunkSize int) *lib.TempWriter[*StagingEntry] {
	return lib.NewTempWriter[*StagingEntry](
		StagingEntryPathCompare,
		stagingEntryChunkMarshaller{},
		fs,
		maxChunkSize,
	)
}

func OpenStagingCache(fs lib.FS, maxChunksInCache int) (*lib.TempCache[*StagingEntry], error) {
	temp, err := lib.OpenTemp[*StagingEntry](fs, stagingEntryChunkMarshaller{})
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open temp")
	}
	cache, err := lib.NewTempCache(temp, StagingCacheKey, maxChunksInCache)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create new TempCache")
	}
	return cache, nil
}

// stagingEntryChunkMarshaller serializes batches of `*StagingEntry` via the
// `StagingEntryChunk` wire format.
type stagingEntryChunkMarshaller struct{}

func (stagingEntryChunkMarshaller) MarshallAll(entries []*StagingEntry, w lib.ProtobufWriter) error {
	chunk := StagingEntryChunk{Entries: make([]StagingEntry, len(entries))}
	for i, e := range entries {
		chunk.Entries[i] = *e
	}
	return chunk.Marshall(w)
}

func (stagingEntryChunkMarshaller) UnmarshallAll(r *lib.ProtobufReader) ([]*StagingEntry, error) {
	chunk, err := UnmarshallStagingEntryChunk(r)
	if err != nil {
		return nil, err
	}
	out := make([]*StagingEntry, len(chunk.Entries))
	for i := range chunk.Entries {
		out[i] = &chunk.Entries[i]
	}
	return out, nil
}
