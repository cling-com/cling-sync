// Staging reflects the current state of the working directory.
// It can then be used to create a new revision together with a `RevisionSnapshot` of
// the base revision.
package lib

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const defaultChunkSize = 4 * 1024 * 1024

type Staging struct {
	BaseRevision RevisionId
	targetFile   string
	chunkWriter  *ChunkWriter
	tmpDir       string
}

func NewStaging(parent RevisionId, tmpDir string) (*Staging, error) {
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read temporary directory %s", tmpDir)
	}
	if len(files) > 0 {
		return nil, Errorf("temporary directory %s is not empty", tmpDir)
	}
	chunkWriter := NewChunkWriter(tmpDir, "staging", defaultChunkSize)
	return &Staging{parent, filepath.Join(tmpDir, "staging"), chunkWriter, tmpDir}, nil
}

func (s *Staging) Add(path Path, md *FileMetadata) error {
	if md == nil {
		return Errorf("file metadata is nil")
	}
	if s.chunkWriter == nil {
		return Errorf("staging is closed")
	}
	re, err := NewRevisionEntry(path, RevisionEntryAdd, md)
	if err != nil {
		return WrapErrorf(err, "failed to create revision entry")
	}
	return s.chunkWriter.Write(&re)
}

func (s *Staging) MergeChunks() error {
	if s.chunkWriter == nil {
		return nil
	}
	defer func() { s.chunkWriter = nil }()
	if err := s.chunkWriter.Close(); err != nil {
		return err
	}
	// Open all chunks with a buffered reader.
	readers := make([]io.Reader, s.chunkWriter.Chunks())
	for i := range s.chunkWriter.Chunks() {
		f, err := s.chunkWriter.ChunkReader(i)
		if err != nil {
			return WrapErrorf(err, "failed to open staging chunk file")
		}
		defer f.Close() //nolint:errcheck
		readers[i] = bufio.NewReader(f)
	}
	file, err := os.OpenFile(s.targetFile, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return WrapErrorf(err, "failed to open target file")
	}
	writer := bufio.NewWriter(file)
	defer file.Close() //nolint:errcheck
	err = nWayMergeSort(
		readers,
		writer,
		UnmarshalRevisionEntry,
		MarshalRevisionEntry,
		func(a, b *RevisionEntry) (int, error) {
			c := strings.Compare(string(a.Path), string(b.Path))
			if c == 0 {
				return 0, Errorf("duplicate revision entry path: %s", a.Path)
			}
			return c, nil
		},
	)
	if err != nil {
		return WrapErrorf(err, "failed to sort and merge chunks")
	}
	if err := writer.Flush(); err != nil {
		return WrapErrorf(err, "failed to flush target file")
	}
	return nil
}

//nolint:funlen
func (s *Staging) Commit(repository *Repository, snapshot *RevisionSnapshot, info *CommitInfo) (RevisionId, error) {
	if err := s.MergeChunks(); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to merge staging chunks")
	}
	if s.BaseRevision != snapshot.RevisionId {
		return RevisionId{}, Errorf(
			"staging base revision %s does not match snapshot revision %s",
			s.BaseRevision,
			snapshot.RevisionId,
		)
	}
	head, err := repository.Head()
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to read repository head")
	}
	if head != s.BaseRevision {
		return RevisionId{}, Errorf(
			"staging base revision %s does not match repository head %s",
			s.BaseRevision,
			head,
		)
	}
	revReader, err := snapshot.Reader()
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to open revision snapshot")
	}
	stgFile, err := os.Open(s.targetFile)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to open staging snapshot")
	}
	defer stgFile.Close() //nolint:errcheck
	stgReader := bufio.NewReader(stgFile)
	stgDone := false
	revDone := false
	cw := NewChunkWriter(s.tmpDir, "commit", MaxBlockDataSize)
	for !stgDone && !revDone {
		stg, err := UnmarshalRevisionEntry(stgReader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				stgDone = true
				break
			}
			return RevisionId{}, WrapErrorf(err, "failed to read staging snapshot")
		}
		rev, err := revReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				revDone = true
				break
			}
			return RevisionId{}, WrapErrorf(err, "failed to read revision snapshot")
		}
		if stg.Path == rev.Path {
			if stg.Metadata.IsEqual(rev.Metadata) {
				// The file is unchanged, skip it.
				continue
			}
			// Write an update.
			re, err := NewRevisionEntry(stg.Path, RevisionEntryUpdate, stg.Metadata)
			if err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to create update revision entry for path %s", stg.Path)
			}
			if err := cw.Write(&re); err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to write update revision entry for path %s", stg.Path)
			}
			continue
		}
		if stg.Path < rev.Path {
			// Write an add and continue to read stg until it is not smaller than rev.
			for {
				if err := cw.Write(stg); err != nil {
					return RevisionId{}, WrapErrorf(err, "failed to write add revision entry for path %s", stg.Path)
				}
				stg, err = UnmarshalRevisionEntry(stgReader)
				if err != nil {
					if errors.Is(err, io.EOF) {
						stgDone = true
						break
					}
					return RevisionId{}, WrapErrorf(err, "failed to read staging snapshot")
				}
				if stg.Path >= rev.Path {
					break
				}
			}
			continue
		}
		// Write a delete and continue to read rev until it is not smaller than stg.
		for {
			re, err := NewRevisionEntry(rev.Path, RevisionEntryDelete, nil)
			if err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to create delete revision entry for path %s", rev.Path)
			}
			if err := cw.Write(&re); err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to write delete revision entry for path %s", rev.Path)
			}
			rev, err = revReader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					revDone = true
					break
				}
				return RevisionId{}, WrapErrorf(err, "failed to read revision snapshot")
			}
			if rev.Path >= stg.Path {
				break
			}
		}
	}
	if stgDone {
		// All subsequent entries in `revReader` have been removed.
		for {
			rev, err := revReader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to read revision snapshot")
			}
			// Write a delete.
			re, err := NewRevisionEntry(rev.Path, RevisionEntryDelete, nil)
			if err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to create delete revision entry for path %s", rev.Path)
			}
			if err := cw.Write(&re); err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to write delete revision entry for path %s", rev.Path)
			}
		}
	} else if revDone {
		// All subsequent entries in `stgReader` have been added.
		for {
			stg, err := UnmarshalRevisionEntry(stgReader)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to read staging snapshot")
			}
			// Write an add.
			re, err := NewRevisionEntry(stg.Path, RevisionEntryAdd, stg.Metadata)
			if err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to create add revision entry for path %s", stg.Path)
			}
			if err := cw.Write(&re); err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to write add revision entry for path %s", stg.Path)
			}
		}
	}
	if err := cw.Close(); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to close revision chunk writer")
	}
	// Create Blocks out of each chunk.
	now := time.Now()
	revision := &Revision{
		TimestampSec:  now.Unix(),
		TimestampNSec: int32(now.Nanosecond()), //nolint:gosec
		Message:       info.Message,
		Author:        info.Author,
		Parent:        s.BaseRevision,
		Blocks:        make([]BlockId, 0),
	}
	blockBuf := BlockBuf{}
	for i := range cw.Chunks() {
		f, err := cw.ChunkReader(i)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to open revision chunk file")
		}
		defer f.Close() //nolint:errcheck
		data, err := io.ReadAll(f)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to read revision chunk file")
		}
		f.Close() //nolint:errcheck,gosec
		_, header, err := repository.WriteBlock(data, blockBuf)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to write block")
		}
		revision.Blocks = append(revision.Blocks, header.BlockId)
	}
	revisionId, err := repository.WriteRevision(revision, blockBuf)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write revision")
	}
	return revisionId, nil
}

type ChunkWriter struct {
	tmpDir       string
	filePrefix   string
	chunk        []*RevisionEntry
	chunkSize    int
	chunkIndex   int
	maxChunkSize int
}

func NewChunkWriter(tmpDir string, filePrefix string, maxChunkSize int) *ChunkWriter {
	return &ChunkWriter{tmpDir: tmpDir, filePrefix: filePrefix, maxChunkSize: maxChunkSize} //nolint:exhaustruct
}

func (cw *ChunkWriter) Write(re *RevisionEntry) error {
	size := re.EstimatedSize()
	if cw.chunkSize > 0 && cw.chunkSize+size > cw.maxChunkSize {
		if err := cw.rotateChunk(); err != nil {
			return err
		}
	}
	cw.chunk = append(cw.chunk, re)
	cw.chunkSize += re.EstimatedSize()
	return nil
}

func (cw *ChunkWriter) Chunks() int {
	return cw.chunkIndex
}

func (cw *ChunkWriter) ChunkReader(index int) (io.ReadCloser, error) {
	if index < 0 || index >= cw.chunkIndex {
		return nil, Errorf("chunk index out of range")
	}
	f, err := os.Open(cw.chunkFilename(index))
	if err != nil {
		return nil, WrapErrorf(err, "failed to open chunk file")
	}
	return f, nil
}

func (cw *ChunkWriter) Close() error {
	if len(cw.chunk) != 0 {
		return cw.rotateChunk()
	}
	return nil
}

func (cw *ChunkWriter) chunkFilename(index int) string {
	return filepath.Join(cw.tmpDir, fmt.Sprintf("%s-%d", cw.filePrefix, index))
}

// Sort the current chunk and write it to disk.
func (cw *ChunkWriter) rotateChunk() error {
	var err error
	slices.SortFunc(cw.chunk, func(a, b *RevisionEntry) int {
		c := strings.Compare(string(a.Path), string(b.Path))
		if c == 0 {
			err = Errorf("duplicate revision entry path: %s", a.Path)
		}
		return c
	})
	if err != nil {
		return err
	}
	// todo: encrypt the data before writing to disk.
	file, err := os.OpenFile(cw.chunkFilename(cw.chunkIndex), os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return WrapErrorf(err, "failed to open chunk file")
	}
	defer file.Close() //nolint:errcheck
	w := bufio.NewWriter(file)
	for _, entry := range cw.chunk {
		if err := MarshalRevisionEntry(entry, w); err != nil {
			return WrapErrorf(err, "failed to write to chunk file")
		}
	}
	if err := w.Flush(); err != nil {
		return WrapErrorf(err, "failed to flush chunk file")
	}
	if err := file.Close(); err != nil {
		return WrapErrorf(err, "failed to close chunk file")
	}
	cw.chunk = nil
	cw.chunkSize = 0
	cw.chunkIndex += 1
	return nil
}

func nWayMergeSort[T any](
	readers []io.Reader,
	writer io.Writer,
	unmarshal func(r io.Reader) (T, error),
	marshal func(v T, w io.Writer) error,
	compare func(a, b T) (int, error),
) error {
	type entry struct {
		value      T
		chunkIndex int
	}
	// First, read the first entry of each file.
	entries := make([]*entry, 0, len(readers))
	for i, r := range readers {
		// todo(perf): We should not need to unmarshal and the marshal all entries.
		value, err := unmarshal(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				continue
			}
			return WrapErrorf(err, "failed to read from chunk %d", i)
		}
		entries = append(entries, &entry{value, i})
	}
	for len(entries) > 0 {
		// Find the "smallest" FileMetadata.
		minIndex := 0
		for i := 1; i < len(entries); i++ {
			c, err := compare(entries[i].value, entries[minIndex].value)
			if err != nil {
				return err
			}
			if c < 0 {
				minIndex = i
			}
		}
		// Write the "smallest" value.
		if err := marshal(entries[minIndex].value, writer); err != nil {
			return WrapErrorf(err, "failed to write to target file")
		}
		// Read next value from the same chunk.
		chunkIdx := entries[minIndex].chunkIndex
		value, err := unmarshal(readers[chunkIdx])
		if err != nil {
			if errors.Is(err, io.EOF) {
				entries = slices.Delete(entries, minIndex, minIndex+1)
				continue
			}
			return WrapErrorf(err, "failed to read next from chunk %d", chunkIdx)
		}
		entries[minIndex] = &entry{value, chunkIdx}
	}
	return nil
}
