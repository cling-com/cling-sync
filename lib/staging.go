// Staging reflects the current state of the working directory.
// It can then be used to create a new revision together with a `RevisionSnapshot` of
// the base revision.
//
// After commit the repository reflects the working directory, i.e. files that are not
// in the working directory will be removed from the repository head revision.
package lib

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

var ErrEmptyCommit = Errorf("empty commit")

type Staging struct {
	BaseRevision RevisionId
	PathFilter   PathFilter
	targetFile   string
	chunks       *RevisionEntryChunks
	tmpDir       string
}

func NewStaging(parent RevisionId, pathFilter PathFilter, tmpDir string) (*Staging, error) {
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read temporary directory %s", tmpDir)
	}
	if len(files) > 0 {
		return nil, Errorf("temporary directory %s is not empty", tmpDir)
	}
	chunkWriter := NewRevisionEntryChunks(tmpDir, "staging", defaultChunkSize)
	return &Staging{parent, pathFilter, filepath.Join(tmpDir, "staging"), chunkWriter, tmpDir}, nil
}

// Return `true` if the file was added, `false` if it was ignored.
func (s *Staging) Add(path Path, md *FileMetadata) (bool, error) {
	if md == nil {
		return false, Errorf("file metadata is nil")
	}
	if s.chunks == nil {
		return false, Errorf("staging is closed")
	}
	if s.PathFilter != nil && !s.PathFilter.Include(path.FSString()) {
		return false, nil
	}
	re, err := NewRevisionEntry(path, RevisionEntryAdd, md)
	if err != nil {
		return false, WrapErrorf(err, "failed to create revision entry")
	}
	if err := s.chunks.Add(&re); err != nil {
		return false, err
	}
	return true, nil
}

// Commit by merging the staging snapshot with the revision snapshot. The new revision will
// only contain the entries that are in the staging snapshot.
//
// Return `ErrEmptyCommit` if there are no changes.
//
//nolint:funlen
func (s *Staging) Commit(repository *Repository, snapshot *RevisionSnapshot, info *CommitInfo) (RevisionId, error) {
	if err := s.mergeChunks(); err != nil {
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
	revReader, err := snapshot.Reader(s.PathFilter)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to open revision snapshot")
	}
	stgFile, err := os.Open(s.targetFile)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to open staging snapshot")
	}
	defer stgFile.Close() //nolint:errcheck
	stgReader := bufio.NewReader(stgFile)
	cw := NewRevisionEntryChunks(s.tmpDir, "commit", MaxBlockDataSize)
	add := func(path Path, typ RevisionEntryType, md *FileMetadata) error {
		re, err := NewRevisionEntry(path, typ, md)
		if err != nil {
			return WrapErrorf(err, "failed to create revision entry for path %s", path.FSString())
		}
		if err := cw.Add(&re); err != nil {
			return WrapErrorf(err, "failed to write revision entry for path %s", path.FSString())
		}
		return nil
	}
	var stg *RevisionEntry
	var rev *RevisionEntry
	for {
		if stg == nil {
			// Read the next staging entry.
			stg, err = UnmarshalRevisionEntry(stgReader)
			if errors.Is(err, io.EOF) {
				// Write a delete for all remaining revision snapshot entries.
				for {
					if rev != nil { // The current one might be nil.
						// Write a delete.
						if err := add(rev.Path, RevisionEntryDelete, nil); err != nil {
							return RevisionId{}, err
						}
					}
					rev, err = revReader.Read()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return RevisionId{}, WrapErrorf(err, "failed to read revision snapshot")
					}
				}
				break
			}
			if err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to read staging snapshot")
			}
		}
		if rev == nil {
			// Read the next revision snapshot entry.
			rev, err = revReader.Read()
			if errors.Is(err, io.EOF) {
				// Write an add for all remaining staging entries.
				for {
					if stg != nil { // The current one might be nil.
						if err := add(stg.Path, RevisionEntryAdd, stg.Metadata); err != nil {
							return RevisionId{}, err
						}
					}
					stg, err = UnmarshalRevisionEntry(stgReader)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return RevisionId{}, WrapErrorf(err, "failed to read staging snapshot")
					}
				}
				break
			}
			if err != nil {
				return RevisionId{}, WrapErrorf(err, "failed to read revision snapshot")
			}
		}
		if stg.Path == rev.Path { //nolint:gocritic
			if !stg.Metadata.IsEqual(rev.Metadata) {
				// Write an update.
				if err := add(stg.Path, RevisionEntryUpdate, stg.Metadata); err != nil {
					return RevisionId{}, err
				}
			}
			stg = nil
			rev = nil
		} else if stg.Path < rev.Path {
			// Write an add.
			if err := add(stg.Path, RevisionEntryAdd, stg.Metadata); err != nil {
				return RevisionId{}, err
			}
			stg = nil
			continue
		} else {
			// Write a delete.
			if err := add(rev.Path, RevisionEntryDelete, nil); err != nil {
				return RevisionId{}, err
			}
			rev = nil
			continue
		}
	}
	if err := cw.Close(); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to close revision chunk writer")
	}
	if cw.Chunks() == 0 {
		return RevisionId{}, ErrEmptyCommit
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

func (s *Staging) mergeChunks() error {
	if s.chunks == nil {
		return nil
	}
	defer func() { s.chunks = nil }()
	if err := s.chunks.Close(); err != nil {
		return err
	}
	file, err := os.OpenFile(s.targetFile, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return WrapErrorf(err, "failed to open target file")
	}
	defer file.Close() //nolint:errcheck
	fileBuf := bufio.NewWriter(file)
	write := func(re *RevisionEntry) error {
		if err := MarshalRevisionEntry(re, fileBuf); err != nil {
			return WrapErrorf(err, "failed to write revision entry")
		}
		return nil
	}
	err = s.chunks.MergeChunks(write)
	if err != nil {
		return WrapErrorf(err, "failed to sort and merge chunks")
	}
	if err := fileBuf.Flush(); err != nil {
		return WrapErrorf(err, "failed to flush target file")
	}
	if err := file.Close(); err != nil {
		return WrapErrorf(err, "failed to close target file")
	}
	return nil
}
