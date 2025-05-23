// Staging reflects the current state of the working directory.
// It can then be used to create a new revision together with a `RevisionSnapshot` of
// the base revision.
//
// After commit the repository reflects the working directory, i.e. files that are not
// in the working directory will be removed from the repository head revision.
package lib

import (
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
	tempWriter   *RevisionTempWriter
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
	tempWriter := NewRevisionTempWriter(tmpDir, defaultChunkSize)
	return &Staging{parent, pathFilter, tempWriter, tmpDir}, nil
}

// Return `true` if the file was added, `false` if it was ignored.
func (s *Staging) Add(path Path, md *FileMetadata) (bool, error) {
	if md == nil {
		return false, Errorf("file metadata is nil")
	}
	if s.tempWriter == nil {
		return false, Errorf("staging is closed")
	}
	if s.PathFilter != nil && !s.PathFilter.Include(path.FSString()) {
		return false, nil
	}
	re, err := NewRevisionEntry(path, RevisionEntryAdd, md)
	if err != nil {
		return false, WrapErrorf(err, "failed to create revision entry")
	}
	if err := s.tempWriter.Add(&re); err != nil {
		return false, err
	}
	return true, nil
}

// Merge the staging snapshot with the revision snapshot.
// The resulting `RevisionEntryChunks` will only contain entries that are in the staging snapshot.
func (s *Staging) MergeWithSnapshot(repository *Repository) (*RevisionTemp, error) { //nolint:funlen
	stgTemp, err := s.tempWriter.Finalize()
	if err != nil {
		return nil, WrapErrorf(err, "failed to finalize staging temp writer")
	}
	snapshot, err := s.revisionSnapshot(repository)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create revision snapshot")
	}
	head, err := repository.Head()
	if err != nil {
		return nil, WrapErrorf(err, "failed to read repository head")
	}
	if head != s.BaseRevision {
		return nil, Errorf(
			"staging base revision %s does not match repository head %s",
			s.BaseRevision,
			head,
		)
	}
	revReader := snapshot.Reader(s.PathFilter)
	stgReader := stgTemp.Reader(s.PathFilter)
	final := filepath.Join(s.tmpDir, "final")
	if err := os.MkdirAll(final, 0o700); err != nil {
		return nil, WrapErrorf(err, "failed to create commit directory")
	}
	finalWriter := NewRevisionTempWriter(final, MaxBlockDataSize)
	add := func(path Path, typ RevisionEntryType, md *FileMetadata) error {
		re, err := NewRevisionEntry(path, typ, md)
		if err != nil {
			return WrapErrorf(err, "failed to create revision entry for path %s", path.FSString())
		}
		if err := finalWriter.Add(&re); err != nil {
			return WrapErrorf(err, "failed to write revision entry for path %s", path.FSString())
		}
		return nil
	}
	var stg *RevisionEntry
	var rev *RevisionEntry
	for {
		if stg == nil {
			// Read the next staging entry.
			stg, err = stgReader.Read()
			if errors.Is(err, io.EOF) {
				// Write a delete for all remaining revision snapshot entries.
				for {
					if rev != nil { // The current one might be nil.
						// Write a delete.
						if err := add(rev.Path, RevisionEntryDelete, nil); err != nil {
							return nil, err
						}
					}
					rev, err = revReader.Read()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return nil, WrapErrorf(err, "failed to read revision snapshot")
					}
				}
				break
			}
			if err != nil {
				return nil, WrapErrorf(err, "failed to read staging snapshot")
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
							return nil, err
						}
					}
					stg, err = stgReader.Read()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return nil, WrapErrorf(err, "failed to read staging snapshot")
					}
				}
				break
			}
			if err != nil {
				return nil, WrapErrorf(err, "failed to read revision snapshot")
			}
		}
		c := RevisionEntryPathCompare(stg, rev)
		if c == 0 { //nolint:gocritic
			if !stg.Metadata.IsEqualIgnoringBlockIds(rev.Metadata) {
				// Write an update.
				if err := add(stg.Path, RevisionEntryUpdate, stg.Metadata); err != nil {
					return nil, err
				}
			}
			stg = nil
			rev = nil
		} else if c < 0 {
			// Write an add.
			if err := add(stg.Path, RevisionEntryAdd, stg.Metadata); err != nil {
				return nil, err
			}
			stg = nil
			continue
		} else {
			// Write a delete.
			if err := add(rev.Path, RevisionEntryDelete, nil); err != nil {
				return nil, err
			}
			rev = nil
			continue
		}
	}
	temp, err := finalWriter.Finalize()
	if err != nil {
		return nil, WrapErrorf(err, "failed to finalize commit")
	}
	return temp, nil
}

// First, merge the staging snapshot with the revision snapshot (see `MergeWithSnapshot`).
// Then, create a new revision with the merged `RevisionEntryChunks`.
//
// Return `ErrEmptyCommit` if there are no changes.
func (s *Staging) Commit(repository *Repository, info *CommitInfo) (RevisionId, error) {
	revisionTemp, err := s.MergeWithSnapshot(repository)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to merge staging chunks")
	}
	if revisionTemp.Chunks() == 0 {
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
	revisionTempReader := revisionTemp.Reader(s.PathFilter)
	for i := range revisionTemp.Chunks() {
		data, err := revisionTempReader.ReadChunkRaw(i)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to read revision chunk file")
		}
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

func (s *Staging) revisionSnapshot(repository *Repository) (*RevisionTemp, error) {
	tmpDir := filepath.Join(s.tmpDir, "revision-snapshot")
	_ = os.RemoveAll(tmpDir)
	if err := os.MkdirAll(tmpDir, 0o700); err != nil {
		return nil, WrapErrorf(err, "failed to create temporary directory %s", tmpDir)
	}
	return NewRevisionSnapshot(repository, s.BaseRevision, tmpDir)
}
