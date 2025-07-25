package workspace

import (
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

type StagingEntryMonitor interface {
	OnStart(path string, dirEntry fs.DirEntry)
	OnEnd(path string, excluded bool, metadata *lib.FileMetadata)
}

type Staging struct {
	PathFilter lib.PathFilter
	tempWriter *lib.RevisionTempWriter
	temp       *lib.RevisionTemp
	tmpFS      lib.FS
}

// Build a `Staging` from the `src` directory.
// `.cling` is always ignored.
func NewStaging(
	src lib.FS,
	pathFilter lib.PathFilter,
	tmp lib.FS,
	mon StagingEntryMonitor,
) (*Staging, error) {
	tempWriter := lib.NewRevisionTempWriter(tmp, lib.DefaultRevisionTempChunkSize)
	staging := &Staging{pathFilter, tempWriter, nil, tmp}
	err := src.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Base(path) == ".cling" {
			return filepath.SkipDir
		}
		fileInfo, err := d.Info()
		if err != nil {
			return lib.WrapErrorf(err, "failed to get file info for %s", path)
		}
		if !d.Type().IsRegular() && !d.Type().IsDir() {
			// todo: handle symlinks
			return nil
		}
		mon.OnStart(path, d)
		// Even though files are filtered out in Staging.Add, we still
		// want to eagerly exclude them to avoid unnecessary work (file hash).
		// Especially, we want to skip directories if they are excluded.
		if pathFilter != nil && !pathFilter.Include(path) {
			mon.OnEnd(path, true, nil)
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if path == "." {
			mon.OnEnd(path, true, nil)
			return nil
		}
		// todo: this might be insecure, perhaps we should use filepath.Split and
		// filepath.Clean directly in lib.NewPath.
		repoPath := lib.NewPath(strings.Split(path, lib.PathSeparator)...)
		var fileMetadata lib.FileMetadata
		fileMetadata, err = computeFileHash(src, path, fileInfo)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get metadata for %s", path)
		}
		_, err = staging.add(repoPath, &fileMetadata)
		if err != nil {
			return lib.WrapErrorf(err, "failed to add path %s to staging", path)
		}
		mon.OnEnd(path, false, &fileMetadata)
		return nil
	})
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to walk directory %s", src)
	}
	return staging, nil
}

func (s *Staging) Finalize() (*lib.RevisionTemp, error) {
	if s.temp == nil {
		t, err := s.tempWriter.Finalize()
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to finalize staging temp writer")
		}
		s.temp = t
	}
	return s.temp, nil
}

// Merge the staging snapshot with the revision snapshot.
// The resulting `RevisionTemp` will contain all entries that transition from the
// revision snapshot to the staging snapshot.
func (s *Staging) MergeWithSnapshot(snapshot *lib.RevisionSnapshot) (*lib.RevisionTemp, error) { //nolint:funlen
	stgTemp, err := s.Finalize()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to finalize staging temp writer")
	}
	revReader := snapshot.Reader(s.PathFilter)
	stgReader := stgTemp.Reader(s.PathFilter)
	final, err := s.tmpFS.MkSub("final")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create commit directory")
	}
	finalWriter := lib.NewRevisionTempWriter(final, lib.MaxBlockDataSize)
	add := func(path lib.Path, typ lib.RevisionEntryType, md *lib.FileMetadata) error {
		re, err := lib.NewRevisionEntry(path, typ, md)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create revision entry for path %s", path.FSString())
		}
		if err := finalWriter.Add(&re); err != nil {
			return lib.WrapErrorf(err, "failed to write revision entry for path %s", path.FSString())
		}
		return nil
	}
	var stg *lib.RevisionEntry
	var rev *lib.RevisionEntry
	for {
		if stg == nil {
			// Read the next staging entry.
			stg, err = stgReader.Read()
			if errors.Is(err, io.EOF) {
				// Write a delete for all remaining revision snapshot entries.
				for {
					if rev != nil { // The current one might be nil.
						// Write a delete.
						if err := add(rev.Path, lib.RevisionEntryDelete, rev.Metadata); err != nil {
							return nil, err
						}
					}
					rev, err = revReader.Read()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return nil, lib.WrapErrorf(err, "failed to read revision snapshot")
					}
				}
				break
			}
			if err != nil {
				return nil, lib.WrapErrorf(err, "failed to read staging snapshot")
			}
		}
		if rev == nil {
			// Read the next revision snapshot entry.
			rev, err = revReader.Read()
			if errors.Is(err, io.EOF) {
				// Write an add for all remaining staging entries.
				for {
					if stg != nil { // The current one might be nil.
						if err := add(stg.Path, lib.RevisionEntryAdd, stg.Metadata); err != nil {
							return nil, err
						}
					}
					stg, err = stgReader.Read()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return nil, lib.WrapErrorf(err, "failed to read staging snapshot")
					}
				}
				break
			}
			if err != nil {
				return nil, lib.WrapErrorf(err, "failed to read revision snapshot")
			}
		}
		c := lib.RevisionEntryPathCompare(stg, rev)
		if c == 0 { //nolint:gocritic
			if !stg.Metadata.IsEqualRestorableAttributes(rev.Metadata) {
				// Write an update.
				if err := add(stg.Path, lib.RevisionEntryUpdate, stg.Metadata); err != nil {
					return nil, err
				}
			}
			stg = nil
			rev = nil
		} else if c < 0 {
			// Write an add.
			if err := add(stg.Path, lib.RevisionEntryAdd, stg.Metadata); err != nil {
				return nil, err
			}
			stg = nil
			continue
		} else {
			// Write a delete.
			if err := add(rev.Path, lib.RevisionEntryDelete, rev.Metadata); err != nil {
				return nil, err
			}
			rev = nil
			continue
		}
	}
	temp, err := finalWriter.Finalize()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to finalize commit")
	}
	return temp, nil
}

// Return `true` if the file was added, `false` if it was ignored.
func (s *Staging) add(path lib.Path, md *lib.FileMetadata) (bool, error) {
	if md == nil {
		return false, lib.Errorf("file metadata is nil")
	}
	if s.tempWriter == nil {
		return false, lib.Errorf("staging is closed")
	}
	if s.PathFilter != nil && !s.PathFilter.Include(path.FSString()) {
		return false, nil
	}
	re, err := lib.NewRevisionEntry(path, lib.RevisionEntryAdd, md)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to create revision entry")
	}
	if err := s.tempWriter.Add(&re); err != nil {
		return false, err //nolint:wrapcheck
	}
	return true, nil
}

func computeFileHash(fs lib.FS, path string, fileInfo fs.FileInfo) (lib.FileMetadata, error) {
	if fileInfo.IsDir() {
		return lib.NewFileMetadataFromFileInfo(fileInfo, lib.Sha256{}, nil), nil
	}
	f, err := fs.OpenRead(path)
	if err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
	}
	defer f.Close() //nolint:errcheck
	fileHash := sha256.New()
	if _, err := io.Copy(fileHash, f); err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
	}
	return lib.NewFileMetadataFromFileInfo(fileInfo, lib.Sha256(fileHash.Sum(nil)), nil), nil
}
