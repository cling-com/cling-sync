package workspace

import (
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"path/filepath"

	"github.com/flunderpero/cling-sync/lib"
)

type StagingEntryMonitor interface {
	OnStart(path lib.Path, dirEntry fs.DirEntry)
	OnEnd(path lib.Path, excluded bool, metadata *lib.FileMetadata)
}

type Staging struct {
	PathFilter lib.PathFilter
	pathPrefix lib.Path
	tempWriter *lib.RevisionTempWriter
	temp       *lib.RevisionTemp
	tmpFS      lib.FS
}

// Build a `Staging` from the `src` directory.
// `.cling` is always ignored.
// If `pathPrefix` is not empty, it will be prepended to all paths *after* the
// `pathFilter` is applied.
func NewStaging(
	src lib.FS,
	pathPrefix lib.Path,
	pathFilter lib.PathFilter,
	tmp lib.FS,
	mon StagingEntryMonitor,
) (*Staging, error) {
	tempWriter := lib.NewRevisionTempWriter(lib.RevisionId{}, tmp, lib.DefaultRevisionTempChunkSize)
	staging := &Staging{pathFilter, pathPrefix, tempWriter, nil, tmp}
	err := lib.WalkDirIgnore(src, ".", func(path_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path_ == "." {
			return nil
		}
		path, err := lib.NewPath(path_)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create path from %s", path_)
		}
		if path.Base().String() == ".cling" {
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
		var fileMetadata lib.FileMetadata
		fileMetadata, err = computeFileHash(src, path, fileInfo)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get metadata for %s", path)
		}
		oldPath := path
		path = pathPrefix.Join(path)
		_, err = staging.add(path, &fileMetadata)
		if err != nil {
			return lib.WrapErrorf(err, "failed to add path %s to staging (as %s)", oldPath, path)
		}
		mon.OnEnd(oldPath, false, &fileMetadata)
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
func (s *Staging) MergeWithSnapshot(snapshot *lib.RevisionTemp) (*lib.RevisionTemp, error) { //nolint:funlen
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
	finalWriter := lib.NewRevisionTempWriter(snapshot.RevisionId, final, lib.MaxBlockDataSize)
	add := func(path lib.Path, typ lib.RevisionEntryType, md *lib.FileMetadata) error {
		re, err := lib.NewRevisionEntry(path, typ, md)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create revision entry for path %s", path)
		}
		if err := finalWriter.Add(&re); err != nil {
			return lib.WrapErrorf(err, "failed to write revision entry for path %s", path)
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
	if s.PathFilter != nil && !s.PathFilter.Include(path) {
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

func computeFileHash(fs lib.FS, path lib.Path, fileInfo fs.FileInfo) (lib.FileMetadata, error) {
	if fileInfo.IsDir() {
		return lib.NewFileMetadataFromFileInfo(fileInfo, lib.Sha256{}, nil), nil
	}
	f, err := fs.OpenRead(path.String())
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
