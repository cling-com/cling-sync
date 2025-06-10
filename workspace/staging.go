package workspace

import (
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/flunderpero/cling-sync/lib"
)

type StagingEntryMonitor interface {
	OnStart(path string, dirEntry os.DirEntry)
	OnEnd(path string, excluded bool, metadata *lib.FileMetadata)
}

type Staging struct {
	PathFilter lib.PathFilter
	tempWriter *lib.RevisionTempWriter
	temp       *lib.RevisionTemp
	tmpDir     string
}

// Build a `Staging` from the `src` directory.
// `.cling` is always ignored.
func NewStaging(
	src string,
	pathFilter lib.PathFilter,
	tmpDir string,
	mon StagingEntryMonitor,
) (*Staging, error) {
	tempWriter := lib.NewRevisionTempWriter(tmpDir, lib.DefaultRevisionTempChunkSize)
	staging := &Staging{pathFilter, tempWriter, nil, tmpDir}
	err := filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
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
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get relative path for %s", path)
		}
		// Even though files are filtered out in Staging.Add, we still
		// want to eagerly exclude them to avoid unnecessary work (file hash).
		// Especially, we want to skip directories if they are excluded.
		if pathFilter != nil && !pathFilter.Include(relPath) {
			mon.OnEnd(path, true, nil)
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if relPath == "." {
			mon.OnEnd(path, true, nil)
			return nil
		}
		// todo: this might be insecure, perhaps we should use filepath.Split and
		// filepath.Clean directly in lib.NewPath.
		repoPath := lib.NewPath(strings.Split(relPath, string(os.PathSeparator))...)
		var fileMetadata lib.FileMetadata
		fileMetadata, err = computeFileHash(path, fileInfo)
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
	final := filepath.Join(s.tmpDir, "final")
	if err := os.MkdirAll(final, 0o700); err != nil {
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

func computeFileHash(path string, fileInfo os.FileInfo) (lib.FileMetadata, error) {
	if fileInfo.IsDir() {
		return newFileMetadata(fileInfo, lib.Sha256{}, nil), nil
	}
	f, err := os.Open(path)
	if err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
	}
	defer f.Close() //nolint:errcheck
	fileHash := sha256.New()
	if _, err := io.Copy(fileHash, f); err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
	}
	return newFileMetadata(fileInfo, lib.Sha256(fileHash.Sum(nil)), nil), nil
}

func newFileMetadata(fileInfo fs.FileInfo, fileHash lib.Sha256, blockIds []lib.BlockId) lib.FileMetadata {
	mtime := fileInfo.ModTime().UTC()
	fileSys := fileInfo.Sys()
	var gid uint32 = 0xffffffff
	var uid uint32 = 0xffffffff
	var birthtimeSec int64 = -1
	var birthtimeNSec int32 = -1
	if stat, ok := fileSys.(*syscall.Stat_t); ok {
		gid = stat.Gid
		uid = stat.Uid
		birthtimeSec = stat.Birthtimespec.Sec
		birthtimeNSec = int32(stat.Birthtimespec.Nsec) //nolint:gosec
	}
	var size int64
	if !fileInfo.IsDir() {
		size = fileInfo.Size()
	}
	md := lib.FileMetadata{
		ModeAndPerm: lib.NewModeAndPerm(fileInfo.Mode()),
		MTimeSec:    mtime.Unix(),
		MTimeNSec:   int32(mtime.Nanosecond()), //nolint:gosec
		Size:        size,
		FileHash:    fileHash,
		BlockIds:    blockIds,

		SymlinkTarget: "", // todo: handle symlinks

		UID:           uid,
		GID:           gid,
		BirthtimeSec:  birthtimeSec,
		BirthtimeNSec: birthtimeNSec,
	}
	return md
}
