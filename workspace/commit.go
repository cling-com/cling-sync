package workspace

import (
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/flunderpero/cling-sync/lib"
)

type StagingOnError int

const (
	StagingOnErrorIgnore StagingOnError = 1
	StagingOnErrorAbort  StagingOnError = 2
)

type StagingEntryMonitor interface {
	OnStart(path string, dirEntry os.DirEntry)
	OnAddBlock(path string, header *lib.BlockHeader, existed bool, dataSize int)
	OnError(path string, err error) StagingOnError
	OnEnd(path string, excluded bool, metadata *lib.FileMetadata)
}

type CommitOptions struct {
	PathFilter     lib.PathFilter
	Author         string
	Message        string
	Monitor        StagingEntryMonitor
	OnBeforeCommit func() error
}

// Commit all changes in the local directory.
// `.cling` is always ignored.
func Commit(src string, repository *lib.Repository, opts *CommitOptions, tmpDir string) (lib.RevisionId, error) {
	staging, err := NewStaging(src, repository, opts.PathFilter, true, tmpDir, opts.Monitor)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to stage changes")
	}
	if err := opts.OnBeforeCommit(); err != nil {
		return lib.RevisionId{}, err
	}
	return staging.Commit( //nolint:wrapcheck
		repository,
		&lib.CommitInfo{Author: opts.Author, Message: opts.Message},
	)
}

// Build a `lib.Staging` from the `src` directory.
// `.cling` is always ignored.
//
// Parameters:
//   - `addContents`: Whether to add the contents of the `src` directory to the repository.
func NewStaging( //nolint:funlen
	src string,
	repository *lib.Repository,
	pathFilter lib.PathFilter,
	addContents bool,
	tmpDir string,
	mon StagingEntryMonitor,
) (*lib.Staging, error) {
	head, err := repository.Head()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get head")
	}
	clingPattern, err := lib.NewPathPattern(".cling")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create path pattern")
	}
	clingFilter := &lib.PathExclusionFilter{Excludes: []lib.PathPattern{clingPattern}, Includes: nil}
	if pathFilter != nil {
		pathFilter = &lib.AllPathFilter{Filters: []lib.PathFilter{pathFilter, clingFilter}}
	} else {
		pathFilter = clingFilter
	}
	handleErr := func(path string, err error) error {
		if err == nil || mon.OnError(path, err) == StagingOnErrorIgnore {
			return nil
		}
		return err
	}

	// Stage all files.
	staging, err := lib.NewStaging(head, pathFilter, tmpDir)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create staging")
	}
	err = filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if handleErr(path, err) != nil {
			return err
		}
		mon.OnStart(path, d)
		relPath, err := filepath.Rel(src, path)
		if handleErr(path, err) != nil {
			return lib.WrapErrorf(err, "failed to get relative path for %s", path)
		}
		// Even though files are filtered out in Staging.Add, we still
		// want to eagerly exclude them to avoid unnecessary work (encryption/file hash).
		if !pathFilter.Include(relPath) {
			mon.OnEnd(path, true, nil)
			return nil
		}
		if relPath == "." {
			mon.OnEnd(path, true, nil)
			return nil
		}
		repoPath := lib.NewPath(strings.Split(relPath, string(os.PathSeparator))...)
		fileMetadata, err := processDirEntry(path, d, repository, mon.OnAddBlock, addContents)
		if handleErr(path, err) != nil {
			return lib.WrapErrorf(err, "failed to add path %s to repository", path)
		}
		_, err = staging.Add(repoPath, &fileMetadata)
		if handleErr(path, err) != nil {
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

// Calculate the file hash and add the file contents to the repository (if `writeBlocks` is `true`).
//
// Parameters:
//   - `writeBlocks`: Whether to write the file contents to the repository.
//     If `false`, `FileMetadata.BlockIds` will be empty.
func processDirEntry( //nolint:funlen
	path string,
	d os.DirEntry,
	repo *lib.Repository,
	onAddBlock func(path string, header *lib.BlockHeader, existed bool, dataSize int),
	writeBlocks bool,
) (lib.FileMetadata, error) {
	// todo: what about symlinks
	fileHash := sha256.New()
	var fileSize int64
	fileInfo, err := d.Info()
	if err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to get file info for %s", path)
	}
	blockIds := []lib.BlockId{}
	if d.Type().IsRegular() {
		f, err := os.Open(path)
		if err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
		}
		defer f.Close() //nolint:errcheck
		blockBuf := lib.BlockBuf{}
		// Read blocks and add them to the repository.
		cdc := NewGearCDCWithDefaults(f)
		for {
			data, err := cdc.Read(blockBuf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
			}
			fileSize += int64(len(data))
			if _, err := fileHash.Write(data); err != nil {
				return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to update file hash")
			}
			if writeBlocks {
				existed, blockHeader, err := repo.WriteBlock(data, blockBuf)
				if err != nil {
					return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to write block")
				}
				onAddBlock(path, &blockHeader, existed, len(data))
				blockIds = append(blockIds, blockHeader.BlockId)
			}
		}
	}
	// Create RevisionEntry.
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
	md := lib.FileMetadata{
		ModeAndPerm: lib.NewModeAndPerm(fileInfo.Mode()),
		MTimeSec:    mtime.Unix(),
		MTimeNSec:   int32(mtime.Nanosecond()), //nolint:gosec
		Size:        fileSize,
		FileHash:    lib.Sha256(fileHash.Sum(nil)),
		BlockIds:    blockIds,

		SymlinkTarget: "", // todo: handle symlinks

		UID:           uid,
		GID:           gid,
		BirthtimeSec:  birthtimeSec,
		BirthtimeNSec: birthtimeNSec,
	}
	return md, nil
}
