package workspace

import (
	"bytes"
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

type StagingOnError int

const (
	StagingOnErrorIgnore StagingOnError = 1
	StagingOnErrorAbort  StagingOnError = 2
)

type StagingEntryMonitor interface {
	OnStart(path string, dirEntry os.DirEntry)
	OnAddBlock(path string, header *lib.BlockHeader, existed bool, dataSize int64)
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
	head, err := repository.Head()
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get head")
	}
	snapshotDir := filepath.Join(tmpDir, "snapshot")
	stagingDir := filepath.Join(tmpDir, "staging")
	for _, dir := range []string{snapshotDir, stagingDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create temporary directory %s", dir)
		}
	}
	snapshot, err := lib.NewRevisionSnapshot(repository, head, snapshotDir)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	staging, err := NewStaging(src, repository, snapshot, opts.PathFilter, true, stagingDir, opts.Monitor)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to stage changes")
	}
	if err := opts.OnBeforeCommit(); err != nil {
		return lib.RevisionId{}, err
	}
	return staging.Commit( //nolint:wrapcheck
		repository,
		snapshot,
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
	snapshot *lib.RevisionSnapshot,
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
	// todo: using the clingFilter like this leads to the files in .cling being
	//       reported as excluded to `StaginEntryMonitor`.
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
	// todo: the 10 is a bit magical here. It is equivalent to 80MB of metadata.
	cache, err := lib.NewRevisionTempCache(&snapshot.RevisionTemp, 10)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision temp cache")
	}
	blockBuf := lib.BlockBuf{}
	err = filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if handleErr(path, err) != nil {
			return err
		}
		if !d.Type().IsRegular() && !d.Type().IsDir() {
			// todo: handle symlinks
			return nil
		}
		mon.OnStart(path, d)
		relPath, err := filepath.Rel(src, path)
		if handleErr(path, err) != nil {
			return lib.WrapErrorf(err, "failed to get relative path for %s", path)
		}
		// Even though files are filtered out in Staging.Add, we still
		// want to eagerly exclude them to avoid unnecessary work (encryption/file hash).
		// Especially, we want to skip directories if they are excluded.
		if !pathFilter.Include(relPath) {
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
		repoPath := lib.NewPath(strings.Split(relPath, string(os.PathSeparator))...)
		var fileMetadata lib.FileMetadata
		if addContents {
			entry, _, err := cache.Get(repoPath, d.IsDir())
			if err != nil {
				return lib.WrapErrorf(err, "failed to get entry from cache for %s", path)
			}
			fileMetadata, err = addToRepository(path, d, repository, entry, mon.OnAddBlock, blockBuf)
			if handleErr(path, err) != nil {
				return lib.WrapErrorf(err, "failed to add blocks and get metadata for %s", path)
			}
		} else {
			fileMetadata, err = computeFileHash(path, d)
			if handleErr(path, err) != nil {
				return lib.WrapErrorf(err, "failed to get metadata for %s", path)
			}
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

func computeFileHash(path string, d os.DirEntry) (lib.FileMetadata, error) {
	fileInfo, err := d.Info()
	if err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to get file info for %s", path)
	}
	if d.Type().IsDir() {
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

// Add the file contents to the repository and return the file metadata.
func addToRepository(
	path string,
	d os.DirEntry,
	repository *lib.Repository,
	entry *lib.RevisionEntry,
	onAddBlock func(path string, header *lib.BlockHeader, existed bool, dataSize int64),
	blockBuf lib.BlockBuf,
) (lib.FileMetadata, error) {
	fileInfo, err := d.Info()
	if err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to get file info for %s", path)
	}
	if d.Type().IsDir() {
		return newFileMetadata(fileInfo, lib.Sha256{}, nil), nil
	}
	// Fast path: If the size of the file did not change, only calculate the hash.
	// If the hash is the same, we can skip the whole block calculation.
	if entry != nil && entry.Metadata != nil && entry.Metadata.Size == fileInfo.Size() {
		md, err := computeFileHash(path, d)
		if err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to create file metadata")
		}
		if bytes.Equal(md.FileHash[:], entry.Metadata.FileHash[:]) {
			md.BlockIds = entry.Metadata.BlockIds
			return md, nil
		}
	}
	// todo: what about symlinks
	blockIds := []lib.BlockId{}
	fileHash := sha256.New()
	f, err := os.Open(path)
	if err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
	}
	defer f.Close() //nolint:errcheck
	// Read blocks and add them to the repository.
	cdc := NewGearCDCWithDefaults(f)
	for {
		data, err := cdc.Read(blockBuf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
		}
		if _, err := fileHash.Write(data); err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to update file hash")
		}
		existed, blockHeader, err := repository.WriteBlock(data, blockBuf)
		if err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to write block")
		}
		onAddBlock(path, &blockHeader, existed, int64(len(data)))
		blockIds = append(blockIds, blockHeader.BlockId)
	}
	return newFileMetadata(fileInfo, lib.Sha256(fileHash.Sum(nil)), blockIds), nil
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
