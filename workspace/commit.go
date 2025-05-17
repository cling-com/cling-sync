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

type CommitConfig struct {
	PathFilter lib.PathFilter
	Author     string
	Message    string
}

// Commit all changes in the local directory.
// `.cling` is always ignored.
func Commit(src string, repository *lib.Repository, config *CommitConfig) (lib.RevisionId, error) { //nolint:funlen
	head, err := repository.Head()
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get head")
	}
	tmpDir, err := os.MkdirTemp(os.TempDir(), "cling-sync")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create temporary directory")
	}
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir) //nolint:errcheck
	stagingTmpDir := filepath.Join(tmpDir, "staging")
	snapshotTmpDir := filepath.Join(tmpDir, "snapshot")
	for _, d := range []string{stagingTmpDir, snapshotTmpDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to temporary directory %s", d)
		}
	}
	// todo: We should rename `ignore` to `exclude` and add `include`
	//		 patterns that override the exclude patterns.
	clingPattern, err := lib.NewPathPattern(".cling")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create path pattern")
	}
	clingFilter := &lib.PathExclusionFilter{Excludes: []lib.PathPattern{clingPattern}, Includes: nil}
	var pathFilter lib.PathFilter
	if config.PathFilter != nil {
		pathFilter = &lib.AllPathFilter{Filters: []lib.PathFilter{config.PathFilter, clingFilter}}
	} else {
		pathFilter = clingFilter
	}

	// Stage all files.
	staging, err := lib.NewStaging(head, pathFilter, stagingTmpDir)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create staging")
	}
	err = filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(src, path)
		// Even though files are filtered out in Staging.Add, we still
		// want to eagerly exclude them to avoid unnecessary work (encryption/file hash).
		if !pathFilter.Include(relPath) {
			return nil
		}
		if err != nil {
			return lib.WrapErrorf(err, "failed to get relative path for %s", path)
		}
		if relPath == "." {
			return nil
		}
		repoPath := lib.NewPath(strings.Split(relPath, string(os.PathSeparator))...)
		fileMetadata, err := addContentToRepo(path, d, repository)
		if err != nil {
			return lib.WrapErrorf(err, "failed to add path %s to repository", path)
		}
		if _, err := staging.Add(repoPath, &fileMetadata); err != nil {
			return lib.WrapErrorf(err, "failed to add path %s to staging", path)
		}
		return nil
	})
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to walk directory %s", src)
	}
	snapshot, err := lib.NewRevisionSnapshot(repository, head, snapshotTmpDir)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	// Create commit.
	return staging.Commit( //nolint:wrapcheck
		repository,
		snapshot,
		&lib.CommitInfo{Author: config.Author, Message: config.Message},
	)
}

func addContentToRepo(path string, d os.DirEntry, repo *lib.Repository) (lib.FileMetadata, error) {
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
		buf := [lib.MaxBlockDataSize]byte{}
		blockBuf := lib.BlockBuf{}
		// Read blocks and add them to the repository.
		for {
			n, err := f.Read(buf[:])
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
			}
			fileSize += int64(n)
			if _, err := fileHash.Write(buf[:n]); err != nil {
				return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to update file hash")
			}
			_, blockHeader, err := repo.WriteBlock(buf[:n], blockBuf)
			if err != nil {
				return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to write block")
			}
			blockIds = append(blockIds, blockHeader.BlockId)
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
