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
	Ignore  []lib.PathPattern
	Author  string
	Message string
}

func Commit(src string, repository *lib.Repository, config *CommitConfig) (lib.RevisionId, error) {
	head, err := repository.Head()
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get head")
	}
	tmpDir := filepath.Join(os.TempDir(), "cling-sync")
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()
	stagingTmpDir := filepath.Join(tmpDir, "staging")
	snapshotTmpDir := filepath.Join(tmpDir, "snapshot")
	for _, d := range []string{stagingTmpDir, snapshotTmpDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to temporary directory %s", d)
		}
	}
	// Stage all files.
	staging, err := lib.NewStaging(head, stagingTmpDir)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create staging")
	}
	err = filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		for _, pattern := range config.Ignore {
			if pattern.Match(path) {
				return nil
			}
		}
		repoPath := lib.NewPath(strings.Split(path, string(os.PathSeparator))...)
		fileMetadata, err := addContentToRepo(path, d, repository)
		if err != nil {
			return lib.WrapErrorf(err, "failed to add path %s to repository", path)
		}
		if err := staging.Add(repoPath, &fileMetadata); err != nil {
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
		// todo: make sure they are compatible.
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
