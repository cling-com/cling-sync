package workspace

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

type CpOnError int

const (
	CpOnErrorIgnore CpOnError = 1
	CpOnErrorAbort  CpOnError = 2
)

type CpOnExists int

const (
	CpOnExistsAbort     CpOnExists = 1
	CpOnExistsIgnore    CpOnExists = 2
	CpOnExistsOverwrite CpOnExists = 3
)

type CpMonitor interface {
	OnStart(entry *lib.RevisionEntry, targetPath string)
	OnExists(entry *lib.RevisionEntry, targetPath string) CpOnExists
	OnWrite(entry *lib.RevisionEntry, targetPath string, blockId lib.BlockId, data []byte)
	OnEnd(entry *lib.RevisionEntry, targetPath string)
	OnError(entry *lib.RevisionEntry, targetPath string, err error) CpOnError
}

type CpOptions struct {
	RevisionId lib.RevisionId
	Monitor    CpMonitor
	PathFilter lib.PathFilter
}

func Cp(src string, repository *lib.Repository, targetPath string, opts *CpOptions, tmpDir string) error {
	snapshot, err := lib.NewRevisionSnapshot(repository, opts.RevisionId, tmpDir)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(opts.PathFilter)
	mon := opts.Monitor
	targetPath = filepath.Clean(targetPath)
	directories := []*lib.RevisionEntry{}
	restoreDirFileModes := func() error {
		for _, entry := range directories {
			target := filepath.Join(targetPath, entry.Path.FSString())
			if err := restoreFileMode(target, entry.Metadata); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", entry.Metadata.ModeAndPerm, target)
			}
		}
		return nil
	}
	defer restoreDirFileModes() //nolint:errcheck
	for {
		entry, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		target := filepath.Join(targetPath, entry.Path.FSString())
		mon.OnStart(entry, target)
		if err := restore(entry, repository, target, mon); err != nil {
			return lib.WrapErrorf(err, "failed to restore %s", target)
		}
		if err := restoreFileMode(target, entry.Metadata); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				continue
			}
			return lib.WrapErrorf(err, "failed to restore file mode %s for %s", entry.Metadata.ModeAndPerm, target)
		}
		mode := entry.Metadata.ModeAndPerm.AsFileMode()
		if mode.IsDir() {
			// Temporarily change the permissions if the directory is not writable.
			if mode&0o700 != 0o700 {
				if err := os.Chmod(target, mode|0o700); err != nil {
					if mon.OnError(entry, target, err) == CpOnErrorIgnore {
						mon.OnEnd(entry, target)
						continue
					}
					return lib.WrapErrorf(err, "failed to change permissions of %s", target)
				}
				directories = append(directories, entry)
			}
		}
		mon.OnEnd(entry, target)
	}
	if err := restoreDirFileModes(); err != nil {
		return lib.WrapErrorf(err, "failed to restore file mode for directories")
	}
	directories = nil // Make sure the deferred function does not restore the file modes twice.
	return nil
}

func restore(entry *lib.RevisionEntry, repository *lib.Repository, target string, mon CpMonitor) error { //nolint:funlen
	md := entry.Metadata
	if md.ModeAndPerm.IsDir() {
		if err := os.MkdirAll(target, 0o700); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				return nil
			}
			return lib.WrapErrorf(err, "failed to create directory %s", target)
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				return nil
			}
			return lib.WrapErrorf(err, "failed to create parent directory %s", target)
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, md.ModeAndPerm.AsFileMode())
		if errors.Is(err, os.ErrExist) {
			switch mon.OnExists(entry, target) {
			case CpOnExistsOverwrite:
				f, err = os.OpenFile(target, os.O_CREATE|os.O_WRONLY, md.ModeAndPerm.AsFileMode())
			case CpOnExistsIgnore:
				mon.OnEnd(entry, target)
				return nil
			case CpOnExistsAbort:
				return lib.WrapErrorf(err, "failed to open file %s for writing", target)
			}
		}
		if err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				return nil
			}
			return lib.WrapErrorf(err, "failed to open file %s for writing", target)
		}
		defer f.Close() //nolint:errcheck
		blockBuf := lib.BlockBuf{}
		for _, blockId := range entry.Metadata.BlockIds {
			data, _, err := repository.ReadBlock(blockId, blockBuf)
			if err != nil {
				if mon.OnError(entry, target, err) == CpOnErrorIgnore {
					mon.OnEnd(entry, target)
					return nil
				}
				return lib.WrapErrorf(err, "failed to read block %s", blockId)
			}
			if _, err := f.Write(data); err != nil {
				if mon.OnError(entry, target, err) == CpOnErrorIgnore {
					mon.OnEnd(entry, target)
					return nil
				}
				return lib.WrapErrorf(err, "failed to write block %s", blockId)
			}
			mon.OnWrite(entry, target, blockId, data)
		}
		if err := f.Close(); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				return nil
			}
			return lib.WrapErrorf(err, "failed to close file %s", target)
		}
	}
	return nil
}

func restoreFileMode(path string, md *lib.FileMetadata) error {
	if err := os.Chmod(path, md.ModeAndPerm.AsFileMode()&os.ModePerm); err != nil {
		return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.ModeAndPerm, path)
	}
	if md.HasUID() && md.HasGID() {
		if err := os.Chown(path, int(md.UID), int(md.GID)); err != nil {
			return lib.WrapErrorf(err, "failed to restore file owner %d and group %d for %s", md.GID, md.UID, path)
		}
	}
	mtime := time.Unix(md.MTimeSec, int64(md.MTimeNSec))
	if err := os.Chtimes(path, time.Time{}, mtime); err != nil {
		return lib.WrapErrorf(err, "failed to restore mtime %s for %s", mtime, path)
	}
	// todo: handle birthtime or allow the user to use birthtime instead of mtime.
	return nil
}
