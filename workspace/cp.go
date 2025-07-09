package workspace

import (
	"errors"
	"io"
	"io/fs"
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

func Cp(repository *lib.Repository, targetFS lib.FS, opts *CpOptions, tmpFS lib.FS) error {
	snapshot, err := lib.NewRevisionSnapshot(repository, opts.RevisionId, tmpFS)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(opts.PathFilter)
	mon := opts.Monitor
	directories := []*lib.RevisionEntry{}
	restoreDirFileModes := func() error {
		for _, entry := range directories {
			target := entry.Path.FSString()
			if err := restoreFileMode(targetFS, target, entry.Metadata); err != nil {
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
		target := entry.Path.FSString()
		mon.OnStart(entry, target)
		if err := restore(entry, repository, targetFS, target, mon); err != nil {
			return lib.WrapErrorf(err, "failed to copy %s", target)
		}
		if err := restoreFileMode(targetFS, target, entry.Metadata); err != nil {
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
				if err := targetFS.Chmod(target, mode|0o700); err != nil {
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

func restore( //nolint:funlen
	entry *lib.RevisionEntry,
	repository *lib.Repository,
	targetFS lib.FS,
	target string,
	mon CpMonitor,
) error {
	md := entry.Metadata
	if md.ModeAndPerm.IsDir() {
		if err := targetFS.MkdirAll(target); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				return nil
			}
			return lib.WrapErrorf(err, "failed to create directory %s", target)
		}
	} else {
		if err := targetFS.MkdirAll(filepath.Dir(target)); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				return nil
			}
			return lib.WrapErrorf(err, "failed to create parent directory %s", target)
		}
		f, err := targetFS.OpenWriteExcl(target)
		if errors.Is(err, fs.ErrExist) {
			switch mon.OnExists(entry, target) {
			case CpOnExistsOverwrite:
				f, err = targetFS.OpenWrite(target)
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
		for _, blockId := range entry.Metadata.BlockIds {
			data, _, err := repository.ReadBlock(blockId)
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
		if err := targetFS.Chmod(target, md.ModeAndPerm.AsFileMode()); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				return nil
			}
			return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.ModeAndPerm, target)
		}
	}
	return nil
}

func restoreFileMode(fs lib.FS, path string, md *lib.FileMetadata) error {
	if err := fs.Chmod(path, (md.ModeAndPerm & lib.ModePerm).AsFileMode()); err != nil {
		return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.ModeAndPerm, path)
	}
	if md.HasUID() && md.HasGID() {
		if err := fs.Chown(path, int(md.UID), int(md.GID)); err != nil {
			return lib.WrapErrorf(err, "failed to restore file owner %d and group %d for %s", md.GID, md.UID, path)
		}
	}
	mtime := time.Unix(md.MTimeSec, int64(md.MTimeNSec))
	if err := fs.Chmtime(path, mtime); err != nil {
		return lib.WrapErrorf(err, "failed to restore mtime %s for %s", mtime, path)
	}
	// todo: handle birthtime or allow the user to use birthtime instead of mtime.
	return nil
}
