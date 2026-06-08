package workspace

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"path/filepath"

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
	OnStart(entry *lib.RevisionEntry, targetPath string) error
	OnExists(entry *lib.RevisionEntry, targetPath string) CpOnExists
	OnWrite(entry *lib.RevisionEntry, targetPath string, blockId lib.BlockId, data []byte) error
	OnEnd(entry *lib.RevisionEntry, targetPath string) error
	OnError(entry *lib.RevisionEntry, targetPath string, err error) CpOnError
}

type CpOptions struct {
	RevisionId             lib.RevisionId
	Monitor                CpMonitor
	PathFilter             lib.PathFilter
	PathPrefix             lib.Path
	RestorableMetadataFlag lib.RestorableMetadataFlag
}

func Cp( //nolint:funlen
	ctx context.Context,
	repository *lib.Repository,
	targetFS lib.FS,
	opts *CpOptions,
	tmpFS lib.FS,
) error {
	snapshot, err := lib.NewRevisionSnapshot(ctx, repository, opts.RevisionId, tmpFS)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(nil)
	mon := opts.Monitor
	// Directory modes are restored last, after their contents. We carry the
	// prefix-relative restore target because the entry itself is left untouched.
	type restorableDir struct {
		md     *lib.PathMetadata
		target string
	}
	directories := []restorableDir{}
	restoreDirFileModes := func() error {
		for _, dir := range directories {
			if err := restoreFileMode(targetFS, dir.target, dir.md, opts.RestorableMetadataFlag); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", dir.md.FileMode, dir.target)
			}
		}
		return nil
	}
	defer restoreDirFileModes() //nolint:errcheck
	buf := lib.NewBlockBuf()
	for {
		entry, err := reader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		// Match the filter and restore under the prefix-relative path the user
		// sees.
		path, ok := entry.Path.TrimBase(opts.PathPrefix)
		if !ok {
			continue
		}
		if opts.PathFilter != nil && !opts.PathFilter.Include(path, entry.Metadata.FileMode.IsDir()) {
			continue
		}
		target := path.String()
		if err := mon.OnStart(entry, target); err != nil {
			return lib.WrapErrorf(err, "cp monitor start failed for %s", target)
		}
		if err := restore(ctx, entry, repository, targetFS, target, buf, mon); err != nil {
			return lib.WrapErrorf(err, "failed to copy %s", target)
		}
		if err := restoreFileMode(targetFS, target, &entry.Metadata, opts.RestorableMetadataFlag); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				if endErr := mon.OnEnd(entry, target); endErr != nil {
					return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
				}
				continue
			}
			return lib.WrapErrorf(err, "failed to restore file mode %s for %s", entry.Metadata.FileMode, target)
		}
		mode := entry.Metadata.FileMode.AsFsFileMode()
		if mode.IsDir() {
			// Temporarily change the permissions if the directory is not writable.
			if mode&0o700 != 0o700 {
				if err := targetFS.Chmod(target, mode|0o700); err != nil {
					if mon.OnError(entry, target, err) == CpOnErrorIgnore {
						if endErr := mon.OnEnd(entry, target); endErr != nil {
							return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
						}
						continue
					}
					return lib.WrapErrorf(err, "failed to change permissions of %s", target)
				}
				directories = append(directories, restorableDir{&entry.Metadata, target})
			}
		}
		if err := mon.OnEnd(entry, target); err != nil {
			return lib.WrapErrorf(err, "cp monitor end failed for %s", target)
		}
	}
	if err := restoreDirFileModes(); err != nil {
		return lib.WrapErrorf(err, "failed to restore file mode for directories")
	}
	directories = nil // Make sure the deferred function does not restore the file modes twice.
	return nil
}

func restore( //nolint:funlen
	ctx context.Context,
	entry *lib.RevisionEntry,
	repository *lib.Repository,
	targetFS lib.FS,
	target string,
	buf lib.BlockBuf,
	mon CpMonitor,
) error {
	md := entry.Metadata
	localInfo, statErr := targetFS.Stat(target)
	if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		return lib.WrapErrorf(statErr, "failed to stat %s", target)
	}
	if statErr == nil {
		// Delete if type changed (dir vs symlink vs file).
		localMode := lib.NewFileMode(localInfo.Mode())
		if localMode.IsDir() != md.FileMode.IsDir() || localMode.IsSymlink() != md.FileMode.IsSymlink() {
			switch mon.OnExists(entry, target) {
			case CpOnExistsOverwrite:
				var removeErr error
				if localMode.IsDir() {
					removeErr = targetFS.RemoveAll(target)
				} else {
					removeErr = targetFS.Remove(target)
				}
				if removeErr != nil {
					return lib.WrapErrorf(removeErr, "failed to remove existing %s", target)
				}
			case CpOnExistsIgnore:
				if endErr := mon.OnEnd(entry, target); endErr != nil {
					return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
				}
				return nil
			case CpOnExistsAbort:
				return lib.Errorf("%s already exists with a different kind", target)
			}
		}
	}
	if md.FileMode.IsSymlink() {
		return restoreSymlink(entry, targetFS, target, mon)
	}
	if md.FileMode.IsDir() {
		if err := targetFS.MkdirAll(target); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				if endErr := mon.OnEnd(entry, target); endErr != nil {
					return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
				}
				return nil
			}
			return lib.WrapErrorf(err, "failed to create directory %s", target)
		}
		return nil
	}
	if err := targetFS.MkdirAll(filepath.Dir(target)); err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
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
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		case CpOnExistsAbort:
			return lib.WrapErrorf(err, "failed to open file %s for writing", target)
		}
	}
	if err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		}
		return lib.WrapErrorf(err, "failed to open file %s for writing", target)
	}
	defer f.Close() //nolint:errcheck
	for _, blockId := range entry.Metadata.BlockIds {
		data, err := repository.ReadBlock(ctx, blockId, buf)
		if err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				if endErr := mon.OnEnd(entry, target); endErr != nil {
					return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
				}
				return nil
			}
			return lib.WrapErrorf(err, "failed to read block %s", blockId)
		}
		if _, err := f.Write(data); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				if endErr := mon.OnEnd(entry, target); endErr != nil {
					return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
				}
				return nil
			}
			return lib.WrapErrorf(err, "failed to write block %s", blockId)
		}
		if err := mon.OnWrite(entry, target, blockId, data); err != nil {
			return lib.WrapErrorf(err, "cp monitor write failed for %s", target)
		}
	}
	if err := f.Close(); err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		}
		return lib.WrapErrorf(err, "failed to close file %s", target)
	}
	if err := targetFS.Chmod(target, md.FileMode.AsFsFileMode()); err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		}
		return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.FileMode, target)
	}
	return nil
}

func restoreSymlink(entry *lib.RevisionEntry, targetFS lib.FS, target string, mon CpMonitor) error {
	md := entry.Metadata
	if md.SymLinkTarget == nil {
		return lib.Errorf("symlink %s has no target", entry.Path)
	}
	if err := targetFS.MkdirAll(filepath.Dir(target)); err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		}
		return lib.WrapErrorf(err, "failed to create parent directory %s", target)
	}
	linkStr, err := filepath.Rel(filepath.Dir(entry.Path.String()), md.SymLinkTarget.String())
	if err != nil {
		return lib.WrapErrorf(err, "failed to compute symlink string for %s", target)
	}
	linkStr = filepath.ToSlash(linkStr)
	err = targetFS.Symlink(linkStr, target)
	if errors.Is(err, fs.ErrExist) {
		switch mon.OnExists(entry, target) {
		case CpOnExistsOverwrite:
			if rmErr := targetFS.Remove(target); rmErr != nil {
				return lib.WrapErrorf(rmErr, "failed to remove existing %s", target)
			}
			err = targetFS.Symlink(linkStr, target)
		case CpOnExistsIgnore:
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		case CpOnExistsAbort:
			return lib.WrapErrorf(err, "failed to create symlink %s", target)
		}
	}
	if err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		}
		return lib.WrapErrorf(err, "failed to create symlink %s", target)
	}
	return nil
}

func restoreFileMode(
	fs lib.FS,
	path string,
	md *lib.PathMetadata,
	restorableMetadataFlag lib.RestorableMetadataFlag,
) error {
	isSymlink := md.FileMode.IsSymlink()
	// Chmod and Chown follow symlinks on most platforms, and we never want
	// to mutate the target. Skip them for links regardless of the flag.
	if !isSymlink {
		if md.HasUID() && md.HasGID() && restorableMetadataFlag&lib.RestorableMetadataOwnership != 0 {
			if err := fs.Chown(path, int(*md.Uid), int(*md.Gid)); err != nil {
				return lib.WrapErrorf(
					err,
					"failed to restore file owner %d and group %d for %s",
					*md.Uid,
					*md.Gid,
					path,
				)
			}
		}
		if restorableMetadataFlag&lib.RestorableMetadataMode != 0 {
			if err := fs.Chmod(path, (md.FileMode & lib.FileModePerm).AsFsFileMode()); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.FileMode, path)
			}
		}
	}
	if restorableMetadataFlag&lib.RestorableMetadataMTime != 0 {
		mtime := md.MTime()
		if err := fs.Chmtime(path, mtime); err != nil {
			return lib.WrapErrorf(err, "failed to restore mtime %s for %s", mtime, path)
		}
	}
	// todo: handle birthtime or allow the user to use birthtime instead of mtime.
	return nil
}
