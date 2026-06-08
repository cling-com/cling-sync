package workspace

import (
	"context"
	"errors"
	"io"

	"github.com/flunderpero/cling-sync/lib"
)

type CatOptions struct {
	RevisionId lib.RevisionId
	Path       lib.Path
}

// Cat writes the contents of a single regular file from the repository to w.
func Cat(ctx context.Context, repository *lib.Repository, w io.Writer, opts *CatOptions, tmpFS lib.FS) error {
	snapshot, err := lib.NewRevisionSnapshot(ctx, repository, opts.RevisionId, tmpFS)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(nil)
	buf := lib.NewBlockBuf()
	for {
		entry, err := reader.Read(buf)
		if errors.Is(err, io.EOF) {
			return lib.Errorf("file not found: %s", opts.Path)
		}
		if err != nil {
			return lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		if entry.Path != opts.Path {
			continue
		}
		if entry.Metadata.FileMode.IsDir() {
			return lib.Errorf("%s is a directory", opts.Path)
		}
		if entry.Metadata.FileMode.IsSymlink() {
			return lib.Errorf("%s is a symlink to %s", opts.Path, *entry.Metadata.SymLinkTarget)
		}
		for _, blockId := range entry.Metadata.BlockIds {
			data, err := repository.ReadBlock(ctx, blockId, buf)
			if err != nil {
				return lib.WrapErrorf(err, "failed to read block %s", blockId)
			}
			if _, err := w.Write(data); err != nil {
				return lib.WrapErrorf(err, "failed to write %s", opts.Path)
			}
		}
		return nil
	}
}
