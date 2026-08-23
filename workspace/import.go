// Import a directory into a repository without attaching it as a workspace.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cling-com/cling-sync/lib"
)

type ImportOptions struct {
	// The directory in the view that receives the contents of the source.
	Dest                   lib.Path
	Include                *lib.PathInclusionFilter
	Exclude                *lib.PathExclusionFilter
	StagingMonitor         StagingEntryMonitor
	CommitMonitor          CommitMonitor
	SnapshotMonitor        lib.RevisionSnapshotMonitor
	RestorableMetadataFlag lib.RestorableMetadataFlag
}

type Import struct {
	Changes StatusFiles
	// The view of `Dest`, which the source maps onto.
	view     *lib.RepositoryView
	src      lib.FS
	baseline *lib.ViewSnapshot
	entries  *lib.Temp[*lib.RevisionEntry]
	opts     *ImportOptions
	tmpFS    lib.FS
}

// Scan `src` and compute the changes `Commit` would write to the repository.
// Nothing is written until `Commit` is called.
func NewImport(
	ctx context.Context,
	view *lib.RepositoryView,
	src lib.FS,
	opts *ImportOptions,
	tmpFS lib.FS,
) (*Import, error) {
	view = view.Sub(opts.Dest)
	head, err := view.Repository.Head(ctx)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get repository head")
	}
	snapshotTmpFS, err := tmpFS.MkSub("snapshot")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary snapshot directory")
	}
	stagingTmpFS, err := tmpFS.MkSub("staging")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary staging directory")
	}
	baseline, err := view.NewSnapshot(ctx, head, snapshotTmpFS, opts.SnapshotMonitor)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	// A staging cache would have to live inside `src`, which is not ours to write to.
	staging, err := NewStaging(src, opts.Include, opts.Exclude, nil, stagingTmpFS, opts.StagingMonitor)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to scan %s", src)
	}
	// An import never removes anything: paths below `Dest` that the source
	// does not have are left as they are.
	entries, err := staging.MergeWithSnapshot(baseline, opts.RestorableMetadataFlag, true)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to merge staging and revision snapshot")
	}
	changes := StatusFiles{}
	reader := lib.NewDisplayOrderReader(entries.Reader(nil).Read)
	buf := lib.NewBlockBuf()
	for {
		entry, err := reader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read changes")
		}
		if entry.Kind == lib.RevisionEntryKindDelete {
			// Deletes are suppressed just above, an import never removes anything.
			panic(fmt.Sprintf("import must not delete %s", entry.Path))
		}
		// Paths are reported in the space of the caller's view, like every other command.
		changes = append(changes, StatusFile{opts.Dest.Join(entry.Path), entry.Kind, entry.Metadata})
	}
	return &Import{changes, view, src, baseline, entries, opts, tmpFS}, nil
}

// Commit the changes as a new revision.
// Return `lib.ErrEmptyCommit` if there is nothing to commit and
// `lib.ErrHeadChanged` if the repository moved on since the scan.
func (i *Import) Commit(ctx context.Context, info *lib.CommitInfo) (lib.RevisionId, error) {
	tmpFS, err := i.tmpFS.MkSub("commit")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create commit tmp dir")
	}
	src := &CommitFilesSrc{Src: i.src, Files: i.entries}
	dest := &CommitFilesDest{View: i.view, Snapshot: i.baseline}
	opts := &CommitFilesOptions{
		Author:                 info.Author,
		Message:                info.Message,
		Monitor:                i.opts.CommitMonitor,
		RestorableMetadataFlag: i.opts.RestorableMetadataFlag,
	}
	return CommitFiles(ctx, src, dest, opts, tmpFS)
}
