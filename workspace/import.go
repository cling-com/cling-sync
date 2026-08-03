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
	// The subtree `Dest` is relative to, and the space `Changes` is reported in.
	PathPrefix lib.Path
	// The directory below `PathPrefix` that receives the contents of the source.
	Dest                   lib.Path
	Include                *lib.PathInclusionFilter
	Exclude                *lib.PathExclusionFilter
	StagingMonitor         StagingEntryMonitor
	CommitMonitor          CommitMonitor
	SnapshotMonitor        lib.RevisionSnapshotMonitor
	RestorableMetadataFlag lib.RestorableMetadataFlag
}

type Import struct {
	Changes    StatusFiles
	repository *lib.Repository
	src        lib.FS
	dest       lib.Path
	head       lib.RevisionId
	baseline   *lib.TempCache[*lib.RevisionEntry]
	entries    *lib.Temp[*lib.RevisionEntry]
	opts       *ImportOptions
	tmpFS      lib.FS
}

// Scan `src` and compute the changes `Commit` would write to the repository.
// Nothing is written until `Commit` is called.
func NewImport(
	ctx context.Context,
	repository *lib.Repository,
	src lib.FS,
	opts *ImportOptions,
	tmpFS lib.FS,
) (*Import, error) {
	head, err := repository.Head(ctx)
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
	snapshot, err := lib.NewRevisionSnapshot(ctx, repository, head, snapshotTmpFS, opts.SnapshotMonitor)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	baseline, err := lib.NewRevisionEntryTempCache(snapshot, 10)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision temp cache")
	}
	dest := opts.PathPrefix.Join(opts.Dest)
	// A staging cache would have to live inside `src`, which is not ours to write to.
	staging, err := NewStaging(src, dest, opts.Include, opts.Exclude, nil, stagingTmpFS, opts.StagingMonitor)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to scan %s", src)
	}
	// An import never removes anything: paths below `PathPrefix` that the source
	// does not have are left as they are.
	entries, err := staging.MergeWithSnapshot(snapshot, opts.RestorableMetadataFlag, true)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to merge staging and revision snapshot")
	}
	changes := StatusFiles{}
	reader := entries.Reader(nil)
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
		// Paths are reported relative to the prefix, like every other command.
		path, ok := entry.Path.TrimBase(opts.PathPrefix)
		if !ok {
			continue
		}
		changes = append(changes, StatusFile{path, entry.Kind, entry.Metadata})
	}
	return &Import{changes, repository, src, dest, head, baseline, entries, opts, tmpFS}, nil
}

// Commit the changes as a new revision.
// Return `lib.ErrEmptyCommit` if there is nothing to commit and
// `lib.ErrHeadChanged` if the repository moved on since the scan.
func (i *Import) Commit(ctx context.Context, info *lib.CommitInfo) (lib.RevisionId, error) {
	tmpFS, err := i.tmpFS.MkSub("commit")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create commit tmp dir")
	}
	src := &CommitFilesSrc{Src: i.src, SrcPrefix: i.dest, Files: i.entries}
	dest := &CommitFilesDest{Repository: i.repository, RevisionId: i.head, Snapshot: i.baseline}
	opts := &CommitFilesOptions{
		Author:                 info.Author,
		Message:                info.Message,
		Monitor:                i.opts.CommitMonitor,
		RestorableMetadataFlag: i.opts.RestorableMetadataFlag,
	}
	return CommitFiles(ctx, src, dest, opts, tmpFS)
}
