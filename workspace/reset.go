package workspace

import (
	"io/fs"

	"github.com/flunderpero/cling-sync/lib"
)

type ResetOptions struct {
	RevisionId             lib.RevisionId
	Force                  bool
	StagingMonitor         StagingEntryMonitor
	CpMonitor              CpMonitor
	RestorableMetadataFlag lib.RestorableMetadataFlag
	UseStagingCache        bool
}

func (e ResetError) Error() string {
	return "Reset aborted due to local changes"
}

type ResetError struct {
	LocalChanges *lib.TempCache[lib.RevisionEntry]
}

// Reset the workspace to a specific revision.
// Return `ResetError` if there are local changes and `opts.Force` is not set.
func Reset(ws *Workspace, repository *lib.Repository, opts *ResetOptions) error {
	tempFS, err := ws.TempFS.MkSub("reset")
	if err != nil {
		return lib.WrapErrorf(err, "failed to create reset tmp dir")
	}
	defer tempFS.RemoveAll(".") //nolint:errcheck
	// todo: Refactor Merger and MergeOptions to suit both, Reset and Merge better.
	// todo: Actually, we should commit first in Merge and then Reset and move the code to Reset.
	mergeOptions := MergeOptions{
		StagingMonitor:         opts.StagingMonitor,
		CpMonitor:              opts.CpMonitor,
		CommitMonitor:          nil,
		Author:                 "unused",
		Message:                "unused",
		RestorableMetadataFlag: opts.RestorableMetadataFlag,
		UseStagingCache:        opts.UseStagingCache,
	}
	wsHead, staging, localChanges, _, err := buildLocalChanges(ws, tempFS, repository, &mergeOptions)
	if err != nil {
		return lib.WrapErrorf(err, "failed to build local changes")
	}
	if localChanges.Source.Chunks() > 0 {
		if !opts.Force {
			return ResetError{localChanges}
		}
	}
	// We ignore local changes.
	localChanges = nil
	remoteRevision, err := buildRemoteChanges(tempFS, repository, opts.RevisionId)
	if err != nil {
		return lib.WrapErrorf(err, "failed to build remote changes")
	}
	merger := &Merger{ws, wsHead, opts.RevisionId, tempFS, repository, make(map[string]fs.FileInfo), &mergeOptions}
	defer merger.restoreDirFileModes() //nolint:errcheck
	if err := merger.copyRepositoryFiles(remoteRevision.Source, staging, localChanges); err != nil {
		return lib.WrapErrorf(err, "failed to copy remote files")
	}
	if err := merger.deleteObsoleteWorkspaceFiles(remoteRevision, staging, localChanges); err != nil {
		return lib.WrapErrorf(err, "failed to delete obsolete workspace files")
	}
	if err := merger.restoreDirFileModes(); err != nil {
		return lib.WrapErrorf(err, "failed to restore file mode for directories")
	}
	if err := lib.WriteRef(ws.Storage, "head", opts.RevisionId); err != nil {
		return lib.WrapErrorf(err, "failed to write workspace head reference - please re-run reset")
	}
	return nil
}
