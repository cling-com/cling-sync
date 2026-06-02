// Merge changes from the repository into the workspace and vice versa.
package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	ErrUpToDate      = lib.Errorf("workspace is up to date")
	ErrRemoteChanged = lib.Errorf("remote repository has changed during merge")
)

type CommitMonitor interface {
	OnStart(entry *lib.RevisionEntry) error
	// bytesWritten: if nil, the block already existed; otherwise, the total block size (including
	// header) written.
	OnAddBlock(entry *lib.RevisionEntry, blockId lib.BlockId, dataSize int, bytesWritten *int) error
	OnEnd(entry *lib.RevisionEntry) error
	OnBeforeCommit() error
}

type MergeOptions struct {
	StagingMonitor         StagingEntryMonitor
	CpMonitor              CpMonitor
	CommitMonitor          CommitMonitor
	Author                 string
	Message                string
	RestorableMetadataFlag lib.RestorableMetadataFlag
	UseStagingCache        bool
	// todo: add a `MergeMonitor` that is called after each merge step.
}

type MergeConflict struct {
	WorkspaceEntry  *lib.RevisionEntry
	RepositoryEntry *lib.RevisionEntry
}

type MergeConflictsError []MergeConflict

func (mc MergeConflictsError) Error() string {
	var s strings.Builder
	s.WriteString("MergeConflictsError(")
	for i, conflict := range mc {
		if i > 0 {
			s.WriteString(", ")
		}
		fmt.Fprintf(&s, "%q", conflict.WorkspaceEntry.Path)
	}
	return s.String() + ")"
}

type Merger struct {
	ws               *Workspace
	wsHead           lib.RevisionId
	remoteRevisionId lib.RevisionId
	tempFS           lib.FS
	repository       *lib.Repository
	directories      map[string]fs.FileInfo
	opts             *MergeOptions
	blockBuf         lib.BlockBuf
}

// Merge the changes from the repository into the workspace and vice versa.
// Return a `MergeConflictsError` error if there are conflicts.
// todo: return new revision id and the local changes.
func Merge(ctx context.Context, ws *Workspace, repository *lib.Repository, opts *MergeOptions) (lib.RevisionId, error) {
	tempFS, err := ws.TempFS.MkSub("merge")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create merge tmp dir")
	}
	defer tempFS.RemoveAll(".") //nolint:errcheck
	head, err := repository.Head(ctx)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get repository head")
	}
	wsHead, staging, localChanges, wsRevision, err := buildLocalChanges(ctx, ws, tempFS, repository, opts)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build local changes")
	}
	if head == wsHead && localChanges.Source.Chunks() == 0 {
		return lib.RevisionId{}, ErrUpToDate
	}
	if !wsHead.IsRoot() {
		chain, err := lib.ReadRevisionChain(ctx, repository)
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to read repository revision chain")
		}
		if !slices.Contains(chain, wsHead) {
			return lib.RevisionId{}, lib.Errorf("workspace head %s is not in the repository's revision chain", wsHead)
		}
	}
	remoteRevision, err := buildRemoteChanges(ctx, tempFS, repository, head)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build remote changes")
	}
	merger := Merger{ws, wsHead, head, tempFS, repository, make(map[string]fs.FileInfo), opts, lib.NewBlockBuf()}
	conflicts, err := merger.findConflicts(localChanges.Source, remoteRevision, wsRevision)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to find conflicts")
	}
	if len(conflicts) > 0 {
		return lib.RevisionId{}, conflicts
	}
	if err := merger.applyRemoteChanges(ctx, head, remoteRevision, staging, localChanges); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to apply remote changes")
	}
	if localChanges.Source.Chunks() > 0 {
		err := opts.CommitMonitor.OnBeforeCommit()
		if err != nil {
			return lib.RevisionId{}, err //nolint:wrapcheck
		}
		newHead, err := merger.commitLocalChanges(
			ctx,
			localChanges.Source,
			remoteRevision,
			opts.CommitMonitor,
			opts.Author,
			opts.Message,
		)
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to commit local changes")
		}
		head = newHead
	}
	if err := lib.WriteRef(ctx, ws.Storage, "head", head); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to write workspace head reference - please re-run merge")
	}
	return head, nil
}

type ForceCommitOptions struct {
	MergeOptions
}

// Commit all local changes ignoring possible conflicts.
// Afterwards, merge the repository into the workspace.
// Return a `lib.EmptyCommit` error if there are no local changes.
func ForceCommit( //nolint:funlen
	ctx context.Context,
	ws *Workspace,
	repository *lib.Repository,
	opts *ForceCommitOptions,
) (lib.RevisionId, error) {
	tempFS, err := ws.TempFS.MkSub("merge")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create merge tmp dir")
	}
	defer tempFS.RemoveAll(".") //nolint:errcheck
	wsHead, staging, localChanges, _, err := buildLocalChanges(ctx, ws, tempFS, repository, &opts.MergeOptions)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build local changes")
	}
	if localChanges.Source.Chunks() == 0 {
		return lib.RevisionId{}, lib.ErrEmptyCommit
	}
	if !wsHead.IsRoot() {
		chain, err := lib.ReadRevisionChain(ctx, repository)
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to read repository revision chain")
		}
		if !slices.Contains(chain, wsHead) {
			return lib.RevisionId{}, lib.Errorf("workspace head %s is not in the repository's revision chain", wsHead)
		}
	}
	head, err := repository.Head(ctx)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get repository head")
	}
	remoteRevision, err := buildRemoteChanges(ctx, tempFS, repository, head)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build remote changes")
	}
	merger := &Merger{
		ws,
		wsHead,
		head,
		tempFS,
		repository,
		make(map[string]fs.FileInfo),
		&opts.MergeOptions,
		lib.NewBlockBuf(),
	}
	newHead, err := merger.commitLocalChanges(
		ctx,
		localChanges.Source,
		remoteRevision,
		opts.CommitMonitor,
		opts.Author,
		opts.Message,
	)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to commit local changes")
	}
	remoteRevision, err = buildRemoteChanges(ctx, tempFS, repository, newHead)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build remote changes")
	}
	if err := merger.applyRemoteChanges(ctx, newHead, remoteRevision, staging, localChanges); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to apply remote changes")
	}
	if err := lib.WriteRef(ctx, ws.Storage, "head", newHead); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to write workspace head reference - please re-run merge")
	}
	return newHead, nil
}

func hasRemoteChanged(ctx context.Context, repository *lib.Repository, revisionId lib.RevisionId) error {
	head, err := repository.Head(ctx)
	if err != nil {
		return lib.WrapErrorf(err, "failed to get repository head")
	}
	if head == revisionId {
		return nil
	}
	return ErrRemoteChanged
}

func (m *Merger) commitLocalChanges( //nolint:funlen
	ctx context.Context,
	localChanges *lib.Temp[*lib.RevisionEntry],
	remoteRevision *lib.TempCache[*lib.RevisionEntry],
	mon CommitMonitor,
	author string,
	message string,
) (lib.RevisionId, error) {
	tmpFS, err := m.tempFS.MkSub("commit")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create commit tmp dir")
	}
	commit, err := lib.NewCommit(ctx, m.repository, tmpFS)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create commit")
	}
	r := localChanges.Reader(nil)
	for {
		entry, err := r.Read(m.blockBuf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		localPath, _ := entry.Path.TrimBase(m.ws.PathPrefix)
		if err := mon.OnStart(entry); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "commit monitor start failed for %s", entry.Path)
		}
		if entry.Kind == lib.RevisionEntryKindDelete {
			if err := commit.Add(entry); err != nil {
				return lib.RevisionId{}, lib.WrapErrorf(err, "failed to add revision entry to commit")
			}
			if err := mon.OnEnd(entry); err != nil {
				return lib.RevisionId{}, lib.WrapErrorf(err, "commit monitor end failed for %s", entry.Path)
			}
			continue
		}
		stat, err := m.ws.FS.Stat(localPath.String())
		if errors.Is(err, fs.ErrNotExist) {
			// todo: make special errors out of these so we can distinguish them later.
			return lib.RevisionId{}, lib.Errorf("file %s was deleted during merge - aborting merge", localPath)
		}
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to stat %s", localPath)
		}
		remoteEntry, existsInRemote, err := remoteRevision.Get(lib.RevisionEntryPathCompareString(entry))
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(
				err,
				"failed to get entry from repository snapshot cache for %s",
				entry.Path,
			)
		}
		var md lib.PathMetadata
		if existsInRemote && entry.Metadata.FileHash == remoteEntry.Metadata.FileHash {
			if entry.Metadata.IsEqualRestorableAttributes(remoteEntry.Metadata, m.opts.RestorableMetadataFlag) {
				// The file did not change at all, we can skip it completely.
				if err := mon.OnEnd(entry); err != nil {
					return lib.RevisionId{}, lib.WrapErrorf(err, "commit monitor end failed for %s", entry.Path)
				}
				continue
			}
			// Only metadata changed.
			md = entry.Metadata
			md.BlockIds = remoteEntry.Metadata.BlockIds
		} else {
			uploadedMD, err := AddFileToRepository(ctx, m.ws.FS, localPath, stat, m.repository, entry, mon)
			if err != nil {
				return lib.RevisionId{}, lib.WrapErrorf(err, "failed to add blocks and get metadata for %s", localPath)
			}
			md = uploadedMD
		}
		if md.FileHash != entry.Metadata.FileHash {
			return lib.RevisionId{}, lib.Errorf(
				"file %s was modified during merge - aborting merge (hash: %s vs %s)",
				localPath,
				md.FileHash,
				entry.Metadata.FileHash,
			)
		}
		entry.Metadata = md
		if err := commit.Add(entry); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to add revision entry to commit")
		}
		if err := mon.OnEnd(entry); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "commit monitor end failed for %s", entry.Path)
		}
	}
	// Make sure the path prefix exists in the repository after the commit.
	if err := commit.EnsureDirExists(m.ws.PathPrefix, remoteRevision, m.remoteRevisionId); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(
			err,
			"failed to ensure path %s exists in the repository",
			m.ws.PathPrefix,
		)
	}
	info := &lib.CommitInfo{Author: author, Message: message}
	revisionId, err := commit.Commit(ctx, info)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to commit")
	}
	return revisionId, nil
}

func (m *Merger) findConflicts(
	localChanges *lib.Temp[*lib.RevisionEntry],
	remoteRevisionCache *lib.TempCache[*lib.RevisionEntry],
	wsRevisionCache *lib.TempCache[*lib.RevisionEntry],
) (MergeConflictsError, error) {
	r := localChanges.Reader(nil)
	conflicts := MergeConflictsError{}
	for {
		localChange, err := r.Read(m.blockBuf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		path := localChange.Path
		remoteChange, remoteChangeExists, err := remoteRevisionCache.Get(
			lib.RevisionEntryPathCompareString(localChange),
		)
		if err != nil {
			return nil, lib.WrapErrorf(
				err,
				"failed to get entry from repository snapshot cache for %s",
				path,
			)
		}
		if remoteChangeExists {
			if localChange.Metadata.FileMode.IsDir() && remoteChange.Metadata.FileMode.IsDir() {
				// Directories cannot conflict, we always overwrite the attributes of the directory.
				// todo: document that changes to local directories are ignored if they are also present in the repository.
				//       We overwrite the attributes of the directory. Contained files are not affected.
				continue
			}
			wsChange, wsChangeExists, err := wsRevisionCache.Get(lib.RevisionEntryPathCompareString(localChange))
			if err != nil {
				return nil, lib.WrapErrorf(
					err,
					"failed to get entry from workspace snapshot cache for %s",
					path,
				)
			}
			if wsChangeExists &&
				wsChange.Metadata.IsEqualRestorableAttributes(remoteChange.Metadata, m.opts.RestorableMetadataFlag) {
				// The file did not change between the workspace revision and the repository revision.
				continue
			}
			if localChange.Metadata.IsEqualRestorableAttributes(remoteChange.Metadata, m.opts.RestorableMetadataFlag) {
				continue
			}
			localChange.Path, _ = localChange.Path.TrimBase(m.ws.PathPrefix)
			conflicts = append(conflicts, MergeConflict{localChange, remoteChange})
		}
	}
	return conflicts, nil
}

func (m *Merger) makeDirsWritable(relPath string) error {
	parent := filepath.Dir(relPath)
	for parent != "." {
		if _, ok := m.directories[parent]; ok {
			break
		}
		stat, err := m.ws.FS.Stat(parent)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return lib.WrapErrorf(err, "failed to stat directory %s", parent)
		}
		m.directories[parent] = stat
		if stat.Mode()&0o700 != 0o700 {
			if err := m.ws.FS.Chmod(parent, stat.Mode()|0o700); err != nil {
				return lib.WrapErrorf(err, "failed to make directory %s writable", parent)
			}
		}
		parent = filepath.Dir(parent)
	}
	return nil
}

func (m *Merger) restoreDirFileModes() error {
	paths := make([]string, 0, len(m.directories))
	for path := range m.directories {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	slices.Reverse(paths)
	for _, path := range paths {
		fileInfo := m.directories[path]
		mode := fileInfo.Mode()
		if mode&0o700 != 0o700 {
			if err := m.ws.FS.Chmod(path, mode); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", mode, path)
			}
		}
		// Restore mtime on a best-effort basis.
		_ = m.ws.FS.Chmtime(path, fileInfo.ModTime())
	}
	m.directories = nil // Make sure the deferred function does not restore the file modes twice.
	return nil
}

// Make the workspace look like the remote repository by applying all remote changes (add, update, remove).
func (m *Merger) applyRemoteChanges(
	ctx context.Context,
	head lib.RevisionId,
	remoteRevision *lib.TempCache[*lib.RevisionEntry],
	staging *lib.TempCache[*StagingEntry],
	localChanges *lib.TempCache[*lib.RevisionEntry],
) error {
	defer m.restoreDirFileModes() //nolint:errcheck
	if err := hasRemoteChanged(ctx, m.repository, head); err != nil {
		return err
	}
	if err := m.copyRepositoryFiles(ctx, remoteRevision.Source, staging, localChanges); err != nil {
		return lib.WrapErrorf(err, "failed to copy remote files")
	}
	if err := m.deleteObsoleteWorkspaceFiles(remoteRevision, staging, localChanges); err != nil {
		return lib.WrapErrorf(err, "failed to delete obsolete workspace files")
	}
	if err := m.restoreDirFileModes(); err != nil {
		return lib.WrapErrorf(err, "failed to restore file mode for directories")
	}
	return nil
}

// Copy all remote files that are not part of the local changes.
func (m *Merger) copyRepositoryFiles( //nolint:funlen
	ctx context.Context,
	remoteRevision *lib.Temp[*lib.RevisionEntry],
	staging *lib.TempCache[*StagingEntry],
	localChanges *lib.TempCache[*lib.RevisionEntry],
) error {
	r := remoteRevision.Reader(lib.RevisionEntryPathFilter(m.ws.PathPrefix.AsFilter()))
	for {
		remoteEntry, err := r.Read(m.blockBuf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		if remoteEntry.Path == m.ws.PathPrefix {
			continue
		}
		if remoteEntry.Kind != lib.RevisionEntryKindAdd && remoteEntry.Kind != lib.RevisionEntryKindUpdate {
			return lib.Errorf("unexpected revision entry type %s for %s", remoteEntry.Kind, remoteEntry.Path)
		}
		localPath, _ := remoteEntry.Path.TrimBase(m.ws.PathPrefix)
		targetPath := localPath.String()
		if err := m.makeDirsWritable(targetPath); err != nil {
			return lib.WrapErrorf(err, "failed to make directories writable for %s", remoteEntry.Path)
		}
		stagingEntry, existsInStaging, err := staging.Get(lib.RevisionEntryPathCompareString(remoteEntry))
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from cache for %s", localPath)
		}
		_, isLocalChange, err := localChanges.Get(lib.RevisionEntryPathCompareString(remoteEntry))
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from cache for %s", localPath)
		}
		if isLocalChange {
			continue
		}
		md := remoteEntry.Metadata
		removed, err := removeLocalIfTypeMismatch(m.ws.FS, targetPath, md.FileMode)
		if err != nil {
			return lib.WrapErrorf(err, "failed to clear type-mismatched %s", targetPath)
		}
		if removed {
			existsInStaging = false
			stagingEntry = nil
		}
		switch {
		case md.FileMode.IsSymlink():
			if md.SymLinkTarget == nil {
				return lib.Errorf("symlink %s has no target", remoteEntry.Path)
			}
			localTargetPath, inside := md.SymLinkTarget.TrimBase(m.ws.PathPrefix)
			if !inside {
				// Target falls outside `PathPrefix` - silently skip (see README).
				continue
			}
			sameAsStaging := existsInStaging &&
				stagingEntry.Metadata.FileMode.IsSymlink() &&
				stagingEntry.Metadata.SymLinkTarget != nil &&
				*stagingEntry.Metadata.SymLinkTarget == *md.SymLinkTarget
			if !sameAsStaging {
				if err := m.ws.FS.MkdirAll(filepath.Dir(targetPath)); err != nil {
					return lib.WrapErrorf(err, "failed to create parent directory for %s", targetPath)
				}
				if existsInStaging {
					if err := m.ws.FS.Remove(targetPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
						return lib.WrapErrorf(err, "failed to remove existing %s", targetPath)
					}
				}
				linkStr, err := filepath.Rel(filepath.Dir(localPath.String()), localTargetPath.String())
				if err != nil {
					return lib.WrapErrorf(err, "failed to compute symlink string for %s", targetPath)
				}
				if err := m.ws.FS.Symlink(filepath.ToSlash(linkStr), targetPath); err != nil {
					return lib.WrapErrorf(err, "failed to create symlink %s", targetPath)
				}
			}
		case md.FileMode.IsDir():
			if !existsInStaging {
				if err := m.ws.FS.Mkdir(targetPath); err != nil {
					return lib.WrapErrorf(err, "failed to create directory %s", targetPath)
				}
			}
		default:
			// todo: check whether the file was modified since merging started.
			if !existsInStaging || md.FileHash != stagingEntry.Metadata.FileHash ||
				md.Size != stagingEntry.Metadata.Size {
				if err := m.restoreFromRepository(ctx, remoteEntry, m.opts.CpMonitor, targetPath); err != nil {
					return lib.WrapErrorf(err, "failed to restore %s", targetPath)
				}
			}
		}
		restoreMode := m.opts.RestorableMetadataFlag
		if !existsInStaging {
			// Newly created entries also need mode and mtime restored.
			restoreMode |= lib.RestorableMetadataMode | lib.RestorableMetadataMTime
		}
		if err := restoreFileMode(m.ws.FS, targetPath, &md, restoreMode); err != nil {
			return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.FileMode, targetPath)
		}
	}
	return nil
}

// If the entry at `target` is a different kind (file vs dir vs symlink)
// than `remoteMode`, remove it so the caller can recreate it cleanly.
// `removed` reports whether the removal happened.
func removeLocalIfTypeMismatch(targetFS lib.FS, target string, remoteMode lib.FileMode) (removed bool, err error) {
	info, statErr := targetFS.Stat(target)
	if errors.Is(statErr, fs.ErrNotExist) {
		return false, nil
	}
	if statErr != nil {
		return false, lib.WrapErrorf(statErr, "failed to stat %s", target)
	}
	localMode := lib.NewFileMode(info.Mode())
	if localMode.IsDir() == remoteMode.IsDir() && localMode.IsSymlink() == remoteMode.IsSymlink() {
		return false, nil
	}
	if localMode.IsDir() {
		if err := targetFS.RemoveAll(target); err != nil {
			return false, lib.WrapErrorf(err, "failed to remove %s", target)
		}
	} else {
		if err := targetFS.Remove(target); err != nil {
			return false, lib.WrapErrorf(err, "failed to remove %s", target)
		}
	}
	return true, nil
}

// Delete all files in the workspace that are not in the repository and are not local changes.
// Return an error if the workspace changed during the merge.
func (m *Merger) deleteObsoleteWorkspaceFiles( //nolint:funlen
	remoteRevision *lib.TempCache[*lib.RevisionEntry],
	staging *lib.TempCache[*StagingEntry],
	localChanges *lib.TempCache[*lib.RevisionEntry],
) error {
	deleteDirs := make(map[string]bool)
	err := lib.WalkDirIgnore(m.ws.FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return lib.WrapErrorf(err, "failed to walk directory %s", path)
		}
		if path == "." {
			return nil
		}
		if filepath.Base(path) == ".cling" {
			return filepath.SkipDir
		}
		fileInfo, err := d.Info()
		if err != nil {
			return lib.WrapErrorf(err, "failed to get file info for %s", path)
		}
		if !d.Type().IsRegular() && !d.Type().IsDir() && d.Type()&fs.ModeSymlink == 0 {
			return nil
		}
		repositoryPath_, err := lib.NewPath(path)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create path from %s", path)
		}
		repositoryPath := m.ws.PathPrefix.Join(repositoryPath_)
		stagingEntry, existsInStaging, err := staging.Get(lib.PathCompareString(repositoryPath, d.IsDir()))
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from staging cache for %s", path)
		}
		_, existsInLocalChanges, err := localChanges.Get(lib.PathCompareString(repositoryPath, d.IsDir()))
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from local changes cache for %s", path)
		}
		isSymlink := d.Type()&fs.ModeSymlink != 0
		if existsInStaging && existsInLocalChanges {
			if !d.IsDir() && !isSymlink &&
				(stagingEntry.Metadata.MTime() != fileInfo.ModTime() || stagingEntry.Metadata.Size != fileInfo.Size()) {
				return lib.Errorf(
					"metadata of file %s was modified during merge (before: mtime=%s size=%d after: mtime=%s size=%d)- aborting merge",
					path,
					stagingEntry.Metadata.MTime(),
					stagingEntry.Metadata.Size,
					fileInfo.ModTime(),
					fileInfo.Size(),
				)
			}
			return nil
		}
		remoteEntry, existsInRemote, err := remoteRevision.Get(lib.PathCompareString(repositoryPath, d.IsDir()))
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from repository snapshot cache for %s", path)
		}
		if existsInRemote {
			if d.IsDir() || isSymlink {
				return nil
			}
			if remoteEntry.Metadata.Size != fileInfo.Size() {
				// todo: We should record inode and ctime during staging and compare them here.
				return lib.Errorf(
					"file %s was modified during merge - aborting merge (size: %d vs %d)",
					path,
					remoteEntry.Metadata.Size,
					fileInfo.Size(),
				)
			}
			return nil
		}
		if !existsInStaging {
			return lib.Errorf("file %s was created during merge - aborting merge", path)
		}
		if d.IsDir() {
			deleteDirs[path] = true
		} else {
			if fileInfo.ModTime() != stagingEntry.Metadata.MTime() || fileInfo.Size() != stagingEntry.Metadata.Size {
				return lib.Errorf("file %s was modified during merge - aborting merge", path)
			}
			if err := m.makeDirsWritable(path); err != nil {
				return lib.WrapErrorf(err, "failed to make directories writable for %s", path)
			}
			if err := m.ws.FS.Remove(path); err != nil {
				return lib.WrapErrorf(err, "failed to delete %s", path)
			}
		}
		return nil
	})
	if err != nil {
		return lib.WrapErrorf(err, "failed to walk directory %s", m.ws.FS)
	}
	// Delete directories depth-first.
	dirs := make([]string, 0, len(deleteDirs))
	for path := range deleteDirs {
		dirs = append(dirs, path)
	}
	slices.Sort(dirs)
	slices.Reverse(dirs)
	for _, path := range dirs {
		if err := m.makeDirsWritable(path); err != nil {
			return lib.WrapErrorf(err, "failed to make directories writable for %s", path)
		}
		if err := m.ws.FS.Remove(path); err != nil {
			return lib.WrapErrorf(err, "failed to delete %s", path)
		}
	}
	return nil
}

func (m *Merger) restoreFromRepository( //nolint:funlen
	ctx context.Context,
	entry *lib.RevisionEntry,
	mon CpMonitor,
	target string,
) error {
	if err := mon.OnStart(entry, target); err != nil {
		return lib.WrapErrorf(err, "cp monitor start failed for %s", target)
	}
	md := entry.Metadata
	if md.FileMode.IsDir() {
		if err := m.ws.FS.MkdirAll(target); err != nil {
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
	if err := m.ws.FS.MkdirAll(filepath.Dir(target)); err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		}
		return lib.WrapErrorf(err, "failed to create parent directory %s", target)
	}
	tmpPath := lib.AtomicWriteTempFilename(target)
	f, err := m.ws.FS.OpenWrite(tmpPath)
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
		data, err := m.repository.ReadBlock(ctx, blockId, m.blockBuf)
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
	if err := m.ws.FS.Rename(tmpPath, target); err != nil {
		_ = m.ws.FS.Remove(tmpPath)
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		}
		return lib.WrapErrorf(err, "failed to rename %s to %s", tmpPath, target)
	}
	if err := m.ws.FS.Chmod(target, md.FileMode.AsFsFileMode()); err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			if endErr := mon.OnEnd(entry, target); endErr != nil {
				return lib.WrapErrorf(endErr, "cp monitor end failed for %s", target)
			}
			return nil
		}
		return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.FileMode, target)
	}
	if err := mon.OnEnd(entry, target); err != nil {
		return lib.WrapErrorf(err, "cp monitor end failed for %s", target)
	}
	return nil
}

// Add the file contents to the repository and return the file metadata.
func AddFileToRepository(
	ctx context.Context,
	srcFS lib.FS,
	path lib.Path,
	fileInfo fs.FileInfo,
	repository *lib.Repository,
	entry *lib.RevisionEntry,
	mon CommitMonitor,
) (lib.PathMetadata, error) {
	if fileInfo.IsDir() {
		return lib.NewPathMetadataFromFileInfo(fileInfo, lib.Sha256{}, nil), nil
	}
	if fileInfo.Mode()&fs.ModeSymlink != 0 {
		md := lib.NewPathMetadataFromFileInfo(fileInfo, lib.Sha256{}, nil)
		md.SymLinkTarget = entry.Metadata.SymLinkTarget
		return md, nil
	}
	// Fast path: If the entry already has BlockIds and the size of the file did
	// not change, only calculate the hash.
	// If the hash is the same, we can skip the whole block calculation.
	if entry != nil && len(entry.Metadata.BlockIds) > 0 &&
		entry.Metadata.Size == fileInfo.Size() {
		md, err := computeFileHash(srcFS, path, fileInfo)
		if err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to create file metadata")
		}
		if bytes.Equal(md.FileHash[:], entry.Metadata.FileHash[:]) {
			md.BlockIds = entry.Metadata.BlockIds
			return md, nil
		}
	}
	blockIds := []lib.BlockId{}
	fileHash := sha256.New()
	f, err := srcFS.OpenRead(path.String())
	if err != nil {
		return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
	}
	defer f.Close() //nolint:errcheck
	// Read blocks and add them to the repository.
	cdc := lib.NewGearCDCWithDefaults(f, repository.GearCDCTable())
	writeBuf := lib.NewBlockBuf()
	for {
		data, err := cdc.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
		}
		if _, err := fileHash.Write(data); err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to update file hash")
		}
		blockId, bytesWritten, err := repository.WriteBlock(ctx, data, writeBuf)
		if err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to write block")
		}
		if err := mon.OnAddBlock(entry, blockId, len(data), bytesWritten); err != nil {
			return lib.PathMetadata{}, lib.WrapErrorf(err, "commit monitor add block failed for %s", path)
		}
		blockIds = append(blockIds, blockId)
	}
	return lib.NewPathMetadataFromFileInfo(fileInfo, lib.Sha256(fileHash.Sum(nil)), blockIds), nil
}

// Create a `Staging` from `ws.WorkspacePath` and a `lib.RevisionSnapshot` based on the
// workspace `head` revision.
// Then compute the local changes between the `Staging` and the `head` revision.
// Return all three (staging, local changes, workspace revision) as a `lib.RevisionTempCache`.
// When the workspace head is the root revision (the workspace was attached
// but never merged), the repository head is used as the diff baseline and
// `Delete` entries are filtered out of `localChanges`.
func buildLocalChanges(
	ctx context.Context,
	ws *Workspace,
	tempFS lib.FS,
	repository *lib.Repository,
	opts *MergeOptions,
) (wsHead lib.RevisionId, stagingCache *lib.TempCache[*StagingEntry], localChangesCache *lib.TempCache[*lib.RevisionEntry], wsRevisionCache *lib.TempCache[*lib.RevisionEntry], err error) {
	wsHead, err = ws.Head(ctx)
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to get workspace head")
	}
	baselineHead := wsHead
	suppressDeletes := false
	if wsHead.IsRoot() {
		baselineHead, err = repository.Head(ctx)
		if err != nil {
			return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to get repository head")
		}
		suppressDeletes = true
	}
	stagingTmpDir, err := tempFS.MkSub("staging")
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to create staging tmp dir")
	}
	wsSnapshotTmpDir, err := tempFS.MkSub("snapshot")
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to create snapshot tmp dir")
	}
	wsRevisionSnapshot, err := lib.NewRevisionSnapshot(ctx, repository, baselineHead, wsSnapshotTmpDir)
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	wsRevisionCache, err = lib.NewRevisionEntryTempCache(wsRevisionSnapshot, 10)
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to create revision temp cache")
	}
	staging, err := NewStaging(ws.FS, ws.PathPrefix, nil, opts.UseStagingCache, stagingTmpDir, opts.StagingMonitor)
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to detect local changes")
	}
	finalStaging, err := staging.Finalize()
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to finalize staging temp writer")
	}
	stagingCache, err = lib.NewTempCache(finalStaging, StagingCacheKey, 10)
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to create staging cache")
	}
	localChanges, err := staging.MergeWithSnapshot(wsRevisionSnapshot, opts.RestorableMetadataFlag, suppressDeletes)
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to merge staging and workspace snapshot")
	}
	localChangesCache, err = lib.NewRevisionEntryTempCache(localChanges, 10)
	if err != nil {
		return wsHead, nil, nil, nil, lib.WrapErrorf(err, "failed to create local changes cache")
	}
	return wsHead, stagingCache, localChangesCache, wsRevisionCache, nil
}

// Build a `lib.RevisionTempCache` based on the `lib.RevisionSnapshot` of the remote `head` revision.
func buildRemoteChanges(
	ctx context.Context,
	tempFS lib.FS,
	repository *lib.Repository,
	head lib.RevisionId,
) (remoteRevisionCache *lib.TempCache[*lib.RevisionEntry], err error) {
	tmp, err := tempFS.MkSub("repository-snapshot")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create repository snapshot tmp dir")
	}
	remoteRevisionSnapshot, err := lib.NewRevisionSnapshot(ctx, repository, head, tmp)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create remote revision snapshot")
	}
	remoteRevisionCache, err = lib.NewRevisionEntryTempCache(remoteRevisionSnapshot, 10)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create remote revision cache")
	}
	return remoteRevisionCache, nil
}
