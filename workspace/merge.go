// Merge changes from the repository into the workspace and vice versa.
package workspace

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"slices"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	ErrUpToDate      = lib.Errorf("workspace is up to date")
	ErrRemoteChanged = lib.Errorf("remote repository has changed during merge")
)

type CommitMonitor interface {
	OnStart(entry *lib.RevisionEntry)
	OnAddBlock(entry *lib.RevisionEntry, header *lib.BlockHeader, existed bool, dataSize int64)
	OnEnd(entry *lib.RevisionEntry)
	OnBeforeCommit() error
}

type MergeOptions struct {
	StagingMonitor StagingEntryMonitor
	CpMonitor      CpMonitor
	CommitMonitor  CommitMonitor
	Author         string
	Message        string
	Chown          bool
	// todo: add a `MergeMonitor` that is called after each merge step.
}

type MergeConflict struct {
	WorkspaceEntry  *lib.RevisionEntry
	RepositoryEntry *lib.RevisionEntry
}

type MergeConflictsError []MergeConflict

func (mc MergeConflictsError) Error() string {
	s := "MergeConflictsError("
	for i, conflict := range mc {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%q", conflict.WorkspaceEntry.Path)
	}
	return s + ")"
}

type Merger struct {
	ws          *Workspace
	tempFS      lib.FS
	repository  *lib.Repository
	directories map[string]fs.FileInfo
	opts        *MergeOptions
}

// Merge the changes from the repository into the workspace and vice versa.
// Return a `MergeConflictsError` error if there are conflicts.
// todo: return new revision id and the local changes.
func Merge(ws *Workspace, repository *lib.Repository, opts *MergeOptions) (lib.RevisionId, error) {
	tempFS, err := ws.TempFS.MkSub("merge")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create merge tmp dir")
	}
	defer tempFS.RemoveAll(".") //nolint:errcheck
	head, err := repository.Head()
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get repository head")
	}
	staging, localChanges, wsRevision, err := buildLocalChanges(ws, tempFS, repository, opts)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build local changes")
	}
	wsHead, err := ws.Head()
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get workspace head")
	}
	if head == wsHead && localChanges.Source.Chunks() == 0 {
		return lib.RevisionId{}, ErrUpToDate
	}
	remoteRevision, err := buildRemoteChanges(tempFS, repository, head)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build remote changes")
	}
	merger := Merger{ws, tempFS, repository, make(map[string]fs.FileInfo), opts}
	conflicts, err := merger.findConflicts(localChanges.Source, remoteRevision, wsRevision)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to find conflicts")
	}
	if len(conflicts) > 0 {
		return lib.RevisionId{}, conflicts
	}
	if err := merger.applyRemoteChanges(head, remoteRevision, staging, localChanges); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to apply remote changes")
	}
	if localChanges.Source.Chunks() > 0 {
		err := opts.CommitMonitor.OnBeforeCommit()
		if err != nil {
			return lib.RevisionId{}, err //nolint:wrapcheck
		}
		newHead, err := merger.commitLocalChanges(
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
	if err := lib.WriteRef(ws.Storage, "head", head); err != nil {
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
func ForceCommit(ws *Workspace, repository *lib.Repository, opts *ForceCommitOptions) (lib.RevisionId, error) {
	tempFS, err := ws.TempFS.MkSub("merge")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create merge tmp dir")
	}
	defer tempFS.RemoveAll(".") //nolint:errcheck
	staging, localChanges, _, err := buildLocalChanges(ws, tempFS, repository, &opts.MergeOptions)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build local changes")
	}
	if localChanges.Source.Chunks() == 0 {
		return lib.RevisionId{}, lib.ErrEmptyCommit
	}
	merger := &Merger{ws, tempFS, repository, make(map[string]fs.FileInfo), &opts.MergeOptions}
	var remoteRevision *lib.RevisionTempCache
	if !ws.PathPrefix.IsEmpty() {
		// If the workspace has a path prefix, we have to build the remote changes
		// to make sure that the path prefix exists in the repository.
		curHead, err := repository.Head()
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get repository head")
		}
		remoteRevision, err = buildRemoteChanges(tempFS, repository, curHead)
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build remote changes")
		}
	}
	newHead, err := merger.commitLocalChanges(
		localChanges.Source,
		remoteRevision,
		opts.CommitMonitor,
		opts.Author,
		opts.Message,
	)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to commit local changes")
	}
	remoteRevision, err = buildRemoteChanges(tempFS, repository, newHead)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to build remote changes")
	}
	if err := merger.applyRemoteChanges(newHead, remoteRevision, staging, localChanges); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to apply remote changes")
	}
	if err := lib.WriteRef(ws.Storage, "head", newHead); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to write workspace head reference - please re-run merge")
	}
	return newHead, nil
}

func hasRemoteChanged(repository *lib.Repository, revisionId lib.RevisionId) error {
	head, err := repository.Head()
	if err != nil {
		return lib.WrapErrorf(err, "failed to get repository head")
	}
	if head == revisionId {
		return nil
	}
	return ErrRemoteChanged
}

func (m *Merger) commitLocalChanges( //nolint:funlen
	localChanges *lib.RevisionTemp,
	remoteRevision *lib.RevisionTempCache,
	mon CommitMonitor,
	author string,
	message string,
) (lib.RevisionId, error) {
	tmpFS, err := m.tempFS.MkSub("commit")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create commit tmp dir")
	}
	commit, err := lib.NewCommit(m.repository, tmpFS)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create commit")
	}
	r := localChanges.Reader(nil)
	for {
		entry, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		localPath, _ := entry.Path.TrimBase(m.ws.PathPrefix)
		mon.OnStart(entry)
		if entry.Type == lib.RevisionEntryDelete {
			if err := commit.Add(entry); err != nil {
				return lib.RevisionId{}, lib.WrapErrorf(err, "failed to add revision entry to commit")
			}
			mon.OnEnd(entry)
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
		md, err := AddFileToRepository(m.ws.FS, localPath, stat, m.repository, entry, mon.OnAddBlock)
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to add blocks and get metadata for %s", localPath)
		}
		if md.FileHash != entry.Metadata.FileHash {
			return lib.RevisionId{}, lib.Errorf("file %s was modified during merge - aborting merge", localPath)
		}
		entry.Metadata = &md
		if err := commit.Add(entry); err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to add revision entry to commit")
		}
		mon.OnEnd(entry)
	}
	// Make sure the path prefix exists in the repository after the commit.
	if err := commit.EnsureDirExists(m.ws.PathPrefix, remoteRevision); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(
			err,
			"failed to ensure path %s exists in the repository",
			m.ws.PathPrefix,
		)
	}
	info := &lib.CommitInfo{Author: author, Message: message}
	revisionId, err := commit.Commit(info)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to commit")
	}
	return revisionId, nil
}

func (m *Merger) findConflicts(
	localChanges *lib.RevisionTemp,
	remoteRevisionCache *lib.RevisionTempCache,
	wsRevisionCache *lib.RevisionTempCache,
) (MergeConflictsError, error) {
	r := localChanges.Reader(nil)
	conflicts := MergeConflictsError{}
	for {
		localChange, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		path := localChange.Path
		remoteChange, remoteChangeExists, err := remoteRevisionCache.Get(
			path,
			localChange.Metadata.ModeAndPerm.IsDir(),
		)
		if err != nil {
			return nil, lib.WrapErrorf(
				err,
				"failed to get entry from repository snapshot cache for %s",
				path,
			)
		}
		if remoteChangeExists {
			if localChange.Metadata.ModeAndPerm.IsDir() && remoteChange.Metadata.ModeAndPerm.IsDir() {
				// Directories cannot conflict, we always overwrite the attributes of the directory.
				// todo: document that changes to local directories are ignored if they are also present in the repository.
				//       We overwrite the attributes of the directory. Contained files are not affected.
				continue
			}
			wsChange, wsChangeExists, err := wsRevisionCache.Get(path, localChange.Metadata.ModeAndPerm.IsDir())
			if err != nil {
				return nil, lib.WrapErrorf(
					err,
					"failed to get entry from workspace snapshot cache for %s",
					path,
				)
			}
			if wsChangeExists && wsChange.Metadata.IsEqualRestorableAttributes(remoteChange.Metadata, m.opts.Chown) {
				// The file did not change between the workspace revision and the repository revision.
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
		parent = filepath.Dir(parent)
		if stat.Mode()&0o700 != 0o700 {
			if err := m.ws.FS.Chmod(parent, stat.Mode()|0o700); err != nil {
				return lib.WrapErrorf(err, "failed to make directory %s writable", parent)
			}
		}
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
			if err := m.ws.FS.Chmod(path, mode|0o700); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", mode, path)
			}
		}
		// Restore mtime.
		if err := m.ws.FS.Chmtime(path, fileInfo.ModTime()); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return lib.WrapErrorf(err, "failed to restore mtime %s for %s", fileInfo.ModTime(), path)
		}
	}
	m.directories = nil // Make sure the deferred function does not restore the file modes twice.
	return nil
}

// Make the workspace look like the remote repository by applying all remote changes (add, update, remove).
func (m *Merger) applyRemoteChanges(
	head lib.RevisionId,
	remoteRevision *lib.RevisionTempCache,
	staging *lib.RevisionTempCache,
	localChanges *lib.RevisionTempCache,
) error {
	defer m.restoreDirFileModes() //nolint:errcheck
	if err := hasRemoteChanged(m.repository, head); err != nil {
		return err
	}
	if err := m.copyRepositoryFiles(remoteRevision.Source, staging, localChanges); err != nil {
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
func (m *Merger) copyRepositoryFiles(
	remoteRevision *lib.RevisionTemp,
	staging *lib.RevisionTempCache,
	localChanges *lib.RevisionTempCache,
) error {
	r := remoteRevision.Reader(m.ws.PathPrefix.AsFilter())
	for {
		entry, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		if entry.Path == m.ws.PathPrefix {
			continue
		}
		localPath, _ := entry.Path.TrimBase(m.ws.PathPrefix)
		if err := m.makeDirsWritable(localPath.String()); err != nil {
			return lib.WrapErrorf(err, "failed to make directories writable for %s", entry.Path)
		}
		md := entry.Metadata
		isDir := md.ModeAndPerm.IsDir()
		stagingEntry, existsInStaging, err := staging.Get(entry.Path, isDir)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from cache for %s", localPath)
		}
		_, isLocalChange, err := localChanges.Get(entry.Path, isDir)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from cache for %s", localPath)
		}
		if isLocalChange {
			continue
		}
		// Make sure parent directories are writable.
		targetPath := localPath.String()
		if entry.Type != lib.RevisionEntryAdd && entry.Type != lib.RevisionEntryUpdate {
			return lib.Errorf("unexpected revision entry type %s for %s", entry.Type, targetPath)
		}
		// Write the file if it is different or does not exist.
		if isDir {
			if !existsInStaging {
				if err := m.ws.FS.Mkdir(targetPath); err != nil {
					return lib.WrapErrorf(err, "failed to create directory %s", targetPath)
				}
			}
			// Only update metadata.
			if err := restoreFileMode(m.ws.FS, targetPath, md, m.opts.Chown); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.ModeAndPerm, targetPath)
			}
			continue
		}
		// todo: we should check whether the file has been modified since merging has been started.
		//       or if it is new
		if !existsInStaging || md.FileHash != stagingEntry.Metadata.FileHash {
			// Write the file.
			if err := m.restoreFromRepository(entry, m.opts.CpMonitor, targetPath); err != nil {
				return lib.WrapErrorf(err, "failed to restore %s", targetPath)
			}
		}
		if !existsInStaging || !md.IsEqualRestorableAttributes(stagingEntry.Metadata, m.opts.Chown) {
			if err := restoreFileMode(m.ws.FS, targetPath, md, m.opts.Chown); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.ModeAndPerm, targetPath)
			}
		}
	}
	return nil
}

// Delete all files in the workspace that are not in the repository and are not local changes.
// Return an error if the workspace changed during the merge.
func (m *Merger) deleteObsoleteWorkspaceFiles( //nolint:funlen
	remoteRevision *lib.RevisionTempCache,
	staging *lib.RevisionTempCache,
	localChanges *lib.RevisionTempCache,
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
		if !d.Type().IsRegular() && !d.Type().IsDir() {
			// todo: handle symlinks
			return nil
		}
		repositoryPath_, err := lib.NewPath(path)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create path from %s", path)
		}
		repositoryPath := m.ws.PathPrefix.Join(repositoryPath_)
		stagingEntry, existsInStaging, err := staging.Get(repositoryPath, d.IsDir())
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from staging cache for %s", path)
		}
		_, existsInLocalChanges, err := localChanges.Get(repositoryPath, d.IsDir())
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from local changes cache for %s", path)
		}
		if existsInStaging && existsInLocalChanges {
			if !d.IsDir() &&
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
		remoteEntry, existsInRemote, err := remoteRevision.Get(repositoryPath, d.IsDir())
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from repository snapshot cache for %s", path)
		}
		if existsInRemote {
			if !d.IsDir() &&
				(remoteEntry.Metadata.MTime() != fileInfo.ModTime() || remoteEntry.Metadata.Size != fileInfo.Size()) {
				return lib.Errorf("file %s was modified during merge - aborting merge", path)
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
	for path := range deleteDirs {
		if err := m.makeDirsWritable(path); err != nil {
			return lib.WrapErrorf(err, "failed to make directories writable for %s", path)
		}
		if err := m.ws.FS.Remove(path); err != nil {
			return lib.WrapErrorf(err, "failed to delete %s", path)
		}
	}
	return nil
}

func (m *Merger) restoreFromRepository(entry *lib.RevisionEntry, mon CpMonitor, target string) error { //nolint:funlen
	mon.OnStart(entry, target)
	md := entry.Metadata
	if md.ModeAndPerm.IsDir() {
		if err := m.ws.FS.MkdirAll(target); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				return nil
			}
			return lib.WrapErrorf(err, "failed to create directory %s", target)
		}
		return nil
	}
	if err := m.ws.FS.MkdirAll(filepath.Dir(target)); err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			mon.OnEnd(entry, target)
			return nil
		}
		return lib.WrapErrorf(err, "failed to create parent directory %s", target)
	}
	f, err := m.ws.FS.OpenWrite(target)
	if err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			mon.OnEnd(entry, target)
			return nil
		}
		return lib.WrapErrorf(err, "failed to open file %s for writing", target)
	}
	defer f.Close() //nolint:errcheck
	for _, blockId := range entry.Metadata.BlockIds {
		data, _, err := m.repository.ReadBlock(blockId)
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
	if err := m.ws.FS.Chmod(target, md.ModeAndPerm.AsFileMode()); err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			mon.OnEnd(entry, target)
			return nil
		}
		return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.ModeAndPerm, target)
	}
	mon.OnEnd(entry, target)
	return nil
}

// Add the file contents to the repository and return the file metadata.
func AddFileToRepository(
	fs lib.FS,
	path lib.Path,
	fileInfo fs.FileInfo,
	repository *lib.Repository,
	entry *lib.RevisionEntry,
	onAddBlock func(entry *lib.RevisionEntry, header *lib.BlockHeader, existed bool, dataSize int64),
) (lib.FileMetadata, error) {
	if fileInfo.IsDir() {
		return lib.NewFileMetadataFromFileInfo(fileInfo, lib.Sha256{}, nil), nil
	}
	// Fast path: If the entry already has BlockIds and the size of the file did
	// not change, only calculate the hash.
	// If the hash is the same, we can skip the whole block calculation.
	if entry != nil && len(entry.Metadata.BlockIds) > 0 && entry.Metadata != nil &&
		entry.Metadata.Size == fileInfo.Size() {
		md, err := computeFileHash(fs, path, fileInfo)
		if err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to create file metadata")
		}
		if bytes.Equal(md.FileHash[:], entry.Metadata.FileHash[:]) {
			md.BlockIds = entry.Metadata.BlockIds
			return md, nil
		}
	}
	// todo: what about symlinks
	blockIds := []lib.BlockId{}
	fileHash := sha256.New()
	f, err := fs.OpenRead(path.String())
	if err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
	}
	defer f.Close() //nolint:errcheck
	// Read blocks and add them to the repository.
	cdc := NewGearCDCWithDefaults(f)
	for {
		data, err := cdc.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
		}
		if _, err := fileHash.Write(data); err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to update file hash")
		}
		existed, blockHeader, err := repository.WriteBlock(data)
		if err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to write block")
		}
		onAddBlock(entry, &blockHeader, existed, int64(len(data)))
		blockIds = append(blockIds, blockHeader.BlockId)
	}
	return lib.NewFileMetadataFromFileInfo(fileInfo, lib.Sha256(fileHash.Sum(nil)), blockIds), nil
}

// Create a `Staging` from `ws.WorkspacePath` and a `lib.RevisionSnapshot` based on the
// workspace `head` revision.
// Then compute the local changes between the `Staging` and the `head` revision.
// Return all three (staging, local changes, workspace revision) as a `lib.RevisionTempCache`.
func buildLocalChanges(
	ws *Workspace,
	tempFS lib.FS,
	repository *lib.Repository,
	opts *MergeOptions,
) (stagingCache *lib.RevisionTempCache, localChangesCache *lib.RevisionTempCache, wsRevisionCache *lib.RevisionTempCache, err error) {
	wsHead, err := ws.Head()
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to get workspace head")
	}
	stagingTmpDir, err := tempFS.MkSub("staging")
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to create staging tmp dir")
	}
	wsSnapshotTmpDir, err := tempFS.MkSub("snapshot")
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to create snapshot tmp dir")
	}
	wsRevisionSnapshot, err := lib.NewRevisionSnapshot(repository, wsHead, wsSnapshotTmpDir)
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	wsRevisionCache, err = lib.NewRevisionTempCache(wsRevisionSnapshot, 10)
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to create revision temp cache")
	}
	staging, err := NewStaging(ws.FS, ws.PathPrefix, nil, stagingTmpDir, opts.StagingMonitor)
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to detect local changes")
	}
	finalStaging, err := staging.Finalize()
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to finalize staging temp writer")
	}
	stagingCache, err = lib.NewRevisionTempCache(finalStaging, 10)
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to create staging cache")
	}
	localChanges, err := staging.MergeWithSnapshot(wsRevisionSnapshot, opts.Chown)
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to merge staging and workspace snapshot")
	}
	localChangesCache, err = lib.NewRevisionTempCache(localChanges, 10)
	if err != nil {
		return nil, nil, nil, lib.WrapErrorf(err, "failed to create local changes cache")
	}
	return stagingCache, localChangesCache, wsRevisionCache, nil
}

// Build a `lib.RevisionTempCache` based on the `lib.RevisionSnapshot` of the remote `head` revision.
func buildRemoteChanges(
	tempFS lib.FS,
	repository *lib.Repository,
	head lib.RevisionId,
) (remoteRevisionCache *lib.RevisionTempCache, err error) {
	tmp, err := tempFS.MkSub("repository-snapshot")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create repository snapshot tmp dir")
	}
	remoteRevisionSnapshot, err := lib.NewRevisionSnapshot(repository, head, tmp)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create remote revision snapshot")
	}
	remoteRevisionCache, err = lib.NewRevisionTempCache(remoteRevisionSnapshot, 10)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create remote revision cache")
	}
	return remoteRevisionCache, nil
}
