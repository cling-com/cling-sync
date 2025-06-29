// Merge changes from the repository into the workspace and vice versa.
package workspace

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

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
		s += fmt.Sprintf("%q", conflict.WorkspaceEntry.Path.FSString())
	}
	return s + ")"
}

type Merger struct {
	ws                  *Workspace
	repository          *lib.Repository
	cpMonitor           CpMonitor
	stagingEntryMonitor StagingEntryMonitor
	directories         map[string]os.FileInfo
	blockBuf            lib.BlockBuf
}

// Merge the changes from the repository into the workspace and vice versa.
// Return a `MergeConflictsError` error if there are conflicts.
// todo: return new revision id and the local changes.
func Merge(ws *Workspace, repository *lib.Repository, opts *MergeOptions) (lib.RevisionId, error) { //nolint:funlen
	head, err := repository.Head()
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get repository head")
	}
	wsHead, err := ws.Head()
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to get workspace head")
	}
	// First, build a staging against the workspace head to see if there are local changes.
	stagingTmpDir, err := ws.NewTmpDir("staging")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create staging tmp dir")
	}
	wsRevisionStateDir, err := ws.NewTmpDir("snapshot")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create snapshot tmp dir")
	}
	wsRevisionState, err := lib.NewRevisionSnapshot(repository, wsHead, wsRevisionStateDir)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	wsRevisionCache, err := lib.NewRevisionTempCache(&wsRevisionState.RevisionTemp, 10)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create revision temp cache")
	}
	staging, err := NewStaging(ws.WorkspacePath, nil, stagingTmpDir, opts.StagingMonitor)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to detect local changes")
	}
	localChanges, err := staging.MergeWithSnapshot(wsRevisionState)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to merge staging and workspace snapshot")
	}
	if head == wsHead && localChanges.Chunks() == 0 {
		return lib.RevisionId{}, ErrUpToDate
	}
	localChangesCache, err := lib.NewRevisionTempCache(localChanges, 10)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create local changes cache")
	}
	remoteRevisionTmp, err := ws.NewTmpDir("repository-snapshot")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create repository snapshot tmp dir")
	}
	remoteRevisionState, err := lib.NewRevisionSnapshot(repository, head, remoteRevisionTmp)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create remote revision snapshot")
	}
	remoteRevisionCache, err := lib.NewRevisionTempCache(&remoteRevisionState.RevisionTemp, 10)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create remote revision cache")
	}
	stagingState, err := staging.Finalize()
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to finalize staging temp writer")
	}
	stagingCache, err := lib.NewRevisionTempCache(stagingState, 10)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create staging cache")
	}
	m := Merger{
		ws,
		repository,
		opts.CpMonitor,
		opts.StagingMonitor,
		make(map[string]os.FileInfo),
		lib.BlockBuf{},
	}
	defer m.restoreDirFileModes() //nolint:errcheck
	conflicts, err := findConflicts(localChanges, remoteRevisionCache, wsRevisionCache)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to find conflicts")
	}
	if len(conflicts) > 0 {
		return lib.RevisionId{}, conflicts
	}
	if err := hasRemoteChanged(repository, remoteRevisionState.RevisionId); err != nil {
		return lib.RevisionId{}, err
	}
	if err := m.copyRepositoryFiles(remoteRevisionState, stagingCache, localChangesCache); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to copy remote files")
	}
	if err := m.deleteObsoleteWorkspaceFiles(remoteRevisionCache, stagingCache, localChangesCache); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to delete obsolete workspace files")
	}
	if err := m.restoreDirFileModes(); err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to restore file mode for directories")
	}
	m.directories = nil // Make sure the deferred function does not restore the file modes twice.
	if localChanges.Chunks() > 0 {
		err := opts.CommitMonitor.OnBeforeCommit()
		if err != nil {
			return lib.RevisionId{}, err //nolint:wrapcheck
		}
		newHead, err := m.commitLocalChanges(localChanges, opts.CommitMonitor, opts.Author, opts.Message)
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

func (m *Merger) commitLocalChanges(
	localChanges *lib.RevisionTemp,
	mon CommitMonitor,
	author string,
	message string,
) (lib.RevisionId, error) {
	tmpDir, err := m.ws.NewTmpDir("commit")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to create commit tmp dir")
	}
	commit, err := lib.NewCommit(m.repository, tmpDir)
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
		mon.OnStart(entry)
		if entry.Type == lib.RevisionEntryDelete {
			if err := commit.Add(entry); err != nil {
				return lib.RevisionId{}, lib.WrapErrorf(err, "failed to add revision entry to commit")
			}
			mon.OnEnd(entry)
			continue
		}
		localPath := filepath.Join(m.ws.WorkspacePath, entry.Path.FSString())
		stat, err := os.Stat(localPath)
		if os.IsNotExist(err) {
			// todo: make special errors out of these so we can distinguish them later.
			return lib.RevisionId{}, lib.Errorf("file %s was deleted during merge - aborting merge", localPath)
		}
		if err != nil {
			return lib.RevisionId{}, lib.WrapErrorf(err, "failed to stat %s", localPath)
		}
		md, err := addToRepository(localPath, stat, m.repository, entry, mon.OnAddBlock, m.blockBuf)
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
	info := &lib.CommitInfo{Author: author, Message: message}
	revisionId, err := commit.Commit(info)
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to commit")
	}
	return revisionId, nil
}

func findConflicts(
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
		remoteChange, remoteChangeExists, err := remoteRevisionCache.Get(
			localChange.Path,
			localChange.Metadata.ModeAndPerm.IsDir(),
		)
		if err != nil {
			return nil, lib.WrapErrorf(
				err,
				"failed to get entry from repository snapshot cache for %s",
				localChange.Path,
			)
		}
		if remoteChangeExists {
			if localChange.Metadata.ModeAndPerm.IsDir() && remoteChange.Metadata.ModeAndPerm.IsDir() {
				// Directories cannot conflict, we always overwrite the attributes of the directory.
				// todo: document that changes to local directories are ignored if they are also present in the repository.
				//       We overwrite the attributes of the directory. Contained files are not affected.
				continue
			}
			wsChange, wsChangeExists, err := wsRevisionCache.Get(
				localChange.Path,
				localChange.Metadata.ModeAndPerm.IsDir(),
			)
			if err != nil {
				return nil, lib.WrapErrorf(
					err,
					"failed to get entry from workspace snapshot cache for %s",
					localChange.Path,
				)
			}
			if wsChangeExists && wsChange.Metadata.IsEqualRestorableAttributes(remoteChange.Metadata) {
				// The file did not change between the workspace revision and the repository revision.
				continue
			}
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
		path := filepath.Join(m.ws.WorkspacePath, parent)
		stat, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return lib.WrapErrorf(err, "failed to stat directory %s", parent)
		}
		m.directories[parent] = stat
		parent = filepath.Dir(parent)
		if stat.Mode()&0o700 != 0o700 {
			if err := os.Chmod(path, stat.Mode()|0o700); err != nil {
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
			if err := os.Chmod(filepath.Join(m.ws.WorkspacePath, path), mode|0o700); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", mode, path)
			}
		}
		// Restore mtime.
		if err := os.Chtimes(filepath.Join(m.ws.WorkspacePath, path), time.Time{}, fileInfo.ModTime()); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return lib.WrapErrorf(err, "failed to restore mtime %s for %s", fileInfo.ModTime(), path)
		}
	}
	return nil
}

// Copy all remote files that are not part of the local changes.
func (m *Merger) copyRepositoryFiles(
	remoteRevisionState *lib.RevisionSnapshot,
	stagingCache *lib.RevisionTempCache,
	localChangesCache *lib.RevisionTempCache,
) error {
	r := remoteRevisionState.Reader(nil)
	for {
		entry, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		if err := m.makeDirsWritable(entry.Path.FSString()); err != nil {
			return lib.WrapErrorf(err, "failed to make directories writable for %s", entry.Path.FSString())
		}
		md := entry.Metadata
		isDir := md.ModeAndPerm.IsDir()
		stagingEntry, existsInStaging, err := stagingCache.Get(entry.Path, isDir)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from cache for %s", entry.Path.FSString())
		}
		_, isLocalChange, err := localChangesCache.Get(entry.Path, isDir)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from cache for %s", entry.Path.FSString())
		}
		if isLocalChange {
			continue
		}
		// Make sure parent directories are writable.
		targetPath := filepath.Join(m.ws.WorkspacePath, entry.Path.FSString())
		if entry.Type != lib.RevisionEntryAdd && entry.Type != lib.RevisionEntryUpdate {
			return lib.Errorf("unexpected revision entry type %s for %s", entry.Type, targetPath)
		}
		// Write the file if it is different or does not exist.
		if isDir {
			if !existsInStaging {
				if err := os.Mkdir(targetPath, 0o700); err != nil {
					return lib.WrapErrorf(err, "failed to create directory %s", targetPath)
				}
			}
			// Only update metadata.
			if err := restoreFileMode(targetPath, md); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.ModeAndPerm, targetPath)
			}
			continue
		}
		// todo: we should check whether the file has been modified since merging has been started.
		//       or if it is new
		if !existsInStaging || md.FileHash != stagingEntry.Metadata.FileHash {
			// Write the file.
			if err := m.restoreFromRepository(entry, targetPath); err != nil {
				return lib.WrapErrorf(err, "failed to restore %s", targetPath)
			}
		}
		if !existsInStaging || !md.IsEqualRestorableAttributes(stagingEntry.Metadata) {
			if err := restoreFileMode(targetPath, md); err != nil {
				return lib.WrapErrorf(err, "failed to restore file mode %s for %s", md.ModeAndPerm, targetPath)
			}
		}
	}
	return nil
}

// Delete all files in the workspace that are not in the repository and are not local changes.
// Return an error if the workspace changed during the merge.
func (m *Merger) deleteObsoleteWorkspaceFiles( //nolint:funlen
	remoteRevisionCache *lib.RevisionTempCache,
	stagingCache *lib.RevisionTempCache,
	localChangesCache *lib.RevisionTempCache,
) error {
	deleteDirs := make(map[string]bool)
	err := filepath.WalkDir(m.ws.WorkspacePath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return lib.WrapErrorf(err, "failed to walk directory %s", path)
		}
		if path == m.ws.WorkspacePath {
			return nil
		}
		if filepath.Base(path) == ".cling" {
			return filepath.SkipDir
		}
		relPath, err := filepath.Rel(m.ws.WorkspacePath, path)
		if err != nil {
			return lib.WrapErrorf(err, "failed to get relative path for %s", path)
		}
		fileInfo, err := d.Info()
		if err != nil {
			return lib.WrapErrorf(err, "failed to get file info for %s", path)
		}
		if !d.Type().IsRegular() && !d.Type().IsDir() {
			// todo: handle symlinks
			return nil
		}
		repositoryPath := lib.NewPath(strings.Split(relPath, string(os.PathSeparator))...)
		stagingEntry, existsInStaging, err := stagingCache.Get(repositoryPath, d.IsDir())
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from staging cache for %s", path)
		}
		_, existsInLocalChanges, err := localChangesCache.Get(repositoryPath, d.IsDir())
		if err != nil {
			return lib.WrapErrorf(err, "failed to get entry from local changes cache for %s", path)
		}
		if existsInStaging && existsInLocalChanges {
			if !d.IsDir() &&
				(stagingEntry.Metadata.MTime() != fileInfo.ModTime() || stagingEntry.Metadata.Size != fileInfo.Size()) {
				return lib.Errorf("file %s was modified during merge - aborting merge", path)
			}
			return nil
		}
		remoteEntry, existsInRemote, err := remoteRevisionCache.Get(repositoryPath, d.IsDir())
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
			if err := m.makeDirsWritable(relPath); err != nil {
				return lib.WrapErrorf(err, "failed to make directories writable for %s", path)
			}
			if err := os.Remove(path); err != nil {
				return lib.WrapErrorf(err, "failed to delete %s", path)
			}
		}
		return nil
	})
	if err != nil {
		return lib.WrapErrorf(err, "failed to walk directory %s", m.ws.WorkspacePath)
	}
	for path := range deleteDirs {
		if err := m.makeDirsWritable(path); err != nil {
			return lib.WrapErrorf(err, "failed to make directories writable for %s", path)
		}
		if err := os.Remove(path); err != nil {
			return lib.WrapErrorf(err, "failed to delete %s", path)
		}
	}
	return nil
}

func (m *Merger) restoreFromRepository(entry *lib.RevisionEntry, target string) error {
	mon := m.cpMonitor
	md := entry.Metadata
	if md.ModeAndPerm.IsDir() {
		if err := os.MkdirAll(target, 0o700); err != nil {
			if mon.OnError(entry, target, err) == CpOnErrorIgnore {
				mon.OnEnd(entry, target)
				return nil
			}
			return lib.WrapErrorf(err, "failed to create directory %s", target)
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			mon.OnEnd(entry, target)
			return nil
		}
		return lib.WrapErrorf(err, "failed to create parent directory %s", target)
	}
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, md.ModeAndPerm.AsFileMode())
	if err != nil {
		if mon.OnError(entry, target, err) == CpOnErrorIgnore {
			mon.OnEnd(entry, target)
			return nil
		}
		return lib.WrapErrorf(err, "failed to open file %s for writing", target)
	}
	defer f.Close() //nolint:errcheck
	for _, blockId := range entry.Metadata.BlockIds {
		data, _, err := m.repository.ReadBlock(blockId, m.blockBuf)
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
	return nil
}

// Add the file contents to the repository and return the file metadata.
func addToRepository(
	path string,
	fileInfo os.FileInfo,
	repository *lib.Repository,
	entry *lib.RevisionEntry,
	onAddBlock func(entry *lib.RevisionEntry, header *lib.BlockHeader, existed bool, dataSize int64),
	blockBuf lib.BlockBuf,
) (lib.FileMetadata, error) {
	if fileInfo.IsDir() {
		return newFileMetadata(fileInfo, lib.Sha256{}, nil), nil
	}
	// Fast path: If the entry already has BlockIds and the size of the file did
	// not change, only calculate the hash.
	// If the hash is the same, we can skip the whole block calculation.
	if entry != nil && len(entry.Metadata.BlockIds) > 0 && entry.Metadata != nil &&
		entry.Metadata.Size == fileInfo.Size() {
		md, err := computeFileHash(path, fileInfo)
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
	f, err := os.Open(path)
	if err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
	}
	defer f.Close() //nolint:errcheck
	// Read blocks and add them to the repository.
	cdc := NewGearCDCWithDefaults(f)
	for {
		data, err := cdc.Read(blockBuf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
		}
		if _, err := fileHash.Write(data); err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to update file hash")
		}
		existed, blockHeader, err := repository.WriteBlock(data, blockBuf)
		if err != nil {
			return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to write block")
		}
		onAddBlock(entry, &blockHeader, existed, int64(len(data)))
		blockIds = append(blockIds, blockHeader.BlockId)
	}
	return newFileMetadata(fileInfo, lib.Sha256(fileHash.Sum(nil)), blockIds), nil
}
