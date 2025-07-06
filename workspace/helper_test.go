package workspace

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

var td = lib.TestData{} //nolint:gochecknoglobals

type WorkspaceTest struct {
	Workspace     *Workspace
	WorkspacePath string
	t             *testing.T
	assert        lib.Assert
}

func NewWorkspaceTest(t *testing.T, repositoryDir string) *WorkspaceTest {
	t.Helper()
	assert := lib.NewAssert(t)
	workspacePath := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.WalkDir(workspacePath, func(path string, d os.DirEntry, err error) error {
			_ = os.Chmod(path, 0o777) //nolint:gosec
			return nil
		})
	})
	workspace, err := NewWorkspace(workspacePath, RemoteRepository(repositoryDir))
	assert.NoError(err)
	return &WorkspaceTest{workspace, workspacePath, t, assert}
}

func (wt *WorkspaceTest) LocalHead() lib.RevisionId {
	wt.t.Helper()
	head, err := wt.Workspace.Head()
	wt.assert.NoError(err)
	return head
}

func (wt *WorkspaceTest) LocalPath(path string) string {
	wt.t.Helper()
	return filepath.Join(wt.Workspace.WorkspacePath, path)
}

func (wt *WorkspaceTest) AddLocal(path string, content string) {
	wt.t.Helper()
	path = wt.LocalPath(path)
	wt.assert.NoError(os.MkdirAll(filepath.Dir(path), 0o700))
	err := os.WriteFile(path, []byte(content), 0o600)
	wt.assert.NoError(err)
}

func (wt *WorkspaceTest) LocalStat(path string) os.FileInfo {
	wt.t.Helper()
	path = wt.LocalPath(path)
	stat, err := os.Stat(path)
	wt.assert.NoError(err)
	return stat
}

// Return the file metadata of the local file without `BlockIds`.
func (wt *WorkspaceTest) LocalFileMetadata(path string) *lib.FileMetadata {
	wt.t.Helper()
	stat := wt.LocalStat(path)
	md := &lib.FileMetadata{
		ModeAndPerm:   lib.NewModeAndPerm(stat.Mode()),
		MTimeSec:      stat.ModTime().Unix(),
		MTimeNSec:     int32(stat.ModTime().Nanosecond()), //nolint:gosec
		Size:          stat.Size(),
		FileHash:      lib.Sha256{},
		BlockIds:      nil,
		SymlinkTarget: "",
		UID:           0xffffffff,
		GID:           0xffffffff,
		BirthtimeSec:  -1,
		BirthtimeNSec: -1,
	}
	if stat.IsDir() {
		md.Size = 0
	}
	EnhanceMetadata(md, stat)
	return md
}

func (wt *WorkspaceTest) UpdateLocal(path string, content string) {
	wt.t.Helper()
	path = wt.LocalPath(path)
	err := os.WriteFile(path, []byte(content), 0o600)
	wt.assert.NoError(err)
}

func (wt *WorkspaceTest) RemoveLocal(path string) {
	wt.t.Helper()
	path = filepath.Join(wt.Workspace.WorkspacePath, path)
	err := os.RemoveAll(path)
	wt.assert.NoError(err)
}

func (wt *WorkspaceTest) UpdateLocalMTime(path string, t time.Time) {
	wt.t.Helper()
	path = wt.LocalPath(path)
	err := os.Chtimes(path, t, t)
	wt.assert.NoError(err)
}

func (wt *WorkspaceTest) UpdateLocalMode(path string, mode os.FileMode) {
	wt.t.Helper()
	path = wt.LocalPath(path)
	err := os.Chmod(path, mode)
	wt.assert.NoError(err)
}

type RepositoryTest struct {
	WorkspaceTest
	Repository        *lib.Repository
	RepositoryStorage *lib.FileStorage
	t                 *testing.T
	assert            lib.Assert
}

func NewRepositoryTest(t *testing.T) *RepositoryTest {
	t.Helper()
	assert := lib.NewAssert(t)
	repositoryDir := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.WalkDir(repositoryDir, func(path string, d os.DirEntry, err error) error {
			_ = os.Chmod(path, 0o777) //nolint:gosec
			return nil
		})
	})
	repository, storage := testRepository(t, repositoryDir)
	wt := NewWorkspaceTest(t, repositoryDir)
	return &RepositoryTest{
		WorkspaceTest:     *wt,
		Repository:        repository,
		RepositoryStorage: storage,
		t:                 t,
		assert:            assert,
	}
}

func (rt *RepositoryTest) RemoteHead() lib.RevisionId {
	rt.t.Helper()
	head, err := rt.Repository.Head()
	rt.assert.NoError(err)
	return head
}

func (rt *RepositoryTest) VerifyRevisionSnapshot(
	revisionId lib.RevisionId,
	pathFilter lib.PathFilter,
	files []FileInfo,
) {
	rt.t.Helper()
	entries := rt.RevisionSnapshot(revisionId, pathFilter)
	actual := []FileInfo{}
	for _, entry := range entries {
		content := ""
		if entry.Type != lib.RevisionEntryDelete && entry.Metadata.ModeAndPerm.IsRegular() {
			// Rebuild the content from the repository.
			buf := bytes.NewBuffer([]byte{})
			blockBuf := lib.BlockBuf{}
			for _, blockId := range entry.Metadata.BlockIds {
				data, _, err := rt.Repository.ReadBlock(blockId, blockBuf)
				rt.assert.NoError(err)
				buf.Write(data)
			}
			content = buf.String()
		}
		actual = append(actual, FileInfo{
			Path:    entry.Path.FSString(),
			Mode:    entry.Metadata.ModeAndPerm.AsFileMode(),
			Size:    int(entry.Metadata.Size),
			Content: content,
		})
	}
	rt.assert.Equal(files, actual)
}

func (rt *RepositoryTest) RevisionSnapshot(revisionId lib.RevisionId, pathFilter lib.PathFilter) []*lib.RevisionEntry {
	rt.t.Helper()
	tmpDir := filepath.Join(rt.t.TempDir(), "revision-snapshot")
	defer os.RemoveAll(tmpDir) //nolint:errcheck
	rt.assert.NoError(os.MkdirAll(tmpDir, 0o700))
	snapshot, err := lib.NewRevisionSnapshot(rt.Repository, revisionId, tmpDir)
	rt.assert.NoError(err)
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(pathFilter)
	entries := []*lib.RevisionEntry{}
	for {
		entry, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		rt.assert.NoError(err)
		entries = append(entries, entry)
	}
	return entries
}

func (rt *RepositoryTest) VerifyRevision(revisionId lib.RevisionId, expected []RevisionEntryInfo) {
	rt.t.Helper()
	revision, err := rt.Repository.ReadRevision(revisionId, lib.BlockBuf{})
	rt.assert.NoError(err)
	r := lib.NewRevisionReader(rt.Repository, &revision, lib.BlockBuf{})
	VerifyRevisionReader(rt.t, r, expected)
}

type RevisionEntryInfo struct {
	Path    string
	Type    lib.RevisionEntryType
	Mode    os.FileMode
	Content string
}

func VerifyRevisionTemp(t *testing.T, temp *lib.RevisionTemp, expected []RevisionEntryInfo) {
	t.Helper()
	r := temp.Reader(nil)
	VerifyRevisionReader(t, r, expected)
}

func VerifyRevisionReader(t *testing.T, r lib.RevisionEntryReader, expected []RevisionEntryInfo) {
	t.Helper()
	assert := lib.NewAssert(t)
	i := 0
	for {
		entry, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(err)
		assert.Less(i, len(expected), "not enough expected entries, at temp entry: %v", entry)
		expected := expected[i]
		assert.Equal(entry.Path.FSString(), expected.Path, "path of %s", entry.Path.FSString())
		assert.Equal(entry.Metadata.Size, int64(len(expected.Content)), "size of %s", entry.Path.FSString())
		assert.Equal(
			entry.Metadata.ModeAndPerm,
			lib.NewModeAndPerm(expected.Mode),
			"mode of %s",
			entry.Path.FSString(),
		)
		assert.Equal(entry.Type, expected.Type, "type of %s", entry.Path.FSString())
		if expected.Type != lib.RevisionEntryDelete {
			if expected.Mode.IsRegular() {
				hash := sha256.New()
				_, err = hash.Write([]byte(expected.Content))
				assert.NoError(err)
				assert.Equal(lib.Sha256(hash.Sum(nil)), entry.Metadata.FileHash, "hash of %s", entry.Path.FSString())
			} else {
				assert.Equal(lib.Sha256{}, entry.Metadata.FileHash, "hash of directory %s has to be empty", entry.Path.FSString())
			}
		}
		i++
	}
	assert.Equal(len(expected), i, "not enough revision entries, expected file: %v", expected[len(expected)-1])
}

type FileInfo struct {
	Path    string
	Mode    os.FileMode
	Size    int
	Content string
}

func fakeMergeOptions() *MergeOptions {
	return &MergeOptions{NewTestStagingMonitor(), NewTestCpMonitor(), NewTestCommitMonitor(), "author", "message"}
}

type testStagingMonitor struct{}

func NewTestStagingMonitor() *testStagingMonitor {
	return &testStagingMonitor{}
}

func (m *testStagingMonitor) OnStart(path string, dirEntry os.DirEntry) {
}

func (m *testStagingMonitor) OnEnd(path string, excluded bool, metadata *lib.FileMetadata) {
}

func (m *testStagingMonitor) Close() {
}

type testCommitMonitor struct{}

func NewTestCommitMonitor() *testCommitMonitor {
	return &testCommitMonitor{}
}

func (m *testCommitMonitor) OnBeforeCommit() error {
	return nil
}

func (m *testCommitMonitor) OnStart(entry *lib.RevisionEntry) {
}

func (m *testCommitMonitor) OnAddBlock(
	entry *lib.RevisionEntry,
	header *lib.BlockHeader,
	existed bool,
	dataSize int64,
) {
}

func (m *testCommitMonitor) OnEnd(entry *lib.RevisionEntry) {
}

func testRepository(t *testing.T, dir string) (*lib.Repository, *lib.FileStorage) {
	t.Helper()
	userPassphrase := []byte("user passphrase")
	assert := lib.NewAssert(t)
	storage, err := lib.NewFileStorage(dir, lib.StoragePurposeRepository)
	assert.NoError(err)
	repo, err := lib.InitNewRepository(storage, userPassphrase)
	assert.NoError(err)
	return repo, storage
}
