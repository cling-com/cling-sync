package workspace

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

type RepositoryTest struct {
	Repository        *lib.Repository
	RepositoryStorage lib.Storage
	Workspace         *Workspace
	t                 *testing.T
	assert            lib.Assert
}

func NewRepositoryTest(t *testing.T) *RepositoryTest {
	t.Helper()
	assert := lib.NewAssert(t)
	repositoryDir := t.TempDir()
	workspacePath := t.TempDir()
	t.Cleanup(func() {
		for _, dir := range []string{repositoryDir, workspacePath} {
			_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
				_ = os.Chmod(path, 0o777) //nolint:gosec
				return nil
			})
		}
	})
	repository, storage := testRepository(t, repositoryDir)
	workspace, err := NewWorkspace(workspacePath, RemoteRepository(repositoryDir))
	assert.NoError(err)
	return &RepositoryTest{
		Repository:        repository,
		RepositoryStorage: storage,
		Workspace:         workspace,
		t:                 t,
		assert:            assert,
	}
}

func (rt *RepositoryTest) LocalPath(path string) string {
	rt.t.Helper()
	return filepath.Join(rt.Workspace.WorkspacePath, path)
}

func (rt *RepositoryTest) AddLocal(path string, content string) {
	rt.t.Helper()
	path = rt.LocalPath(path)
	rt.assert.NoError(os.MkdirAll(filepath.Dir(path), 0o700))
	err := os.WriteFile(path, []byte(content), 0o600)
	rt.assert.NoError(err)
}

func (rt *RepositoryTest) LocalStat(path string) os.FileInfo {
	rt.t.Helper()
	path = rt.LocalPath(path)
	stat, err := os.Stat(path)
	rt.assert.NoError(err)
	return stat
}

func (rt *RepositoryTest) UpdateLocal(path string, content string) {
	rt.t.Helper()
	path = rt.LocalPath(path)
	err := os.WriteFile(path, []byte(content), 0o600)
	rt.assert.NoError(err)
}

func (rt *RepositoryTest) RemoveLocal(path string) {
	rt.t.Helper()
	path = filepath.Join(rt.Workspace.WorkspacePath, path)
	err := os.RemoveAll(path)
	rt.assert.NoError(err)
}

func (rt *RepositoryTest) UpdateLocalMTime(path string) {
	rt.t.Helper()
	path = rt.LocalPath(path)
	err := os.Chtimes(path, time.Now(), time.Now())
	rt.assert.NoError(err)
}

func (rt *RepositoryTest) UpdateLocalMode(path string, mode os.FileMode) {
	rt.t.Helper()
	path = rt.LocalPath(path)
	err := os.Chmod(path, mode)
	rt.assert.NoError(err)
}

func (rt *RepositoryTest) VerifyRevisionSnapshot(
	revisionId lib.RevisionId,
	pathFilter lib.PathFilter,
	files []FileInfo,
) {
	rt.t.Helper()
	entries := rt.RevisionSnapshot(revisionId, pathFilter)
	for i, entry := range entries {
		rt.assert.Equal(true, i < len(files), "not enough files, expected entry: %v", entry)
		file := files[i]
		stat, err := os.Stat(rt.LocalPath(file.Path))
		rt.assert.NoError(err)
		rt.assert.Equal(entry.Path.FSString(), file.Path, "path of %s", entry.Path.FSString())
		rt.assert.Equal(entry.Metadata.Size, int64(file.Size), "size of %s", entry.Path.FSString())
		rt.assert.Equal(
			entry.Metadata.ModeAndPerm,
			lib.NewModeAndPerm(stat.Mode()),
			"mode of %s",
			entry.Path.FSString(),
		)
		if stat.Mode().IsRegular() {
			expectedContent, err := os.ReadFile(rt.LocalPath(file.Path))
			rt.assert.NoError(err)
			// Rebuild the content from the repository.
			var content []byte
			blockBuf := lib.BlockBuf{}
			for _, blockId := range entry.Metadata.BlockIds {
				data, _, err := rt.Repository.ReadBlock(blockId, blockBuf)
				rt.assert.NoError(err)
				content = append(content, data...)
			}
			rt.assert.Equal(string(expectedContent), string(content), "content of %s", entry.Path.FSString())
		}
	}
	rt.assert.Equal(len(files), len(entries), "not enough revision entries, expected file: %v", files[len(entries)-1])
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

type FileInfo struct {
	Path string
	Size int
}

func fakeCommitConfig() *CommitOptions {
	return &CommitOptions{nil, "author", "message", newTestStagingMonitor(), nopOnBeforeCommit}
}

func nopOnBeforeCommit() error {
	return nil
}

type testStagingMonitor struct{}

func newTestStagingMonitor() *testStagingMonitor {
	return &testStagingMonitor{}
}

func (m *testStagingMonitor) OnStart(path string, dirEntry os.DirEntry) {
}

func (m *testStagingMonitor) OnAddBlock(path string, header *lib.BlockHeader, existed bool, dataSize int64) {
}

func (m *testStagingMonitor) OnError(path string, err error) StagingOnError {
	return StagingOnErrorAbort
}

func (m *testStagingMonitor) OnEnd(path string, excluded bool, metadata *lib.FileMetadata) {
}

func (m *testStagingMonitor) Close() {
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
