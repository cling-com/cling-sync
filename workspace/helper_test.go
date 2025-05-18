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
	Repository    *lib.Repository
	Storage       lib.Storage
	Workspace     *Workspace
	WorkspacePath string
	t             *testing.T
	assert        lib.Assert
}

func NewRepositoryTest(t *testing.T) *RepositoryTest {
	t.Helper()
	assert := lib.NewAssert(t)
	repositoryDir := filepath.Join(t.TempDir(), "repository")
	workspacePath := filepath.Join(t.TempDir(), "local")
	repository, storage := testRepository(t, repositoryDir)
	workspace, err := NewWorkspace(workspacePath, RemoteRepository(repositoryDir))
	assert.NoError(err)
	return &RepositoryTest{
		Repository:    repository,
		Storage:       storage,
		Workspace:     workspace,
		WorkspacePath: workspace.WorkspacePath,
		t:             t,
		assert:        assert,
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
		rt.assert.Equal(entry.Path.FSString(), file.Path, "path of %s", entry.Path.FSString())
		rt.assert.Equal(entry.Metadata.Size, int64(file.Size), "size of %s", entry.Path.FSString())
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
	defer snapshot.Close() //nolint:errcheck
	reader, err := snapshot.Reader(pathFilter)
	rt.assert.NoError(err)
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

func fakeCommitConfig() *CommitConfig {
	return &CommitConfig{PathFilter: nil, Author: "author", Message: "message"}
}

func testRepository(t *testing.T, dir string) (*lib.Repository, *lib.FileStorage) {
	t.Helper()
	userPassphrase := []byte("user passphrase")
	assert := lib.NewAssert(t)
	storage, err := lib.NewFileStorage(dir)
	assert.NoError(err)
	repo, err := lib.InitNewRepository(storage, userPassphrase)
	assert.NoError(err)
	return repo, storage
}
