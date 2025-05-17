package workspace

import (
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestCommit(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)
		rt.AddLocal("a.txt", ".")
		rt.AddLocal("b.txt", "..")
		rt.AddLocal("c/1.txt", "...")
		rt.AddLocal("c/d/2.txt", "....")
		revId, err := Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(revId, nil, []FileInfo{
			{"a.txt", 1},
			{"b.txt", 2},
			{"c", 0},
			{"c/1.txt", 3},
			{"c/d", 0},
			{"c/d/2.txt", 4},
		})

		rt.RemoveLocal("c/d")
		rt.RemoveLocal("b.txt")
		rt.AddLocal("e.txt", ".....")
		revId, err = Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(revId, nil, []FileInfo{
			{"a.txt", 1},
			{"c", 0},
			{"c/1.txt", 3},
			{"e.txt", 5},
		})
	})

	t.Run("Ignore files", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)
		rt.AddLocal("a.txt", ".")
		rt.AddLocal("b.md", "..")
		rt.AddLocal("c/1.txt", "...")
		rt.AddLocal("c/d/2.md", "....")
		rt.AddLocal("c/e/3.md", ".....")
		pathFilter, err := lib.NewPathExclusionFilter([]string{"**/*.txt", "**/e"}, []string{})
		assert.NoError(err)

		revId, err := Commit(
			rt.WorkspacePath,
			rt.Repository,
			&CommitConfig{PathFilter: pathFilter, Author: "author", Message: "message"},
		)
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(revId, nil, []FileInfo{
			{"b.md", 2},
			{"c", 0},
			{"c/d", 0},
			{"c/d/2.md", 4},
		})

		// Ignoring `c` should not delete `c` from the repository and should not commit
		// changes to `c` to the repository.
		rt.AddLocal("c/3.md", "......")
		rt.AddLocal("b.nfo", ".......")
		pathFilter, err = lib.NewPathExclusionFilter([]string{"**/*.txt", "**/e", "c"}, []string{})
		assert.NoError(err)
		revId, err = Commit(
			rt.WorkspacePath,
			rt.Repository,
			&CommitConfig{PathFilter: pathFilter, Author: "author", Message: "message"},
		)
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(revId, nil, []FileInfo{
			{"b.md", 2},
			{"b.nfo", 7},
			{"c", 0},
			{"c/d", 0},
			{"c/d/2.md", 4},
		})

		// Commit without ignoring any files.
		revId, err = Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig())
		assert.NoError(err)
		rt.VerifyRevisionSnapshot(revId, nil, []FileInfo{
			{"a.txt", 1},
			{"b.md", 2},
			{"b.nfo", 7},
			{"c", 0},
			{"c/1.txt", 3},
			{"c/3.md", 6},
			{"c/d", 0},
			{"c/d/2.md", 4},
			{"c/e", 0},
			{"c/e/3.md", 5},
		})
	})

	t.Run("FileMetadata: ModeAndPerm", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)
		check := func(path string, fileMode os.FileMode, expected lib.ModeAndPerm) {
			t.Helper()
			assert.NoError(os.Chmod(rt.LocalPath(path), fileMode))
			revId, err := Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig())
			assert.NoError(err)
			entries := rt.RevisionSnapshot(revId, nil)
			assert.Equal(2, len(entries))
			entryIndex := slices.IndexFunc(
				entries,
				func(e *lib.RevisionEntry) bool { return e.Path.FSString() == path },
			)
			assert.Equal(expected, entries[entryIndex].Metadata.ModeAndPerm, path)
		}
		defer func() {
			// Make sure the temporary directory can be removed.
			_ = os.Chmod(rt.LocalPath("a"), 0o700)       //nolint:gosec
			_ = os.Chmod(rt.LocalPath("a/b.txt"), 0o700) //nolint:gosec
		}()
		rt.AddLocal("a/b.txt", ".")
		for i := range 9 {
			mode := lib.ModeAndPerm(lib.ModeDir | (1 << i) | 0o500) // Directory must always be `xr`.
			check("a", os.FileMode(mode), mode)
		}
		assert.NoError(os.Chmod(rt.LocalPath("a"), 0o700)) //nolint:gosec
		for i := range 9 {
			mode := lib.ModeAndPerm((1 << i) | 0o400) // File must always be `r`.
			check("a/b.txt", os.FileMode(mode), mode)
		}
		check("a", os.ModeSticky|0o700, lib.ModeAndPerm(lib.ModeSticky|lib.ModeDir|0o700))
		check("a", os.ModeSetgid|0o700, lib.ModeAndPerm(lib.ModeSetGID|lib.ModeDir|0o700))
		check("a", os.ModeSetuid|0o700, lib.ModeAndPerm(lib.ModeSetUID|lib.ModeDir|0o700))
	})

	// todo: test symlink

	t.Run("FileMetadata: FileHash, Size, MTime, UID, GID, Birthtime", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)
		rt.AddLocal("a/b.txt", "123")
		stat, err := os.Stat(rt.LocalPath("a/b.txt"))
		assert.NoError(err)
		revId, err := Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig())
		assert.NoError(err)
		entries := rt.RevisionSnapshot(revId, nil)
		assert.Equal(2, len(entries))
		entry := entries[1]
		md := entries[1].Metadata
		assert.Equal("a/b.txt", entry.Path.FSString())
		assert.Equal(stat.ModTime().Unix(), md.MTimeSec)
		assert.Equal(stat.ModTime().Nanosecond(), int(md.MTimeNSec))
		assert.Equal(int64(3), md.Size)
		assert.Equal(
			"a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3",
			hex.EncodeToString(md.FileHash[:]),
		)

		if stat, ok := stat.Sys().(*syscall.Stat_t); ok {
			assert.Equal(stat.Uid, md.UID)
			assert.Equal(stat.Gid, md.GID)
			assert.Equal(stat.Birthtimespec.Sec, md.BirthtimeSec)
			assert.Equal(stat.Birthtimespec.Nsec, int64(md.BirthtimeNSec))
		} else {
			t.Log("Extented file stat not available")
			assert.Equal(0xffffffff, md.UID)
			assert.Equal(0xffffffff, md.GID)
			assert.Equal(-1, md.BirthtimeSec)
			assert.Equal(-1, int64(md.BirthtimeNSec))
		}
	})
}

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
