package workspace

import (
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestCp(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		tmp := filepath.Join(t.TempDir(), "tmp")
		out := filepath.Join(t.TempDir(), "cp")
		assert.NoError(os.MkdirAll(out, 0o700))
		rt := NewRepositoryTest(t)
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b.txt", "b")
		rt.AddLocal("c/1.txt", "c1")
		rt.AddLocal("c/d/2.txt", "c2")
		revId1, err := Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig(), t.TempDir())
		assert.NoError(err)

		rt.UpdateLocal("a.txt", "A")
		rt.AddLocal("c/3.txt", "c3")
		rt.UpdateLocalMode("b.txt", 0o777)
		revId2, err := Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig(), t.TempDir())
		assert.NoError(err)

		// Copy all from rev1.
		assert.NoError(os.MkdirAll(tmp, 0o700))
		err = Cp(rt.WorkspacePath, rt.Repository, out, &CpOptions{revId1, &TestCpMonitor{}, nil}, tmp)
		assert.NoError(err)
		assert.Equal([]PathInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 2, "c1"},
			{"c/d", 0o700 | os.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "c2"},
		}, readDir(t, out))

		// Copy all from the rev2.
		assert.NoError(os.RemoveAll(tmp))
		assert.NoError(os.MkdirAll(tmp, 0o700))
		err = Cp(rt.WorkspacePath, rt.Repository, out, &CpOptions{revId2, &TestCpMonitor{}, nil}, tmp)
		assert.NoError(err)
		assert.Equal([]PathInfo{
			{"a.txt", 0o600, 1, "A"},
			{"b.txt", 0o777, 1, "b"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 2, "c1"},
			{"c/3.txt", 0o600, 2, "c3"},
			{"c/d", 0o700 | os.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "c2"},
		}, readDir(t, out))
	})

	t.Run("Parent directory without rx permission is not created", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		tmp := t.TempDir()
		out := t.TempDir()
		t.Cleanup(func() {
			_ = filepath.WalkDir(out, func(path string, d os.DirEntry, err error) error {
				_ = os.Chmod(path, 0o777) //nolint:gosec
				return nil
			})
		})
		assert.NoError(os.MkdirAll(out, 0o700))
		assert.NoError(os.MkdirAll(tmp, 0o700))
		rt := NewRepositoryTest(t)
		rt.AddLocal("c/1.txt", "c1")
		rt.UpdateLocalMode("c", 0o500)

		revId1, err := Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig(), t.TempDir())
		assert.NoError(err)

		// Copy all from the rev1.
		assert.NoError(os.RemoveAll(tmp))
		assert.NoError(os.MkdirAll(tmp, 0o700))
		err = Cp(rt.WorkspacePath, rt.Repository, out, &CpOptions{revId1, &TestCpMonitor{}, nil}, tmp)
		assert.NoError(err)
		assert.Equal([]PathInfo{
			{"c", 0o500 | os.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 2, "c1"},
		}, readDir(t, out))
	})

	t.Run("PathFilter", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		tmp := filepath.Join(t.TempDir(), "tmp")
		out := filepath.Join(t.TempDir(), "cp")
		assert.NoError(os.MkdirAll(out, 0o700))
		assert.NoError(os.MkdirAll(tmp, 0o700))
		rt := NewRepositoryTest(t)
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b.txt", "b")
		rt.AddLocal("c/1.txt", "c1")
		rt.AddLocal("c/d/2.txt", "c2")
		revId1, err := Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig(), t.TempDir())
		assert.NoError(err)

		pattern, err := lib.NewPathPattern("c/**/*")
		assert.NoError(err)
		pathFilter := &lib.PathInclusionFilter{Includes: []lib.PathPattern{pattern}}
		err = Cp(rt.WorkspacePath, rt.Repository, out, &CpOptions{revId1, &TestCpMonitor{}, pathFilter}, tmp)
		assert.NoError(err)
		assert.Equal([]PathInfo{
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 2, "c1"},
			{"c/d", 0o700 | os.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "c2"},
		}, readDir(t, out))
	})

	t.Run("FileMode is restored (as much as possible)", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		tmp := filepath.Join(t.TempDir(), "tmp")
		out := filepath.Join(t.TempDir(), "cp")
		assert.NoError(os.MkdirAll(out, 0o700))
		assert.NoError(os.MkdirAll(tmp, 0o700))
		rt := NewRepositoryTest(t)

		rt.AddLocal("a.txt", "a")

		// Set the gid of the file to a different group.
		currentUser, err := user.Current()
		assert.NoError(err)
		groups, err := currentUser.GroupIds()
		assert.NoError(err)
		gid := 0
		for _, g := range groups {
			if g == currentUser.Gid {
				continue
			}
			gi, err := strconv.Atoi(g)
			assert.NoError(err)
			gid = gi
		}
		assert.NotEqual(0, gid, "current user has no additional groups")
		err = os.Chown(rt.LocalPath("a.txt"), -1, gid)
		assert.NoError(err)

		// Set `setuid`, `setgid`, and `sticky`.
		err = os.Chmod(rt.LocalPath("a.txt"), 0o777|os.ModeSetuid|os.ModeSetgid|os.ModeSticky)
		assert.NoError(err)

		// Commit.
		revId1, err := Commit(rt.WorkspacePath, rt.Repository, fakeCommitConfig(), t.TempDir())
		assert.NoError(err)

		// Copy all from the rev1.
		err = Cp(rt.WorkspacePath, rt.Repository, out, &CpOptions{revId1, &TestCpMonitor{}, nil}, tmp)
		assert.NoError(err)

		stat := rt.LocalStat("a.txt")
		cpStat, err := os.Stat(filepath.Join(out, "a.txt"))
		assert.NoError(err)
		// We do not preserve setuid, setgid, and sticky.
		assert.Equal(stat.Mode()&os.ModePerm, cpStat.Mode())
		assert.Equal(stat.Size(), cpStat.Size())
		assert.Equal(stat.ModTime(), cpStat.ModTime())
		if sys, ok := stat.Sys().(*syscall.Stat_t); ok {
			cpSys, ok := cpStat.Sys().(*syscall.Stat_t)
			assert.Equal(true, ok)
			assert.Equal(sys.Gid, cpSys.Gid)
			assert.Equal(sys.Uid, cpSys.Uid)
		}
	})
}

type PathInfo struct {
	Path    string
	Mode    os.FileMode
	Size    int64
	Content string
}

func readDir(t *testing.T, basePath string) []PathInfo {
	t.Helper()
	pathInfos := []PathInfo{}
	assert := lib.NewAssert(t)
	err := filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name, err := filepath.Rel(basePath, path)
		assert.NoError(err)
		if name == "." {
			return nil
		}
		info, err := os.Stat(path)
		assert.NoError(err)
		content := ""
		var size int64
		if !info.IsDir() {
			c, err := os.ReadFile(path)
			assert.NoError(err)
			content = string(c)
			size = info.Size()
		}
		pathInfos = append(pathInfos, PathInfo{
			Path:    name,
			Mode:    info.Mode(),
			Size:    size,
			Content: content,
		})
		return nil
	})
	assert.NoError(err)
	return pathInfos
}

type TestCpMonitor struct{}

func (m *TestCpMonitor) OnStart(entry *lib.RevisionEntry, targetPath string) {
}

func (m *TestCpMonitor) OnWrite(entry *lib.RevisionEntry, targetPath string, blockId lib.BlockId, data []byte) {
}

func (m *TestCpMonitor) OnEnd(entry *lib.RevisionEntry, targetPath string) {
}

func (m *TestCpMonitor) OnError(entry *lib.RevisionEntry, targetPath string, err error) CpOnError {
	return CpOnErrorAbort
}
