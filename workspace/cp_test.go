package workspace

import (
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
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
		rt.AddLocal("c/1.txt", "c")
		rt.AddLocal("c/d/2.txt", "cc")
		revId1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		rt.UpdateLocal("a.txt", "a")
		revId2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		// Copy all from rev1.
		assert.NoError(os.MkdirAll(tmp, 0o700))
		err = Cp(rt.Repository, out, &CpOptions{revId1, NewTestCpMonitor(), nil}, tmp)
		assert.NoError(err)
		assert.Equal([]FileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 1, "c"},
			{"c/d", 0o700 | os.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "cc"},
		}, readDir(t, out))

		// Trying to copy from rev2 should fail, because files already exist.
		assert.NoError(os.RemoveAll(tmp))
		assert.NoError(os.MkdirAll(tmp, 0o700))
		err = Cp(rt.Repository, out, &CpOptions{revId2, NewTestCpMonitor(), nil}, tmp)
		assert.Error(err, "failed to copy")
		assert.Error(err, "file exists")
	})

	t.Run("Overwrite", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		tmp := t.TempDir()
		targetDir := t.TempDir()
		assert.NoError(os.MkdirAll(targetDir, 0o700))
		rt := NewRepositoryTest(t)
		rt.AddLocal("a.txt", "aaa")
		rt.AddLocal("b.txt", "b")
		rt.AddLocal("c/1.txt", "c")
		rt.AddLocal("c/d/2.txt", "cc")
		revId1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		// We make the file smaller to ensure it is truncated before overwriting.
		rt.UpdateLocal("a.txt", "a")
		// Removing a file should not affect anything.
		rt.RemoveLocal("c/1.txt")
		rt.AddLocal("c/3.txt", "ccc")
		rt.UpdateLocalMode("b.txt", 0o777)
		revId2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		// Copy all from rev1.
		assert.NoError(os.MkdirAll(tmp, 0o700))
		err = Cp(rt.Repository, targetDir, &CpOptions{revId1, NewTestCpMonitor(), nil}, tmp)
		assert.NoError(err)
		assert.Equal([]FileInfo{
			{"a.txt", 0o600, 3, "aaa"},
			{"b.txt", 0o600, 1, "b"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 1, "c"},
			{"c/d", 0o700 | os.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "cc"},
		}, readDir(t, targetDir))

		// Copy all from the rev2.
		assert.NoError(os.RemoveAll(tmp))
		assert.NoError(os.MkdirAll(tmp, 0o700))
		err = Cp(rt.Repository, targetDir, &CpOptions{revId2, NewTestCpMonitorOverwrite(), nil}, tmp)
		assert.NoError(err)
		assert.Equal([]FileInfo{
			{"a.txt", 0o600, 1, "a"},
			{"b.txt", 0o777, 1, "b"},
			{"c", 0o700 | os.ModeDir, 0, ""},
			{"c/1.txt", 0o600, 1, "c"},
			{"c/3.txt", 0o600, 3, "ccc"},
			{"c/d", 0o700 | os.ModeDir, 0, ""},
			{"c/d/2.txt", 0o600, 2, "cc"},
		}, readDir(t, targetDir))
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

		revId1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		// Copy all from the rev1.
		assert.NoError(os.RemoveAll(tmp))
		assert.NoError(os.MkdirAll(tmp, 0o700))
		err = Cp(rt.Repository, out, &CpOptions{revId1, NewTestCpMonitor(), nil}, tmp)
		assert.NoError(err)
		assert.Equal([]FileInfo{
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
		revId1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		pattern, err := lib.NewPathPattern("c/**/*")
		assert.NoError(err)
		pathFilter := &lib.PathInclusionFilter{Includes: []lib.PathPattern{pattern}}
		err = Cp(rt.Repository, out, &CpOptions{revId1, NewTestCpMonitor(), pathFilter}, tmp)
		assert.NoError(err)
		assert.Equal([]FileInfo{
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
		revId1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		// Copy all from the rev1.
		err = Cp(rt.Repository, out, &CpOptions{revId1, NewTestCpMonitor(), nil}, tmp)
		assert.NoError(err)

		stat := rt.LocalStat("a.txt")
		cpStat, err := os.Stat(filepath.Join(out, "a.txt"))
		assert.NoError(err)
		// We do not preserve setuid, setgid, and sticky.
		assert.Equal(stat.Mode()&os.ModePerm, cpStat.Mode())
		assert.Equal(stat.Size(), cpStat.Size())
		assert.Equal(stat.ModTime(), cpStat.ModTime())
		// Verify extended metadata if supported. We use `EnhanceMetadata` to test this,
		// because it is provides platform specific implementations.
		md := lib.FileMetadata{GID: lib.UIDUnset, UID: lib.UIDUnset} //nolint:exhaustruct
		EnhanceMetadata(&md, stat)
		if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
			assert.NotEqual(lib.UIDUnset, md.UID, "uid should be set on darwin and linux")
			assert.NotEqual(lib.UIDUnset, md.GID, "gid should be set on darwin and linux")
		}
		cpMd := lib.FileMetadata{GID: lib.UIDUnset, UID: lib.UIDUnset} //nolint:exhaustruct
		EnhanceMetadata(&cpMd, cpStat)
		// We don't want to compare `BirthtimeSec` and `BirthtimeNSec` because `Birthtime` is not
		// restored and cannot be restored.
		md.BirthtimeSec = 0
		md.BirthtimeNSec = 0
		cpMd.BirthtimeSec = 0
		cpMd.BirthtimeNSec = 0
		assert.Equal(md, cpMd)
	})
}

func readDir(t *testing.T, basePath string) []FileInfo {
	t.Helper()
	fileInfos := []FileInfo{}
	assert := lib.NewAssert(t)
	err := filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Base(path) == ".cling" {
			return filepath.SkipDir
		}
		name, err := filepath.Rel(basePath, path)
		assert.NoError(err)
		if name == "." {
			return nil
		}
		info, err := os.Stat(path)
		assert.NoError(err)
		content := ""
		var size int
		if !info.IsDir() {
			c, err := os.ReadFile(path)
			assert.NoError(err)
			content = string(c)
			size = int(info.Size())
		}
		fileInfos = append(fileInfos, FileInfo{
			Path:    name,
			Mode:    info.Mode(),
			Size:    size,
			Content: content,
		})
		return nil
	})
	assert.NoError(err)
	return fileInfos
}

type TestCpMonitor struct {
	Exists CpOnExists
}

func NewTestCpMonitor() *TestCpMonitor {
	return &TestCpMonitor{CpOnExistsAbort}
}

func NewTestCpMonitorOverwrite() *TestCpMonitor {
	return &TestCpMonitor{CpOnExistsOverwrite}
}

func (m *TestCpMonitor) OnStart(entry *lib.RevisionEntry, targetPath string) {
}

func (m *TestCpMonitor) OnWrite(entry *lib.RevisionEntry, targetPath string, blockId lib.BlockId, data []byte) {
}

func (m *TestCpMonitor) OnEnd(entry *lib.RevisionEntry, targetPath string) {
}

func (m *TestCpMonitor) OnError(entry *lib.RevisionEntry, targetPath string, err error) CpOnError {
	return CpOnErrorAbort
}

func (m *TestCpMonitor) OnExists(entry *lib.RevisionEntry, targetPath string) CpOnExists {
	return m.Exists
}
