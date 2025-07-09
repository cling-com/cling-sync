package workspace

import (
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestLog(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)

		// Add three commits.
		rt.AddLocal("a.txt", "a")
		revId1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.AddLocal("b.txt", "b")
		revId2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.AddLocal("c.txt", "c")
		revId3, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		// List all revisions.
		logs, err := Log(rt.Repository, &LogOptions{nil, false})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(rt, revId3, nil),
			revisionLog(rt, revId2, nil),
			revisionLog(rt, revId1, nil),
		}, newTestRevisionLogs(logs, false))
	})

	t.Run("Status", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)

		// Add first commit.
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b.txt", "b")
		rt.AddLocal("c/d.txt", "d")
		revId1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		// Add second commit.
		rt.AddLocal("c/e.txt", "e")
		rt.RemoveLocal("a.txt")
		rt.UpdateLocal("b.txt", "bb")
		revId2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		// List all revisions.
		logs, err := Log(rt.Repository, &LogOptions{nil, true})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(rt, revId2, []TestStatusFile{
				{"a.txt", lib.RevisionEntryDelete, 1},
				{"b.txt", lib.RevisionEntryUpdate, 2},
				{"c", lib.RevisionEntryUpdate, 0},
				{"c/e.txt", lib.RevisionEntryAdd, 1},
			}),
			revisionLog(rt, revId1, []TestStatusFile{
				{"a.txt", lib.RevisionEntryAdd, 1},
				{"b.txt", lib.RevisionEntryAdd, 1},
				{"c", lib.RevisionEntryAdd, 0},
				{"c/d.txt", lib.RevisionEntryAdd, 1},
			}),
		}, newTestRevisionLogs(logs, true))
	})

	t.Run("PathFilter", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		rt := NewRepositoryTest(t)

		// Add three commits.
		rt.AddLocal("a.txt", "a")
		rt.AddLocal("b.txt", "b")
		rt.AddLocal("c/d.txt", "d")
		revId1, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.AddLocal("c/e.txt", "e")
		revId2, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)
		rt.RemoveLocal("a.txt")
		revId3, err := Merge(rt.Workspace, rt.Repository, fakeMergeOptions())
		assert.NoError(err)

		// PathFilter on `a.txt` without status.
		pathFilter, err := lib.NewPathInclusionFilter([]string{"a.txt"})
		assert.NoError(err)
		logs, err := Log(rt.Repository, &LogOptions{pathFilter, false})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(rt, revId3, nil),
			revisionLog(rt, revId1, nil),
		}, newTestRevisionLogs(logs, false))

		// PathFilter on `a.txt` with status.
		logs, err = Log(rt.Repository, &LogOptions{pathFilter, true})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(rt, revId3, []TestStatusFile{{"a.txt", lib.RevisionEntryDelete, 1}}),
			revisionLog(rt, revId1, []TestStatusFile{{"a.txt", lib.RevisionEntryAdd, 1}}),
		}, newTestRevisionLogs(logs, true))

		// PathFilter on `c/*` with status.
		pathFilter, err = lib.NewPathInclusionFilter([]string{"c/*"})
		assert.NoError(err)
		logs, err = Log(rt.Repository, &LogOptions{pathFilter, true})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(rt, revId2, []TestStatusFile{{"c/e.txt", lib.RevisionEntryAdd, 1}}),
			revisionLog(rt, revId1, []TestStatusFile{{"c/d.txt", lib.RevisionEntryAdd, 1}}),
		}, newTestRevisionLogs(logs, true))
	})
}

type TestRevisionLog struct {
	RevisionId lib.RevisionId
	Revision   lib.Revision
	Files      []TestStatusFile
}

type TestStatusFile struct {
	Path string
	Type lib.RevisionEntryType
	Size int
}

func revisionLog(rt *RepositoryTest, revId lib.RevisionId, files []TestStatusFile) TestRevisionLog {
	rt.t.Helper()
	revision, err := rt.Repository.ReadRevision(revId)
	rt.assert.NoError(err)
	return TestRevisionLog{revId, revision, files}
}

func newTestRevisionLogs(logs []RevisionLog, status bool) []TestRevisionLog {
	testLogs := []TestRevisionLog{}
	for _, log := range logs {
		testLogs = append(testLogs, newTestRevisionLog(log, status))
	}
	return testLogs
}

func newTestRevisionLog(log RevisionLog, status bool) TestRevisionLog {
	var files []TestStatusFile
	if status {
		files = []TestStatusFile{}
		for _, file := range log.Files {
			files = append(files, TestStatusFile{file.Path, file.Type, int(file.Metadata.Size)})
		}
	}
	return TestRevisionLog{log.RevisionId, log.Revision, files}
}
