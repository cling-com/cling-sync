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

		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add three commits.
		w.Write("a.txt", "a")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		w.Write("b.txt", "b")
		revId2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		w.Write("c.txt", "c")
		revId3, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// List all revisions.
		logs, err := Log(r.Repository, &LogOptions{nil, false})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId3, nil),
			revisionLog(t, r, revId2, nil),
			revisionLog(t, r, revId1, nil),
		}, newTestRevisionLogs(logs, false))
	})

	t.Run("Status", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add first commit.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/d.txt", "d")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add second commit.
		w.Write("c/e.txt", "e")
		w.Rm("a.txt")
		w.Write("b.txt", "bb")
		revId2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// List all revisions.
		logs, err := Log(r.Repository, &LogOptions{nil, true})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId2, []TestStatusFile{
				{"a.txt", lib.RevisionEntryDelete, 1},
				{"b.txt", lib.RevisionEntryUpdate, 2},
				{"c", lib.RevisionEntryUpdate, 0},
				{"c/e.txt", lib.RevisionEntryAdd, 1},
			}),
			revisionLog(t, r, revId1, []TestStatusFile{
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
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add three commits.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/d.txt", "d")
		revId1, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		w.Write("c/e.txt", "e")
		revId2, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		w.Rm("a.txt")
		revId3, err := Merge(w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// PathFilter on `a.txt` without status.
		pathFilter, err := lib.NewPathInclusionFilter([]string{"a.txt"})
		assert.NoError(err)
		logs, err := Log(r.Repository, &LogOptions{pathFilter, false})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId3, nil),
			revisionLog(t, r, revId1, nil),
		}, newTestRevisionLogs(logs, false))

		// PathFilter on `a.txt` with status.
		logs, err = Log(r.Repository, &LogOptions{pathFilter, true})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId3, []TestStatusFile{{"a.txt", lib.RevisionEntryDelete, 1}}),
			revisionLog(t, r, revId1, []TestStatusFile{{"a.txt", lib.RevisionEntryAdd, 1}}),
		}, newTestRevisionLogs(logs, true))

		// PathFilter on `c/*` with status.
		pathFilter, err = lib.NewPathInclusionFilter([]string{"c/*"})
		assert.NoError(err)
		logs, err = Log(r.Repository, &LogOptions{pathFilter, true})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId2, []TestStatusFile{{"c/e.txt", lib.RevisionEntryAdd, 1}}),
			revisionLog(t, r, revId1, []TestStatusFile{{"c/d.txt", lib.RevisionEntryAdd, 1}}),
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

func revisionLog(t *testing.T, r *lib.TestRepository, revId lib.RevisionId, files []TestStatusFile) TestRevisionLog {
	t.Helper()
	revision, err := r.ReadRevision(revId)
	lib.NewAssert(t).NoError(err)
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
