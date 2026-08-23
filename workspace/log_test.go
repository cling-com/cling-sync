package workspace

import (
	"testing"

	"github.com/cling-com/cling-sync/lib"
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
		revId1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		w.Write("b.txt", "b")
		revId2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		w.Write("c.txt", "c")
		revId3, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// List all revisions.
		logs, err := Log(
			t.Context(),
			r.View(""),
			&LogOptions{nil, nil, false, lib.RevisionRange{nil, nil}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId3, nil),
			revisionLog(t, r, revId2, nil),
			revisionLog(t, r, revId1, nil),
		}, newTestRevisionLogs(logs, false))

		// A range walks from Until back to (but excluding) Since.
		logs, err = Log(
			t.Context(),
			r.View(""),
			&LogOptions{nil, nil, false, lib.RevisionRange{Since: &revId1, Until: &revId3}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId3, nil),
			revisionLog(t, r, revId2, nil),
		}, newTestRevisionLogs(logs, false))

		// A nil Since walks from Until back to the root.
		logs, err = Log(
			t.Context(),
			r.View(""),
			&LogOptions{nil, nil, false, lib.RevisionRange{Since: nil, Until: &revId2}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
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
		revId1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Add second commit.
		w.Write("c/e.txt", "e")
		w.Rm("a.txt")
		w.Write("b.txt", "bb")
		revId2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// List all revisions.
		logs, err := Log(
			t.Context(),
			r.View(""),
			&LogOptions{nil, nil, true, lib.RevisionRange{nil, nil}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId2, []TestStatusFile{
				{"a.txt", lib.RevisionEntryKindDelete, 1},
				{"b.txt", lib.RevisionEntryKindUpdate, 2},
				{"c", lib.RevisionEntryKindUpdate, 0},
				{"c/e.txt", lib.RevisionEntryKindAdd, 1},
			}),
			revisionLog(t, r, revId1, []TestStatusFile{
				{"a.txt", lib.RevisionEntryKindAdd, 1},
				{"b.txt", lib.RevisionEntryKindAdd, 1},
				{"c", lib.RevisionEntryKindAdd, 0},
				{"c/d.txt", lib.RevisionEntryKindAdd, 1},
			}),
		}, newTestRevisionLogs(logs, true))
	})

	t.Run("Status lists a directory directly before its contents", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		w.Write("sub.txt", "s")
		w.Write("sub/a.txt", "a")
		revId, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		logs, err := Log(
			t.Context(),
			r.View(""),
			&LogOptions{nil, nil, true, lib.RevisionRange{nil, nil}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId, []TestStatusFile{
				{"sub.txt", lib.RevisionEntryKindAdd, 1},
				{"sub", lib.RevisionEntryKindAdd, 0},
				{"sub/a.txt", lib.RevisionEntryKindAdd, 1},
			}),
		}, newTestRevisionLogs(logs, true))
	})

	t.Run("Include and Exclude", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// Add three commits.
		w.Write("a.txt", "a")
		w.Write("b.txt", "b")
		w.Write("c/d.txt", "d")
		revId1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		w.Write("c/e.txt", "e")
		revId2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		w.Rm("a.txt")
		revId3, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// Include `a.txt` without status.
		filter := lib.NewPathInclusionFilter([]string{"a.txt"})
		logs, err := Log(
			t.Context(),
			r.View(""),
			&LogOptions{filter, nil, false, lib.RevisionRange{nil, nil}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId3, nil),
			revisionLog(t, r, revId1, nil),
		}, newTestRevisionLogs(logs, false))

		// Include `a.txt` with status.
		logs, err = Log(
			t.Context(),
			r.View(""),
			&LogOptions{filter, nil, true, lib.RevisionRange{nil, nil}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId3, []TestStatusFile{{"a.txt", lib.RevisionEntryKindDelete, 1}}),
			revisionLog(t, r, revId1, []TestStatusFile{{"a.txt", lib.RevisionEntryKindAdd, 1}}),
		}, newTestRevisionLogs(logs, true))

		// Include `c/*` with status.
		filter = lib.NewPathInclusionFilter([]string{"c/*"})
		logs, err = Log(
			t.Context(),
			r.View(""),
			&LogOptions{filter, nil, true, lib.RevisionRange{nil, nil}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId2, []TestStatusFile{{"c/e.txt", lib.RevisionEntryKindAdd, 1}}),
			revisionLog(t, r, revId1, []TestStatusFile{{"c/d.txt", lib.RevisionEntryKindAdd, 1}}),
		}, newTestRevisionLogs(logs, true))

		// Exclude drops paths, and a revision left with none of them.
		// `revId2` only added `c/e.txt`, so it disappears entirely.
		exclude := lib.NewPathExclusionFilter([]string{"c"})
		logs, err = Log(
			t.Context(),
			r.View(""),
			&LogOptions{nil, exclude, true, lib.RevisionRange{nil, nil}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId3, []TestStatusFile{{"a.txt", lib.RevisionEntryKindDelete, 1}}),
			revisionLog(t, r, revId1, []TestStatusFile{
				{"a.txt", lib.RevisionEntryKindAdd, 1},
				{"b.txt", lib.RevisionEntryKindAdd, 1},
			}),
		}, newTestRevisionLogs(logs, true))

		// Include and Exclude combine.
		filter = lib.NewPathInclusionFilter([]string{"**/*.txt"})
		exclude = lib.NewPathExclusionFilter([]string{"a.txt"})
		logs, err = Log(
			t.Context(),
			r.View(""),
			&LogOptions{filter, exclude, true, lib.RevisionRange{nil, nil}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId2, []TestStatusFile{{"c/e.txt", lib.RevisionEntryKindAdd, 1}}),
			revisionLog(t, r, revId1, []TestStatusFile{
				{"b.txt", lib.RevisionEntryKindAdd, 1},
				{"c/d.txt", lib.RevisionEntryKindAdd, 1},
			}),
		}, newTestRevisionLogs(logs, true))
	})

	t.Run("PathPrefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		// One commit inside `sub/`, one outside it.
		w.Write("sub/a.txt", "a")
		revId1, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)
		w.Write("b.txt", "b")
		revId2, err := Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
		assert.NoError(err)

		// A prefix reports paths relative to itself and drops the ones outside,
		// but it never hides a revision: `revId2` is still listed, with no
		// paths under it.
		logs, err := Log(t.Context(), r.View("sub"), &LogOptions{nil, nil, true, lib.RevisionRange{nil, nil}})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId2, []TestStatusFile{}),
			revisionLog(t, r, revId1, []TestStatusFile{{"a.txt", lib.RevisionEntryKindAdd, 1}}),
		}, newTestRevisionLogs(logs, true))
		// `TotalFiles` counts what was skipped, so a caller can report it.
		// `revId2` holds `b.txt`, `revId1` holds `sub/` and `sub/a.txt`.
		assert.Equal(1, logs[0].TotalFiles, "b.txt is outside the prefix but still counted")
		assert.Equal(2, logs[1].TotalFiles, "the `sub` directory entry itself is counted too")

		// A pattern is an explicit filter, so it does drop revisions. It
		// matches the prefix-relative path, so a leading `/` anchors at the
		// prefix and not at the repository root.
		filter := lib.NewPathInclusionFilter([]string{"/a.txt"})
		logs, err = Log(t.Context(), r.View("sub"), &LogOptions{filter, nil, false, lib.RevisionRange{nil, nil}})
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{revisionLog(t, r, revId1, nil)}, newTestRevisionLogs(logs, false))

		// Without a prefix both revisions are listed.
		logs, err = Log(
			t.Context(),
			r.View(""),
			&LogOptions{nil, nil, false, lib.RevisionRange{nil, nil}},
		)
		assert.NoError(err)
		assert.Equal([]TestRevisionLog{
			revisionLog(t, r, revId2, nil),
			revisionLog(t, r, revId1, nil),
		}, newTestRevisionLogs(logs, false))
	})
}

type TestRevisionLog struct {
	RevisionId lib.RevisionId
	Revision   lib.Revision
	Files      []TestStatusFile
}

type TestStatusFile struct {
	Path string
	Type lib.RevisionEntryKind
	Size int
}

func revisionLog(t *testing.T, r *lib.TestRepository, revId lib.RevisionId, files []TestStatusFile) TestRevisionLog {
	t.Helper()
	revision, err := r.ReadRevision(t.Context(), revId, lib.NewBlockBuf())
	lib.NewAssert(t).NoError(err)
	return TestRevisionLog{revId, revision, files}
}

func newTestRevisionLogs(logs []RevisionLog, status bool) []TestRevisionLog {
	testLogs := make([]TestRevisionLog, len(logs))
	for i, log := range logs {
		testLogs[i] = newTestRevisionLog(log, status)
	}
	return testLogs
}

func newTestRevisionLog(log RevisionLog, status bool) TestRevisionLog {
	var files []TestStatusFile
	if status {
		files = make([]TestStatusFile, len(log.Files))
		for i, file := range log.Files {
			files[i] = TestStatusFile{file.Path.String(), file.Kind, int(file.Metadata.Size)}
		}
	}
	return TestRevisionLog{log.RevisionId, log.Revision, files}
}
