package lib

import (
	"errors"
	"io"
	"testing"
)

func TestRevisionSnapshot(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		root := r.Head()

		revId1, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/3.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/4.txt", RevisionEntryKindAdd),
		)
		assert.NoError(err)

		revId2, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("b/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("b/2.txt", RevisionEntryKindAdd),
			// Delete an entry.
			td.RevisionEntry("a/2.txt", RevisionEntryKindDelete),
			// Update an entry.
			td.RevisionEntry("a/3.txt", RevisionEntryKindUpdate),
			// Delete another entry to update it in the next revision.
			td.RevisionEntry("a/4.txt", RevisionEntryKindDelete),
		)
		assert.NoError(err)

		revId3, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("b/1.txt", RevisionEntryKindDelete),
			td.RevisionEntry("c/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/1.txt", RevisionEntryKindUpdate),
			// Re-add a deleted file.
			td.RevisionEntry("a/4.txt", RevisionEntryKindAdd),
		)
		assert.NoError(err)

		entries := readRevisionSnapshot(t, r.Repository, revId3, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryKindUpdate),
			td.RevisionEntry("a/3.txt", RevisionEntryKindUpdate),
			td.RevisionEntry("a/4.txt", RevisionEntryKindAdd),
			td.RevisionEntry("b/2.txt", RevisionEntryKindAdd),
			td.RevisionEntry("c/1.txt", RevisionEntryKindAdd),
		}, entries)

		entries = readRevisionSnapshot(t, r.Repository, revId2, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/3.txt", RevisionEntryKindUpdate),
			td.RevisionEntry("b/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("b/2.txt", RevisionEntryKindAdd),
		}, entries)

		entries = readRevisionSnapshot(t, r.Repository, revId1, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/3.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/4.txt", RevisionEntryKindAdd),
		}, entries)

		// Root revision should be empty.
		entries = readRevisionSnapshot(t, r.Repository, root, nil)
		assert.Equal([]*RevisionEntry{}, entries)
	})

	t.Run("Sort order is files, directories, and subdirectories", func(t *testing.T) {
		// This basically makes sure that we always use `RevisionEntryPathCompare`.
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		_, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntryExt("a", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntry("z.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/b/3.txt", RevisionEntryKindAdd),
		)
		assert.NoError(err)
		_, err = testCommit(
			t,
			r.Repository,
			td.RevisionEntryExt("a", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntryExt("a/b", RevisionEntryKindAdd, FileModeDir, ""),
		)
		assert.NoError(err)
		revId3, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("a.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryKindAdd),
		)
		assert.NoError(err)

		entries := readRevisionSnapshot(t, r.Repository, revId3, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a.txt", RevisionEntryKindAdd),
			td.RevisionEntry("z.txt", RevisionEntryKindAdd),
			td.RevisionEntryExt("a", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryKindAdd),
			td.RevisionEntryExt("a/b", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntry("a/b/3.txt", RevisionEntryKindAdd),
		}, entries)
	})

	t.Run("PathFilter", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		revId1, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/b/3.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/b/4.txt", RevisionEntryKindAdd),
		)
		assert.NoError(err)
		assert.NoError(err)
		filter := NewPathExclusionFilter([]string{"a/b"})
		snapshot := readRevisionSnapshot(t, r.Repository, revId1, filter)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryKindAdd),
		}, snapshot)
	})

	t.Run("Delete directory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		_, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/b/3.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/b/4.txt", RevisionEntryKindAdd),
		)
		assert.NoError(err)
		_, err = testCommit(
			t,
			r.Repository,
			td.RevisionEntry("a/b/3.txt", RevisionEntryKindUpdate),
			td.RevisionEntry("a/b/4.txt", RevisionEntryKindUpdate),
		)
		assert.NoError(err)
		revId2, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("a/b", RevisionEntryKindDelete),
			td.RevisionEntry("a/b/3.txt", RevisionEntryKindDelete),
			td.RevisionEntry("a/b/4.txt", RevisionEntryKindDelete),
		)
		assert.NoError(err)

		entries := readRevisionSnapshot(t, r.Repository, revId2, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryKindAdd),
		}, entries)
	})

	t.Run("Monitor should report every revision and every entry read", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		e1 := td.RevisionEntry("a/1.txt", RevisionEntryKindAdd)
		e2 := td.RevisionEntry("a/2.txt", RevisionEntryKindAdd)
		revId1, err := testCommit(t, r.Repository, e1, e2)
		assert.NoError(err)
		e3 := td.RevisionEntry("b/3.txt", RevisionEntryKindAdd)
		revId2, err := testCommit(t, r.Repository, e3)
		assert.NoError(err)

		monitor := td.NewRevisionSnapshotMonitor()
		snapshot, err := NewRevisionSnapshot(t.Context(), r.Repository, revId2, td.NewFS(t), monitor)
		assert.NoError(err)
		defer snapshot.Remove() //nolint:errcheck

		// The chain is walked head to root, then every entry of every revision
		// is read: `revId2` holds `b/3.txt`, `revId1` holds `a/1.txt` and `a/2.txt`.
		assert.Calls([]MockCall{
			NewMockCall("OnRevisionStart", revId2),
			NewMockCall("OnRevisionStart", revId1),
			NewMockCall("OnRevisionEntry", e3),
			NewMockCall("OnRevisionEntry", e1),
			NewMockCall("OnRevisionEntry", e2),
		}, monitor.Calls)
	})
}

func testCommit(t *testing.T, repo *Repository, entries ...*RevisionEntry) (RevisionId, error) {
	t.Helper()
	commit, err := NewCommit(t.Context(), repo, td.NewFS(t))
	if err != nil {
		return RevisionId{}, err
	}
	for _, entry := range entries {
		if err := commit.Add(entry); err != nil {
			return RevisionId{}, err
		}
	}
	return commit.Commit(t.Context(), &CommitInfo{Author: "test author", Message: "test message"})
}

func readRevisionSnapshot(
	t *testing.T,
	repo *Repository,
	revisionId RevisionId,
	pathFilter PathFilter,
) []*RevisionEntry {
	t.Helper()
	assert := NewAssert(t)
	snapshot, err := NewRevisionSnapshot(t.Context(), repo, revisionId, td.NewFS(t), td.NewRevisionSnapshotMonitor())
	assert.NoError(err)
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(RevisionEntryPathFilter(pathFilter))
	assert.NoError(err)
	entries := []*RevisionEntry{}
	buf := NewBlockBuf()
	for {
		entry, err := reader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(err)
		entries = append(entries, entry)
	}
	return entries
}
