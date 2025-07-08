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
		repo, _ := testRepository(t)
		root, err := repo.Head()
		assert.NoError(err)

		revId1, err := testCommit(
			t,
			repo,
			td.RevisionEntry("a/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryAdd),
			td.RevisionEntry("a/3.txt", RevisionEntryAdd),
			td.RevisionEntry("a/4.txt", RevisionEntryAdd),
		)
		assert.NoError(err)

		revId2, err := testCommit(
			t,
			repo,
			td.RevisionEntry("b/1.txt", RevisionEntryAdd),
			td.RevisionEntry("b/2.txt", RevisionEntryAdd),
			// Delete an entry.
			td.RevisionEntry("a/2.txt", RevisionEntryDelete),
			// Update an entry.
			td.RevisionEntry("a/3.txt", RevisionEntryUpdate),
			// Delete another entry to update it in the next revision.
			td.RevisionEntry("a/4.txt", RevisionEntryDelete),
		)
		assert.NoError(err)

		revId3, err := testCommit(
			t,
			repo,
			td.RevisionEntry("b/1.txt", RevisionEntryDelete),
			td.RevisionEntry("c/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/1.txt", RevisionEntryUpdate),
			// Re-add a deleted file.
			td.RevisionEntry("a/4.txt", RevisionEntryAdd),
		)
		assert.NoError(err)

		entries := readRevisionSnapshot(t, repo, revId3, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryUpdate),
			td.RevisionEntry("a/3.txt", RevisionEntryUpdate),
			td.RevisionEntry("a/4.txt", RevisionEntryAdd),
			td.RevisionEntry("b/2.txt", RevisionEntryAdd),
			td.RevisionEntry("c/1.txt", RevisionEntryAdd),
		}, entries)

		entries = readRevisionSnapshot(t, repo, revId2, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/3.txt", RevisionEntryUpdate),
			td.RevisionEntry("b/1.txt", RevisionEntryAdd),
			td.RevisionEntry("b/2.txt", RevisionEntryAdd),
		}, entries)

		entries = readRevisionSnapshot(t, repo, revId1, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryAdd),
			td.RevisionEntry("a/3.txt", RevisionEntryAdd),
			td.RevisionEntry("a/4.txt", RevisionEntryAdd),
		}, entries)

		// Root revision should be empty.
		entries = readRevisionSnapshot(t, repo, root, nil)
		assert.Equal([]*RevisionEntry{}, entries)
	})

	t.Run("Sort order is files, directories, and subdirectories", func(t *testing.T) {
		// This basically makes sure that we always use `RevisionEntryPathCompare`.
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		_, err := testCommit(
			t,
			repo,
			td.RevisionEntryExt("a", RevisionEntryAdd, ModeDir, ""),
			td.RevisionEntry("z.txt", RevisionEntryAdd),
			td.RevisionEntry("a/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/b/3.txt", RevisionEntryAdd),
		)
		assert.NoError(err)
		_, err = testCommit(
			t,
			repo,
			td.RevisionEntryExt("a", RevisionEntryAdd, ModeDir, ""),
			td.RevisionEntryExt("a/b", RevisionEntryAdd, ModeDir, ""),
		)
		assert.NoError(err)
		revId3, err := testCommit(
			t,
			repo,
			td.RevisionEntry("a.txt", RevisionEntryAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryAdd),
		)
		assert.NoError(err)

		entries := readRevisionSnapshot(t, repo, revId3, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a.txt", RevisionEntryAdd),
			td.RevisionEntry("z.txt", RevisionEntryAdd),
			td.RevisionEntryExt("a", RevisionEntryAdd, ModeDir, ""),
			td.RevisionEntry("a/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryAdd),
			td.RevisionEntryExt("a/b", RevisionEntryAdd, ModeDir, ""),
			td.RevisionEntry("a/b/3.txt", RevisionEntryAdd),
		}, entries)
	})

	t.Run("PathFilter", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		revId1, err := testCommit(
			t,
			repo,
			td.RevisionEntry("a/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryAdd),
			td.RevisionEntry("a/b/3.txt", RevisionEntryAdd),
			td.RevisionEntry("a/b/4.txt", RevisionEntryAdd),
		)
		assert.NoError(err)
		assert.NoError(err)
		filter, err := NewPathExclusionFilter([]string{"a/b"}, []string{})
		assert.NoError(err)
		snapshot := readRevisionSnapshot(t, repo, revId1, filter)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryAdd),
		}, snapshot)
	})

	t.Run("Delete directory", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		_, err := testCommit(
			t,
			repo,
			td.RevisionEntry("a/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryAdd),
			td.RevisionEntry("a/b/3.txt", RevisionEntryAdd),
			td.RevisionEntry("a/b/4.txt", RevisionEntryAdd),
		)
		assert.NoError(err)
		_, err = testCommit(
			t,
			repo,
			td.RevisionEntry("a/b/3.txt", RevisionEntryUpdate),
			td.RevisionEntry("a/b/4.txt", RevisionEntryUpdate),
		)
		assert.NoError(err)
		revId2, err := testCommit(
			t,
			repo,
			td.RevisionEntry("a/b", RevisionEntryDelete),
			td.RevisionEntry("a/b/3.txt", RevisionEntryDelete),
			td.RevisionEntry("a/b/4.txt", RevisionEntryDelete),
		)
		assert.NoError(err)

		entries := readRevisionSnapshot(t, repo, revId2, nil)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/1.txt", RevisionEntryAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryAdd),
		}, entries)
	})
}

func testCommit(t *testing.T, repo *Repository, entries ...*RevisionEntry) (RevisionId, error) {
	t.Helper()
	commit, err := NewCommit(repo, td.NewFS(t))
	if err != nil {
		return RevisionId{}, err
	}
	for _, entry := range entries {
		if err := commit.Add(entry); err != nil {
			return RevisionId{}, err
		}
	}
	return commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
}

func readRevisionSnapshot(
	t *testing.T,
	repo *Repository,
	revisionId RevisionId,
	pathFilter PathFilter,
) []*RevisionEntry {
	t.Helper()
	assert := NewAssert(t)
	snapshot, err := NewRevisionSnapshot(repo, revisionId, td.NewFS(t))
	assert.NoError(err)
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(pathFilter)
	assert.NoError(err)
	entries := []*RevisionEntry{}
	for {
		entry, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(err)
		entries = append(entries, entry)
	}
	return entries
}
