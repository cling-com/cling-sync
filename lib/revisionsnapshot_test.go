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
			fakeRevisionEntry("a/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/2.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/3.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/4.txt", RevisionEntryAdd),
		)
		assert.NoError(err)

		revId2, err := testCommit(
			t,
			repo,
			fakeRevisionEntry("b/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("b/2.txt", RevisionEntryAdd),
			// Delete an entry.
			fakeRevisionEntry("a/2.txt", RevisionEntryDelete),
			// Update an entry.
			fakeRevisionEntry("a/3.txt", RevisionEntryUpdate),
			// Delete another entry to update it in the next revision.
			fakeRevisionEntry("a/4.txt", RevisionEntryDelete),
		)
		assert.NoError(err)

		revId3, err := testCommit(
			t,
			repo,
			fakeRevisionEntry("b/1.txt", RevisionEntryDelete),
			fakeRevisionEntry("c/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/1.txt", RevisionEntryUpdate),
			// Re-add a deleted file.
			fakeRevisionEntry("a/4.txt", RevisionEntryAdd),
		)
		assert.NoError(err)

		entries := readRevisionSnapshot(t, repo, revId3, nil)
		assert.Equal([]*RevisionEntry{
			fakeRevisionEntry("a/1.txt", RevisionEntryUpdate),
			fakeRevisionEntry("a/3.txt", RevisionEntryUpdate),
			fakeRevisionEntry("a/4.txt", RevisionEntryAdd),
			fakeRevisionEntry("b/2.txt", RevisionEntryAdd),
			fakeRevisionEntry("c/1.txt", RevisionEntryAdd),
		}, entries)

		entries = readRevisionSnapshot(t, repo, revId2, nil)
		assert.Equal([]*RevisionEntry{
			fakeRevisionEntry("a/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/3.txt", RevisionEntryUpdate),
			fakeRevisionEntry("b/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("b/2.txt", RevisionEntryAdd),
		}, entries)

		entries = readRevisionSnapshot(t, repo, revId1, nil)
		assert.Equal([]*RevisionEntry{
			fakeRevisionEntry("a/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/2.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/3.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/4.txt", RevisionEntryAdd),
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
			fakeRevisionEntryMode("a", RevisionEntryAdd, ModeDir),
			fakeRevisionEntry("z.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/b/3.txt", RevisionEntryAdd),
		)
		assert.NoError(err)
		_, err = testCommit(
			t,
			repo,
			fakeRevisionEntryMode("a", RevisionEntryAdd, ModeDir),
			fakeRevisionEntryMode("a/b", RevisionEntryAdd, ModeDir),
		)
		assert.NoError(err)
		revId3, err := testCommit(
			t,
			repo,
			fakeRevisionEntry("a.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/2.txt", RevisionEntryAdd),
		)
		assert.NoError(err)

		entries := readRevisionSnapshot(t, repo, revId3, nil)
		assert.Equal([]*RevisionEntry{
			fakeRevisionEntry("a.txt", RevisionEntryAdd),
			fakeRevisionEntry("z.txt", RevisionEntryAdd),
			fakeRevisionEntryMode("a", RevisionEntryAdd, ModeDir),
			fakeRevisionEntry("a/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/2.txt", RevisionEntryAdd),
			fakeRevisionEntryMode("a/b", RevisionEntryAdd, ModeDir),
			fakeRevisionEntry("a/b/3.txt", RevisionEntryAdd),
		}, entries)
	})

	t.Run("Ignored paths", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		revId1, err := testCommit(
			t,
			repo,
			fakeRevisionEntry("a/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/2.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/b/3.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/b/4.txt", RevisionEntryAdd),
		)
		assert.NoError(err)
		assert.NoError(err)
		filter, err := NewPathExclusionFilter([]string{"a/b"}, []string{})
		assert.NoError(err)
		snapshot := readRevisionSnapshot(t, repo, revId1, filter)
		assert.Equal([]*RevisionEntry{
			fakeRevisionEntry("a/1.txt", RevisionEntryAdd),
			fakeRevisionEntry("a/2.txt", RevisionEntryAdd),
		}, snapshot)
	})
}

func testCommit(t *testing.T, repo *Repository, entries ...*RevisionEntry) (RevisionId, error) {
	t.Helper()
	commit, err := NewCommit(repo, t.TempDir())
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
	snapshot, err := NewRevisionSnapshot(repo, revisionId, t.TempDir())
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
