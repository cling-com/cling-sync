package lib

import (
	"errors"
	"io"
	"io/fs"
	"testing"
)

func TestCommit(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		e1 := td.RevisionEntry("a/1.txt", RevisionEntryAdd)
		e2 := td.RevisionEntry("a/2.txt", RevisionEntryUpdate)
		e3 := td.RevisionEntry("a/3.txt", RevisionEntryDelete)
		// Add the entries unordered to test that they are sorted before commit.
		assert.NoError(commit.Add(e2))
		assert.NoError(commit.Add(e1))
		assert.NoError(commit.Add(e3))
		revisionId, err := commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
		assert.NoError(err)

		revision, entries, err := readRevision(r.Repository, revisionId)
		assert.NoError(err)
		assert.Equal(true, revision.Parent.IsRoot())
		assert.Equal("test author", revision.Author)
		assert.Equal("test message", revision.Message)
		assert.Equal([]*RevisionEntry{e1, e2, e3}, entries)

		// Add a second revision.
		commit2, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		e4 := td.RevisionEntry("a/1.txt", RevisionEntryDelete)
		assert.NoError(commit2.Add(e4))
		revisionId2, err := commit2.Commit(&CommitInfo{Author: "test author2", Message: "test message2"})
		assert.NoError(err)

		revision, entries, err = readRevision(r.Repository, revisionId2)
		assert.NoError(err)
		assert.Equal(revisionId, revision.Parent)
		assert.Equal("test author2", revision.Author)
		assert.Equal("test message2", revision.Message)
		assert.Equal([]*RevisionEntry{e4}, entries)
	})

	t.Run("Empty commit", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		_, err = commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
		assert.ErrorIs(err, ErrEmptyCommit)
	})

	t.Run("Head changed during commit", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		head := r.Head()

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)

		// Change the head.
		_, blockHeader, err := r.WriteBlock([]byte{1, 2, 3})
		assert.NoError(err)
		_, err = r.WriteRevision(&Revision{
			TimestampSec:  123456789,
			TimestampNSec: 1234,
			Message:       "test message",
			Author:        "test author",
			Parent:        head,
			Blocks:        []BlockId{blockHeader.BlockId},
		})
		assert.NoError(err)

		// Try to commit with the head changed.
		err = commit.Add(td.RevisionEntry("a/1.txt", RevisionEntryAdd))
		assert.NoError(err)
		_, err = commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
		assert.ErrorIs(err, ErrHeadChanged)
	})
}

func TestCommitEnsureDirExists(t *testing.T) {
	t.Parallel()

	run := func(r *TestRepository, assert Assert, dir Path) error {
		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)

		snapshot, err := NewRevisionSnapshot(r.Repository, r.Head(), td.NewFS(t))
		assert.NoError(err)
		snapshotCache, err := NewRevisionEntryTempCache(snapshot, 10)
		assert.NoError(err)

		err = commit.EnsureDirExists(dir, snapshotCache, r.Head())
		if err != nil {
			return err
		}
		_, err = commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
		return err
	}

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		err := run(r, assert, Path{"a/b/c"})
		assert.NoError(err)
		assert.Equal([]TestRevisionEntryInfo{
			{"a", RevisionEntryAdd, 0o700 | fs.ModeDir, Sha256{}},
			{"a/b", RevisionEntryAdd, 0o700 | fs.ModeDir, Sha256{}},
			{"a/b/c", RevisionEntryAdd, 0o700 | fs.ModeDir, Sha256{}},
		}, r.RevisionInfos(r.Head()))

		// Adding another directory level.
		err = run(r, assert, Path{"a/b/c/d"})
		assert.NoError(err)
		assert.Equal([]TestRevisionEntryInfo{
			{"a/b/c/d", RevisionEntryAdd, 0o700 | fs.ModeDir, Sha256{}},
		}, r.RevisionInfos(r.Head()))
	})

	t.Run("Nothing should happen if the directory already exists", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		err := run(r, assert, Path{"a/b/c"})
		assert.NoError(err)
		err = run(r, assert, Path{"a/b/c"})
		assert.ErrorIs(err, ErrEmptyCommit)
		err = run(r, assert, Path{"a/b"})
		assert.ErrorIs(err, ErrEmptyCommit)
		err = run(r, assert, Path{"a"})
		assert.ErrorIs(err, ErrEmptyCommit)
	})

	t.Run("Directories that are already in the current commit should not be created again", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)

		entry := td.RevisionEntryExt("a", RevisionEntryAdd, 0o700|ModeDir, "")
		entry.Metadata.FileHash = Sha256{}
		err = commit.Add(entry)
		assert.NoError(err)

		snapshot, err := NewRevisionSnapshot(r.Repository, r.Head(), td.NewFS(t))
		assert.NoError(err)
		snapshotCache, err := NewRevisionEntryTempCache(snapshot, 10)
		assert.NoError(err)

		err = commit.EnsureDirExists(Path{"a"}, snapshotCache, r.Head())
		assert.NoError(err)

		_, err = commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
		assert.NoError(err)
		assert.Equal([]TestRevisionEntryInfo{
			{"a", RevisionEntryAdd, 0o700 | fs.ModeDir, Sha256{}},
		}, r.RevisionInfos(r.Head()))
	})

	t.Run("Fail if a directory is actually a file", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)

		err = commit.Add(td.RevisionEntry("a/b", RevisionEntryAdd))
		assert.NoError(err)
		_, err = commit.Commit(td.CommitInfo())
		assert.NoError(err)

		// Try to ensure a directory at the position.
		err = run(r, assert, Path{"a/b/c"})
		assert.Error(err, "already exists and is not a directory")
	})

	t.Run("Empty path should do nothing", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		err := run(r, assert, Path{""})
		assert.ErrorIs(err, ErrEmptyCommit)
	})
}

func readRevision(repo *Repository, revisionId RevisionId) (*Revision, []*RevisionEntry, error) {
	revision, err := repo.ReadRevision(revisionId)
	if err != nil {
		return nil, nil, err
	}
	rr := NewRevisionReader(repo, &revision)
	entries := []*RevisionEntry{}
	for {
		entry, err := rr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		entries = append(entries, entry)
	}
	return &revision, entries, nil
}
