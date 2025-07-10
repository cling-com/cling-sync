package lib

import (
	"errors"
	"io"
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
