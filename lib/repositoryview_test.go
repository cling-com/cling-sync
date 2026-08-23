package lib

import (
	"errors"
	"io"
	"testing"
)

func TestRepositoryViewSnapshot(t *testing.T) {
	t.Parallel()

	// `a/b` is the prefix. Everything else is an ancestor, a sibling, a
	// near-miss, or unrelated.
	arrange := func(t *testing.T) (*TestRepository, RevisionId) {
		t.Helper()
		r := td.NewTestRepository(t, td.NewFS(t))
		revId, err := testCommit(
			t,
			r.Repository,
			viewDir("a"),
			viewDir("a/b"),
			td.RevisionEntry("a/b-sibling.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/b/1.txt", RevisionEntryKindAdd),
			viewDir("a/b/c"),
			td.RevisionEntry("a/b/c/2.txt", RevisionEntryKindAdd),
			viewSymlink("a/b/inside", "a/b/c/2.txt"),
			viewSymlink("a/b/outside", "a/b-sibling.txt"),
			td.RevisionEntry("a/bb/3.txt", RevisionEntryKindAdd),
			td.RevisionEntry("z.txt", RevisionEntryKindAdd),
		)
		NewAssert(t).NoError(err)
		return r, revId
	}

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r, revId := arrange(t)
		snapshot := viewSnapshot(t, r, "a/b", revId)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("1.txt", RevisionEntryKindAdd),
			viewDir("c"),
			td.RevisionEntry("c/2.txt", RevisionEntryKindAdd),
			viewSymlink("inside", "c/2.txt"),
		}, readAllEntries(t, snapshot.Reader(nil).Read))
	})

	t.Run("Symlinks pointing outside the prefix are recorded", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r, revId := arrange(t)
		snapshot := viewSnapshot(t, r, "a/b", revId)
		assert.Equal([]*RevisionEntry{
			viewSymlink("a/b/outside", "a/b-sibling.txt"),
		}, readAllEntries(t, snapshot.hiddenLinks.Source.Reader(nil).Read))
	})

	t.Run("The prefix and its parents are recorded", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r, revId := arrange(t)
		snapshot := viewSnapshot(t, r, "a/b", revId)
		assert.Equal([]*RevisionEntry{viewDir("a"), viewDir("a/b")}, snapshot.prefixChain)
	})

	t.Run("An empty prefix hides nothing", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r, revId := arrange(t)
		snapshot := viewSnapshot(t, r, "", revId)
		assert.Equal(readRevisionSnapshot(t, r.Repository, revId, nil), readAllEntries(t, snapshot.Reader(nil).Read))
		assert.Equal(0, len(snapshot.prefixChain))
		assert.Equal(0, snapshot.hiddenLinks.Source.Chunks())
	})
}

func TestRepositoryViewRevisionReader(t *testing.T) {
	t.Parallel()
	assert := NewAssert(t)
	r := td.NewTestRepository(t, td.NewFS(t))
	_, err := testCommit(
		t,
		r.Repository,
		viewDir("a"),
		viewDir("a/b"),
		td.RevisionEntry("a/b/1.txt", RevisionEntryKindAdd),
		td.RevisionEntry("a/b/2.txt", RevisionEntryKindAdd),
	)
	assert.NoError(err)
	revId, err := testCommit(
		t,
		r.Repository,
		td.RevisionEntry("a/b/1.txt", RevisionEntryKindDelete),
		td.RevisionEntry("a/b/2.txt", RevisionEntryKindUpdate),
		td.RevisionEntry("a/b/3.txt", RevisionEntryKindAdd),
		viewSymlink("a/b/outside", "a/other"),
		viewDir("a/other"),
	)
	assert.NoError(err)
	revision, err := r.ReadRevision(t.Context(), revId, NewBlockBuf())
	assert.NoError(err)

	reader := r.View("a/b").NewRevisionReader(&revision)
	assert.Equal([]*RevisionEntry{
		td.RevisionEntry("1.txt", RevisionEntryKindDelete),
		td.RevisionEntry("2.txt", RevisionEntryKindUpdate),
		td.RevisionEntry("3.txt", RevisionEntryKindAdd),
	}, readAllEntries(t, func(buf BlockBuf) (*RevisionEntry, error) {
		return reader.Read(t.Context(), buf)
	}))
	assert.Equal(2, reader.Hidden(), "the outside symlink and a/other")
}

func TestRepositoryViewCommit(t *testing.T) {
	t.Parallel()

	commit := func(t *testing.T, r *TestRepository, prefix string, entries ...*RevisionEntry) RevisionId {
		t.Helper()
		assert := NewAssert(t)
		commit, err := r.View(prefix).NewCommit(t.Context(), td.NewFS(t), viewSnapshot(t, r, prefix, r.Head()))
		assert.NoError(err)
		for _, e := range entries {
			assert.NoError(commit.Add(e))
		}
		revId, err := commit.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)
		return revId
	}

	t.Run("Paths and symlink targets get the prefix", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		_, err := testCommit(t, r.Repository, viewDir("a"), viewDir("a/b"))
		assert.NoError(err)

		revId := commit(t, r, "a/b",
			td.RevisionEntry("1.txt", RevisionEntryKindAdd),
			viewSymlink("link", "1.txt"),
		)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/b/1.txt", RevisionEntryKindAdd),
			viewSymlink("a/b/link", "a/b/1.txt"),
		}, revisionEntries(t, r, revId))
	})

	t.Run("Missing prefix directories are created", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		_, err := testCommit(t, r.Repository, viewDir("a"))
		assert.NoError(err)

		revId := commit(t, r, "a/b/c", td.RevisionEntry("1.txt", RevisionEntryKindAdd))
		entries := revisionEntries(t, r, revId)
		assert.Equal(3, len(entries))
		assert.Equal(td.Path("a/b"), entries[0].Path)
		assert.Equal(true, entries[0].Metadata.FileMode.IsDir())
		assert.Equal(td.Path("a/b/c"), entries[1].Path)
		assert.Equal(true, entries[1].Metadata.FileMode.IsDir())
		assert.Equal(td.Path("a/b/c/1.txt"), entries[2].Path)
	})

	t.Run("A file replacing a hidden symlink is added as an update", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		_, err := testCommit(t, r.Repository,
			viewDir("a"),
			viewDir("a/b"),
			viewSymlink("a/b/added", "a/outside.txt"),
			viewSymlink("a/b/updated", "a/outside.txt"),
		)
		assert.NoError(err)

		revId := commit(t, r, "a/b",
			td.RevisionEntry("added", RevisionEntryKindAdd),
			td.RevisionEntry("updated", RevisionEntryKindUpdate),
		)
		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("a/b/added", RevisionEntryKindAdd),
			td.RevisionEntry("a/b/updated", RevisionEntryKindUpdate),
		}, revisionEntries(t, r, revId))
		assert.Equal([]*RevisionEntry{
			viewDir("a"),
			viewDir("a/b"),
			td.RevisionEntry("a/b/added", RevisionEntryKindAdd),
			td.RevisionEntry("a/b/updated", RevisionEntryKindUpdate),
		}, readRevisionSnapshot(t, r.Repository, revId, nil))
	})

	t.Run("A directory replacing a hidden symlink is added as a delete and add", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		_, err := testCommit(t, r.Repository,
			viewDir("a"),
			viewDir("a/b"),
			viewSymlink("a/b/added", "a/outside.txt"),
			viewSymlink("a/b/updated", "a/outside.txt"),
		)
		assert.NoError(err)

		updated := td.RevisionEntryExt("updated", RevisionEntryKindUpdate, 0o700|FileModeDir, "")
		revId := commit(t, r, "a/b", viewDir("added"), updated)
		updatedInRepo := td.RevisionEntryExt("a/b/updated", RevisionEntryKindUpdate, 0o700|FileModeDir, "")
		assert.Equal([]*RevisionEntry{
			asDelete(viewSymlink("a/b/added", "a/outside.txt")),
			viewDir("a/b/added"),
			asDelete(viewSymlink("a/b/updated", "a/outside.txt")),
			updatedInRepo,
		}, revisionEntries(t, r, revId))
		// Without the deletes the repository would hold a file and a
		// directory at one path, and this would fail.
		assert.Equal([]*RevisionEntry{
			viewDir("a"),
			viewDir("a/b"),
			viewDir("a/b/added"),
			updatedInRepo,
		}, readRevisionSnapshot(t, r.Repository, revId, nil))
	})

	t.Run("A prefix that is a file should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		_, err := testCommit(t, r.Repository, td.RevisionEntry("a", RevisionEntryKindAdd))
		assert.NoError(err)

		commit, err := r.View("a/b").NewCommit(t.Context(), td.NewFS(t), viewSnapshot(t, r, "a/b", r.Head()))
		assert.NoError(err)
		assert.NoError(commit.Add(td.RevisionEntry("1.txt", RevisionEntryKindAdd)))
		_, err = commit.Commit(t.Context(), td.CommitInfo())
		assert.Error(err, "already exists and is not a directory")
	})

	t.Run("A base that is not the head should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		old, err := testCommit(t, r.Repository, viewDir("a"))
		assert.NoError(err)
		_, err = testCommit(t, r.Repository, td.RevisionEntry("b", RevisionEntryKindAdd))
		assert.NoError(err)

		_, err = r.View("a").NewCommit(t.Context(), td.NewFS(t), viewSnapshot(t, r, "a", old))
		assert.ErrorIs(err, ErrHeadChanged)
	})
}

func viewDir(path string) *RevisionEntry {
	return td.RevisionEntryExt(path, RevisionEntryKindAdd, 0o700|FileModeDir, "")
}

func viewSymlink(path string, target string) *RevisionEntry {
	e := td.RevisionEntryExt(path, RevisionEntryKindAdd, FileModeSymlink, "")
	t := td.Path(target)
	e.Metadata.SymLinkTarget = &t
	return e
}

func asDelete(e *RevisionEntry) *RevisionEntry {
	deleted := *e
	deleted.Kind = RevisionEntryKindDelete
	return &deleted
}

func viewSnapshot(t *testing.T, r *TestRepository, prefix string, revId RevisionId) *ViewSnapshot {
	t.Helper()
	snapshot, err := r.View(prefix).NewSnapshot(t.Context(), revId, td.NewFS(t), td.NewRevisionSnapshotMonitor())
	NewAssert(t).NoError(err)
	t.Cleanup(func() { _ = snapshot.Remove() })
	return snapshot
}

func revisionEntries(t *testing.T, r *TestRepository, revId RevisionId) []*RevisionEntry {
	t.Helper()
	revision, err := r.ReadRevision(t.Context(), revId, NewBlockBuf())
	NewAssert(t).NoError(err)
	reader := NewRevisionReader(r.Repository, &revision)
	return readAllEntries(t, func(buf BlockBuf) (*RevisionEntry, error) {
		return reader.Read(t.Context(), buf)
	})
}

func readAllEntries(t *testing.T, read func(BlockBuf) (*RevisionEntry, error)) []*RevisionEntry {
	t.Helper()
	assert := NewAssert(t)
	entries := []*RevisionEntry{}
	buf := NewBlockBuf()
	for {
		entry, err := read(buf)
		if errors.Is(err, io.EOF) {
			return entries
		}
		assert.NoError(err)
		entries = append(entries, entry)
	}
}
