package lib

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
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

	t.Run("Sort order is the path alone", func(t *testing.T) {
		// This basically makes sure that we always use `RevisionEntry.PathCompare`.
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
			td.RevisionEntryExt("a", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntry("a.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/1.txt", RevisionEntryKindAdd),
			td.RevisionEntry("a/2.txt", RevisionEntryKindAdd),
			td.RevisionEntryExt("a/b", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntry("a/b/3.txt", RevisionEntryKindAdd),
			td.RevisionEntry("z.txt", RevisionEntryKindAdd),
		}, entries)
	})

	t.Run("Newest revision wins", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		_, err := testCommit(t, r.Repository, td.RevisionEntryExt("a.txt", RevisionEntryKindAdd, 0o600, "one"))
		assert.NoError(err)
		_, err = testCommit(t, r.Repository, td.RevisionEntryExt("a.txt", RevisionEntryKindUpdate, 0o600, "two"))
		assert.NoError(err)
		revId, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntryExt("a.txt", RevisionEntryKindUpdate, 0o600, "three"),
		)
		assert.NoError(err)

		assert.Equal([]*RevisionEntry{
			td.RevisionEntryExt("a.txt", RevisionEntryKindUpdate, 0o600, "three"),
		}, readRevisionSnapshot(t, r.Repository, revId, nil))
	})

	t.Run("Delete without an older entry", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		revId, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("a.txt", RevisionEntryKindDelete),
			td.RevisionEntry("b.txt", RevisionEntryKindAdd),
		)
		assert.NoError(err)

		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("b.txt", RevisionEntryKindAdd),
		}, readRevisionSnapshot(t, r.Repository, revId, nil))
	})

	t.Run("File replaced by a directory", func(t *testing.T) {
		// A file and a directory of the same path sort to different keys, so
		// both could survive the merge if the delete were missed.
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		_, err := testCommit(t, r.Repository, td.RevisionEntry("x", RevisionEntryKindAdd))
		assert.NoError(err)
		revId, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("x", RevisionEntryKindDelete),
			td.RevisionEntryExt("x", RevisionEntryKindAdd, FileModeDir, ""),
		)
		assert.NoError(err)

		assert.Equal([]*RevisionEntry{
			td.RevisionEntryExt("x", RevisionEntryKindAdd, FileModeDir, ""),
		}, readRevisionSnapshot(t, r.Repository, revId, nil))
	})

	t.Run("Directory replaced by a file", func(t *testing.T) {
		// The reverse ordering: the added file sorts before the deleted
		// directory, so the merge emits the winner before the delete that
		// clears the old entry.
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		_, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntryExt("x", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntry("x/inner.txt", RevisionEntryKindAdd),
		)
		assert.NoError(err)
		revId, err := testCommit(
			t,
			r.Repository,
			td.RevisionEntry("x", RevisionEntryKindAdd),
			td.RevisionEntryExt("x", RevisionEntryKindDelete, FileModeDir, ""),
			td.RevisionEntry("x/inner.txt", RevisionEntryKindDelete),
		)
		assert.NoError(err)

		assert.Equal([]*RevisionEntry{
			td.RevisionEntry("x", RevisionEntryKindAdd),
		}, readRevisionSnapshot(t, r.Repository, revId, nil))
	})

	t.Run("Revision spanning several blocks", func(t *testing.T) {
		// `Commit` splits entries into blocks by size, so build the blocks by
		// hand to reach the boundary with a handful of entries.
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		revId1 := r.AddRevision(r.Head(),
			[]*RevisionEntry{
				td.RevisionEntryExt("a.txt", RevisionEntryKindAdd, 0o600, "old"),
				td.RevisionEntry("b.txt", RevisionEntryKindAdd),
			},
			[]*RevisionEntry{
				td.RevisionEntryExt("c.txt", RevisionEntryKindAdd, 0o600, "old"),
				td.RevisionEntry("d.txt", RevisionEntryKindAdd),
			},
		)
		revId2 := r.AddRevision(revId1,
			[]*RevisionEntry{td.RevisionEntryExt("a.txt", RevisionEntryKindUpdate, 0o600, "new")},
			[]*RevisionEntry{td.RevisionEntryExt("c.txt", RevisionEntryKindUpdate, 0o600, "new")},
		)

		assert.Equal([]*RevisionEntry{
			td.RevisionEntryExt("a.txt", RevisionEntryKindUpdate, 0o600, "new"),
			td.RevisionEntry("b.txt", RevisionEntryKindAdd),
			td.RevisionEntryExt("c.txt", RevisionEntryKindUpdate, 0o600, "new"),
			td.RevisionEntry("d.txt", RevisionEntryKindAdd),
		}, readRevisionSnapshot(t, r.Repository, revId2, nil))
	})

	t.Run("Many overlapping revisions", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		const revs, paths = 25, 40
		var revId RevisionId
		want := map[string]*RevisionEntry{}
		// Revision `v` touches every path divisible by `v+1`, so later
		// revisions overwrite `want`.
		for v := range revs {
			entries := []*RevisionEntry{}
			for p := range paths {
				if p%(v+1) != 0 {
					continue
				}
				path := fmt.Sprintf("d%02d/f%02d.txt", p%5, p)
				entry := td.RevisionEntryExt(path, RevisionEntryKindUpdate, 0o600, strings.Repeat("x", v+1))
				entries = append(entries, entry)
				want[path] = entry
			}
			var err error
			revId, err = testCommit(t, r.Repository, entries...)
			assert.NoError(err)
		}
		expected := slices.Collect(maps.Values(want))
		slices.SortFunc(expected, (*RevisionEntry).PathCompare)

		assert.Equal(expected, readRevisionSnapshot(t, r.Repository, revId, nil))
	})

	t.Run("Entries of two revisions come out interleaved", func(t *testing.T) {
		// Nothing re-sorts the merged result, so the interleaving has to be
		// right as it leaves the merge.
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		older := r.AddRevision(r.Head(), []*RevisionEntry{
			td.RevisionEntry("a.txt", RevisionEntryKindAdd),
			td.RevisionEntry("m.txt", RevisionEntryKindAdd),
			td.RevisionEntry("z.txt", RevisionEntryKindAdd),
		})
		newer := r.AddRevision(older, []*RevisionEntry{
			td.RevisionEntry("b.txt", RevisionEntryKindAdd),
			td.RevisionEntry("m.txt", RevisionEntryKindUpdate),
		})
		got := readRevisionSnapshot(t, r.Repository, newer, nil)
		paths := make([]string, len(got))
		for i, e := range got {
			paths[i] = e.Path.String()
		}
		assert.Equal([]string{"a.txt", "b.txt", "m.txt", "z.txt"}, paths)
	})

	t.Run("Unsorted revision is rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		revId := r.AddRevision(r.Head(),
			[]*RevisionEntry{td.RevisionEntry("b.txt", RevisionEntryKindAdd)},
			[]*RevisionEntry{td.RevisionEntry("a.txt", RevisionEntryKindAdd)},
		)
		_, err := NewRevisionSnapshot(t.Context(), r.Repository, revId, td.NewFS(t), td.NewRevisionSnapshotMonitor())
		assert.Error(err, "not strictly sorted")
		assert.Error(err, "b.txt >= a.txt")
	})

	t.Run("Revision holding a path twice is rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		revId := r.AddRevision(r.Head(),
			[]*RevisionEntry{td.RevisionEntry("a.txt", RevisionEntryKindAdd)},
			[]*RevisionEntry{td.RevisionEntry("a.txt", RevisionEntryKindUpdate)},
		)
		_, err := NewRevisionSnapshot(t.Context(), r.Repository, revId, td.NewFS(t), td.NewRevisionSnapshotMonitor())
		assert.Error(err, "a.txt >= a.txt")
	})

	t.Run("A directory before a file of the same path is rejected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		revId := r.AddRevision(r.Head(),
			[]*RevisionEntry{td.RevisionEntryExt("a", RevisionEntryKindAdd, FileModeDir, "")},
			[]*RevisionEntry{td.RevisionEntry("a", RevisionEntryKindAdd)},
		)
		_, err := NewRevisionSnapshot(t.Context(), r.Repository, revId, td.NewFS(t), td.NewRevisionSnapshotMonitor())
		assert.Error(err, "a/ >= a")
	})

	t.Run("File and directory of the same path is rejected", func(t *testing.T) {
		// A newer revision turns `a/b` into a directory without deleting the
		// file first, so both would end up in the snapshot. `Commit` never
		// writes that: `TestMerge/File replaced by a directory` pins that a type
		// change is a delete plus an add, and the delete cancels the old entry.
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		older := r.AddRevision(r.Head(), []*RevisionEntry{td.RevisionEntry("a/b", RevisionEntryKindAdd)})
		newer := r.AddRevision(older, []*RevisionEntry{
			td.RevisionEntryExt("a/b", RevisionEntryKindAdd, FileModeDir, ""),
		})

		_, err := NewRevisionSnapshot(t.Context(), r.Repository, newer, td.NewFS(t), td.NewRevisionSnapshotMonitor())
		assert.Error(err, "a/b is both a file and a directory")
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

	t.Run("Monitor", func(t *testing.T) {
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

// Building a snapshot is a k-way merge over the whole revision chain, so cost
// grows with the length of the chain as well as with the number of paths.
func BenchmarkRevisionSnapshot(b *testing.B) {
	const paths, churn = 20000, 200
	assert := NewAssert(b)
	for _, revs := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(revs)+"Revisions", func(b *testing.B) {
			r := td.NewTestRepository(b, td.NewFS(b))
			path := func(i int) string { return fmt.Sprintf("d%03d/f%06d.txt", i%100, i) }
			entries := make([]*RevisionEntry, 0, paths)
			for i := range paths {
				entries = append(entries, td.RevisionEntry(path(i), RevisionEntryKindAdd))
			}
			revId := r.AddRevision(r.Head(), entries)
			for v := 1; v < revs; v++ {
				changed := make([]*RevisionEntry, 0, churn)
				for i := range churn {
					changed = append(changed, td.RevisionEntry(path((v*churn+i)%paths), RevisionEntryKindUpdate))
				}
				revId = r.AddRevision(revId, changed)
			}
			mon := benchRevisionSnapshotMonitor{}
			b.ResetTimer()
			for b.Loop() {
				tmpFS := NewMemoryFS(256 * 1024 * 1024)
				_, err := NewRevisionSnapshot(b.Context(), r.Repository, revId, tmpFS, mon)
				assert.NoError(err)
			}
		})
	}
}

// `TestRevisionSnapshotMonitor` records every call, which would dominate the
// benchmark's allocations.
type benchRevisionSnapshotMonitor struct{}

func (benchRevisionSnapshotMonitor) OnRevisionStart(RevisionId)     {}
func (benchRevisionSnapshotMonitor) OnRevisionEntry(*RevisionEntry) {}

// Build revisions from `data` and check the snapshot against a reference built
// with a map. Every input gets its own repository, because `WriteRevision`
// requires the parent to be the current head, so a shared one would carry the
// previous input's revisions into this input's snapshot.
func FuzzRevisionSnapshot(f *testing.F) {
	f.Add([]byte{0, 1, 2, 0, 3, 0})
	f.Add([]byte{1, 1, 1, 2, 2, 2, 0, 0})
	f.Add([]byte{7, 3, 9, 0, 4, 4})
	paths := []string{"a", "a/b", "a/b/c", "ab", "b", "b/c", "z.txt", "a/z.txt"}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 || len(data) > 64 {
			return
		}
		assert := NewAssert(t)
		r := td.NewTestRepository(t, NewMemoryFS(64*1024*1024))
		revisions := [][]*RevisionEntry{{}}
		add := func(entry *RevisionEntry) {
			last := revisions[len(revisions)-1]
			// A revision holds each key once, and starts a new one otherwise.
			if slices.ContainsFunc(last, func(e *RevisionEntry) bool {
				return e.PathCompare(entry) == 0
			}) {
				revisions = append(revisions, []*RevisionEntry{entry})
				return
			}
			revisions[len(revisions)-1] = append(last, entry)
		}
		mode := func(isDir bool) FileMode {
			if isDir {
				return FileModeDir
			}
			return FileMode(0o600)
		}
		// What each path currently is, so that a type change can be written the
		// way `Commit` writes it.
		type state struct{ isDir, exists bool }
		current := map[string]state{}
		// Each byte is one entry: which path, which kind, and which type.
		for _, b := range data {
			path := paths[int(b>>3)%len(paths)]
			kind := []RevisionEntryKind{
				RevisionEntryKindAdd, RevisionEntryKindUpdate, RevisionEntryKindDelete,
			}[int(b>>1)%3]
			isDir := b&1 == 1
			was := current[path]
			switch {
			case kind == RevisionEntryKindDelete && was.exists:
				// Only what is there can be deleted.
				isDir = was.isDir
			case was.exists && was.isDir != isDir:
				// `Commit` turns a type change into a delete of the old entry
				// plus an add of the new one. Without the delete both would
				// reach the snapshot, which is not a state a filesystem has.
				add(td.RevisionEntryExt(path, RevisionEntryKindDelete, mode(was.isDir), path))
			}
			add(td.RevisionEntryExt(path, kind, mode(isDir), path))
			current[path] = state{isDir: isDir, exists: kind != RevisionEntryKindDelete}
		}
		revId := r.Head()
		for _, entries := range revisions {
			if len(entries) == 0 {
				continue
			}
			revId = r.AddRevision(revId, entries)
		}

		// Reference: walk oldest to newest, newest write of a key wins.
		want := map[PathKey]*RevisionEntry{}
		for _, entries := range revisions {
			for _, entry := range entries {
				want[entry.PathKey()] = entry
			}
		}
		expected := []*RevisionEntry{}
		for _, entry := range want {
			if entry.Kind != RevisionEntryKindDelete {
				expected = append(expected, entry)
			}
		}
		slices.SortFunc(expected, (*RevisionEntry).PathCompare)

		got := readRevisionSnapshot(t, r.Repository, revId, nil)
		assert.Equal(expected, got)
		// The snapshot must be strictly ordered, so no key may repeat.
		for i := 1; i < len(got); i++ {
			assert.Equal(true, got[i-1].PathCompare(got[i]) < 0, "at %d", i)
		}
	})
}
