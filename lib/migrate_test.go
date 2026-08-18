package lib

import (
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"
)

func TestFixRewriteRevisions(t *testing.T) {
	t.Parallel()

	// Write a revision whose entries are in the order used before the sort
	// order changed, which is what a repository in the wild looks like.
	staleRevision := func(t *testing.T, r *TestRepository, parent RevisionId, entries ...*RevisionEntry) RevisionId {
		t.Helper()
		assert := NewAssert(t)
		chunk := RevisionEntryChunk{Entries: entries}
		buf := make([]byte, chunk.MarshallSize())
		w := NewProtobufWriter(buf)
		assert.NoError(chunk.Marshall(w))
		blockId, _, err := r.WriteBlock(t.Context(), w.Bytes(), NewBlockBuf())
		assert.NoError(err)
		id, err := r.WriteRevision(t.Context(), &Revision{ //nolint:exhaustruct
			Timestamp:        NewTimestampNow(),
			ParentRevisionId: parent,
			BlockIds:         []BlockId{blockId},
		})
		assert.NoError(err)
		return id
	}

	t.Run("Rewrites every revision and keeps the chain", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// Files before subdirectories, which no longer sorts.
		rev1 := staleRevision(t, r, RevisionId{},
			td.RevisionEntry("a/z.txt", RevisionEntryKindAdd),
			td.RevisionEntryExt("a/b", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntry("a/b/c.txt", RevisionEntryKindAdd),
		)
		rev2 := staleRevision(t, r, rev1,
			td.RevisionEntry("q.txt", RevisionEntryKindAdd),
			td.RevisionEntryExt("p", RevisionEntryKindAdd, FileModeDir, ""),
			td.RevisionEntry("p/r.txt", RevisionEntryKindAdd),
		)
		assert.Equal(rev2, r.Head())

		// Unreadable as it stands.
		_, err := NewRevisionSnapshot(t.Context(), r.Repository, rev2, td.NewFS(t), td.NewRevisionSnapshotMonitor())
		assert.Error(err, "not strictly sorted")

		assert.NoError(
			RewriteRevisions(t.Context(), r.Repository, td.NewFS(t), td.NewHealthCheckMonitor(), DefaultTempChunkSize),
		)

		// The head moved, because every id is the hash of its revision.
		newHead := r.Head()
		assert.Equal(true, newHead != rev2, "the head should have been rewritten")

		// The chain still has both revisions, oldest last.
		chain, err := ReadRevisionChain(t.Context(), r.Repository)
		assert.NoError(err)
		assert.Equal(2, len(chain))
		assert.Equal(newHead, chain[0])

		// And the whole thing reads and checks out now.
		assert.Equal([]TestRevisionEntryInfo{
			{"p", RevisionEntryKindAdd, fs.ModeDir, CalculateSha256(nil)},
			{"p/r.txt", RevisionEntryKindAdd, 0o600, td.SHA256("test")},
			{"q.txt", RevisionEntryKindAdd, 0o600, td.SHA256("test")},
		}, r.RevisionInfos(newHead))
		assert.Equal([]TestRevisionEntryInfo{
			{"a/b", RevisionEntryKindAdd, fs.ModeDir, CalculateSha256(nil)},
			{"a/b/c.txt", RevisionEntryKindAdd, 0o600, td.SHA256("test")},
			{"a/z.txt", RevisionEntryKindAdd, 0o600, td.SHA256("test")},
		}, r.RevisionInfos(chain[1]))
		assert.NoError(CheckHealth(t.Context(), r.Repository, td.NewFS(t), HealthCheckOptions{
			Monitor:             td.NewHealthCheckMonitor(),
			CheckBlocks:         false,
			CheckOrphanedBlocks: false,
		}))
	})

	t.Run("An empty repository is left alone", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		assert.NoError(
			RewriteRevisions(t.Context(), r.Repository, td.NewFS(t), td.NewHealthCheckMonitor(), DefaultTempChunkSize),
		)
		assert.Equal(true, r.Head().IsRoot())
	})
}

// Read a revision's entries straight from its blocks, which is the only way to
// see one that is not in the current sort order.
func rawRevisionEntries(t *testing.T, r *TestRepository, id RevisionId) []TestRevisionEntryInfo {
	t.Helper()
	assert := NewAssert(t)
	buf := NewBlockBuf()
	revision, err := r.ReadRevision(t.Context(), id, buf)
	assert.NoError(err)
	out := []TestRevisionEntryInfo{}
	for _, blockId := range revision.BlockIds {
		data, err := r.ReadBlock(t.Context(), blockId, buf)
		assert.NoError(err)
		chunk, err := UnmarshallRevisionEntryChunk(NewProtobufReader(data))
		assert.NoError(err)
		for _, e := range chunk.Entries {
			out = append(out, TestRevisionEntryInfo{
				e.Path.String(), e.Kind, e.Metadata.FileMode.AsFsFileMode(), e.Metadata.FileHash,
			})
		}
	}
	// Compare as a set, because the order is exactly what changes.
	slices.SortFunc(out, func(a, b TestRevisionEntryInfo) int { return strings.Compare(a.Path, b.Path) })
	return out
}

func TestFixRewriteRevisionsPreservesEverything(t *testing.T) {
	t.Parallel()
	// Shapes that stress a different part of the rewrite.
	for _, shape := range []struct {
		name      string
		revisions [][]string
	}{
		{"one revision", [][]string{{"a/z.txt", "a/b", "a/b/c.txt"}}},
		{"several revisions", [][]string{
			{"a/z.txt", "a/b", "a/b/c.txt"},
			{"q.txt", "p", "p/r.txt"},
			{"z", "y/x.txt", "y"},
		}},
		{"single entry", [][]string{{"only.txt"}}},
		{"already in the new order", [][]string{{"a", "a/b", "a/b/c"}}},
		{"names around the separator", [][]string{{"a.txt", "a", "a/b.", "a/b", "a/b/c", "a-1", "a0"}}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			assert := NewAssert(t)
			r := td.NewTestRepository(t, td.NewFS(t))

			// Write the revisions exactly as given, without sorting, which is
			// what a repository from an older version looks like.
			before := [][]TestRevisionEntryInfo{}
			meta := []Revision{}
			parent := RevisionId{}
			for _, paths := range shape.revisions {
				entries := make([]*RevisionEntry, 0, len(paths))
				for _, path := range paths {
					mode := FileMode(0o600)
					if !strings.Contains(path, ".") {
						mode = FileModeDir
					}
					entries = append(entries, td.RevisionEntryExt(path, RevisionEntryKindAdd, mode, path))
				}
				// One entry per block, so a revision spans as many blocks as it
				// has entries and the read loop cannot stop at the first.
				blockIds := make([]BlockId, 0, len(entries))
				for _, entry := range entries {
					chunk := RevisionEntryChunk{Entries: []*RevisionEntry{entry}}
					chunkBuf := make([]byte, chunk.MarshallSize())
					w := NewProtobufWriter(chunkBuf)
					assert.NoError(chunk.Marshall(w))
					blockId, _, err := r.WriteBlock(t.Context(), w.Bytes(), NewBlockBuf())
					assert.NoError(err)
					blockIds = append(blockIds, blockId)
				}
				message, author := "message for "+paths[0], "author"
				id, err := r.WriteRevision(t.Context(), &Revision{
					Magic:            RevisionMagic,
					Timestamp:        NewTimestampNow(),
					Message:          &message,
					Author:           &author,
					ParentRevisionId: parent,
					BlockIds:         blockIds,
				})
				assert.NoError(err)
				before = append(before, rawRevisionEntries(t, r, id))
				rev, err := r.ReadRevision(t.Context(), id, NewBlockBuf())
				assert.NoError(err)
				meta = append(meta, rev)
				parent = id
			}

			// A tiny chunk size makes the sorted result span several chunks, so
			// the loop that writes them back is exercised past the first.
			assert.NoError(RewriteRevisions(t.Context(), r.Repository, td.NewFS(t), td.NewHealthCheckMonitor(), 1))

			// The chain still has the same revisions, oldest last.
			chain, err := ReadRevisionChain(t.Context(), r.Repository)
			assert.NoError(err)
			assert.Equal(len(shape.revisions), len(chain))
			for i := range chain {
				// `chain` is head first, `before` is oldest first.
				old := len(chain) - 1 - i
				assert.Equal(before[old], rawRevisionEntries(t, r, chain[i]), "entries of revision %d", old)
				rev, err := r.ReadRevision(t.Context(), chain[i], NewBlockBuf())
				assert.NoError(err)
				assert.Equal(meta[old].Timestamp, rev.Timestamp, "timestamp of revision %d", old)
				assert.Equal(*meta[old].Message, *rev.Message, "message of revision %d", old)
				assert.Equal(*meta[old].Author, *rev.Author, "author of revision %d", old)
				assert.Equal(RevisionMagic, rev.Magic, "magic of revision %d", old)
			}

			// It reads back cleanly now, which it did not before. The entries
			// name fabricated content blocks, so only the order is checked.
			assert.NoError(CheckHealth(t.Context(), r.Repository, td.NewFS(t), HealthCheckOptions{
				Monitor:             td.NewHealthCheckMonitor(),
				CheckBlocks:         false,
				CheckOrphanedBlocks: false,
			}))

			// Running it again changes nothing, because the content is the same
			// and an id is the hash of the content.
			head := r.Head()
			assert.NoError(RewriteRevisions(t.Context(), r.Repository, td.NewFS(t), td.NewHealthCheckMonitor(), 1))
			assert.Equal(head, r.Head(), "a second run should be a no-op")
		})
	}
}

// A monitor that commits a revision the first time the migration looks at one,
// which lands inside the window between reading the chain and moving the head.
type commitDuringMigration struct {
	HealthCheckMonitor
	once func()
	done bool
}

func (m *commitDuringMigration) OnRevisionStart(id RevisionId) {
	if !m.done {
		m.done = true
		m.once()
	}
	m.HealthCheckMonitor.OnRevisionStart(id)
}

func TestFixRewriteRevisionsRefusesAConcurrentCommit(t *testing.T) {
	t.Parallel()
	assert := NewAssert(t)
	r := td.NewTestRepository(t, td.NewFS(t))

	// One revision in the old order, so the repository needs migrating.
	chunk := RevisionEntryChunk{Entries: []*RevisionEntry{
		td.RevisionEntry("a/z.txt", RevisionEntryKindAdd),
		td.RevisionEntryExt("a/b", RevisionEntryKindAdd, FileModeDir, ""),
	}}
	buf := make([]byte, chunk.MarshallSize())
	w := NewProtobufWriter(buf)
	assert.NoError(chunk.Marshall(w))
	blockId, _, err := r.WriteBlock(t.Context(), w.Bytes(), NewBlockBuf())
	assert.NoError(err)
	stale, err := r.WriteRevision(t.Context(), &Revision{ //nolint:exhaustruct
		Timestamp:        NewTimestampNow(),
		ParentRevisionId: RevisionId{},
		BlockIds:         []BlockId{blockId},
	})
	assert.NoError(err)

	var landed RevisionId
	monitor := &commitDuringMigration{
		HealthCheckMonitor: td.NewHealthCheckMonitor(),
		done:               false,
		once: func() {
			// A normal commit, which takes the head lock and sees the head the
			// migration has not moved yet, so it succeeds.
			landed = r.AddRevision(
				r.Head(),
				[]*RevisionEntry{td.RevisionEntry("payroll.csv", RevisionEntryKindAdd)},
			)
		},
	}

	err = RewriteRevisions(t.Context(), r.Repository, td.NewFS(t), monitor, DefaultTempChunkSize)
	assert.Error(err, "head moved")
	assert.Equal(true, errors.Is(err, ErrHeadChanged))

	// The commit is still the head and still reachable, and the migration
	// changed nothing.
	assert.Equal(landed, r.Head())
	chain, err := ReadRevisionChain(t.Context(), r.Repository)
	assert.NoError(err)
	assert.Equal(RevisionChain{landed, stale}, chain)
}
