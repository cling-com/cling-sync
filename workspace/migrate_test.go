package workspace

import (
	"testing"

	"github.com/cling-com/cling-sync/lib"
)

func TestFixHeadAfterRewrite(t *testing.T) {
	t.Parallel()

	// A repository with three revisions, migrated, plus the ids before and
	// after so a test can say where a workspace should land.
	// A repository whose revisions are in the order used before the sort order
	// changed, so the migration really has to rewrite them and every id moves.
	setup := func(t *testing.T) (*lib.TestRepository, *TestWorkspace, lib.RevisionChain, lib.RevisionChain) {
		t.Helper()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)
		parent := lib.RevisionId{}
		for _, name := range []string{"one", "two", "three"} {
			// A directory's files before its subdirectories, which no longer sorts.
			chunk := lib.RevisionEntryChunk{Entries: []*lib.RevisionEntry{
				td.RevisionEntry(name+"/z.txt", lib.RevisionEntryKindAdd),
				td.RevisionEntryExt(name+"/b", lib.RevisionEntryKindAdd, lib.FileModeDir, ""),
			}}
			buf := make([]byte, chunk.MarshallSize())
			pw := lib.NewProtobufWriter(buf)
			assert.NoError(chunk.Marshall(pw))
			blockId, _, err := r.WriteBlock(t.Context(), pw.Bytes(), lib.NewBlockBuf())
			assert.NoError(err)
			id, err := r.WriteRevision(t.Context(), &lib.Revision{ //nolint:exhaustruct
				Timestamp:        lib.NewTimestampNow(),
				ParentRevisionId: parent,
				BlockIds:         []lib.BlockId{blockId},
			})
			assert.NoError(err)
			parent = id
		}
		before, err := lib.ReadRevisionChain(t.Context(), r.Repository)
		assert.NoError(err)
		assert.NoError(lib.WriteRef(t.Context(), w.Storage, "head", before[0]))

		assert.NoError(lib.RewriteRevisions(
			t.Context(), r.Repository, td.NewFS(t), td.NewRevisionSnapshotMonitor(), lib.DefaultTempChunkSize,
		))
		after, err := lib.ReadRevisionChain(t.Context(), r.Repository)
		assert.NoError(err)
		assert.Equal(len(before), len(after))
		// Without this the fixture would need no migrating and every assertion
		// below would hold for the wrong reason.
		assert.Equal(true, before[0] != after[0], "the fixture must actually need migrating")
		return r, w, before, after
	}

	t.Run("A workspace at the head follows the head", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r, w, before, after := setup(t)
		assert.Equal(before[0], w.Head())

		from, to, err := FixHeadAfterRewrite(t.Context(), w.Workspace, r.Repository)
		assert.NoError(err)
		assert.Equal(before[0], from)
		assert.Equal(after[0], to)
		assert.Equal(after[0], w.Head())
	})

	t.Run("A workspace left behind stays behind", func(t *testing.T) {
		// The blunt version dragged it to the head, which made the revisions it
		// had not seen look like local deletions.
		t.Parallel()
		assert := lib.NewAssert(t)
		r, w, before, after := setup(t)
		// Pretend it only ever merged the first revision. `before` is head first.
		assert.NoError(lib.WriteRef(t.Context(), w.Storage, "head", before[2]))

		from, to, err := FixHeadAfterRewrite(t.Context(), w.Workspace, r.Repository)
		assert.NoError(err)
		assert.Equal(before[2], from)
		assert.Equal(after[2], to, "should land on the replacement of revision 1, not on the head")
		assert.Equal(after[2], w.Head())
	})

	t.Run("Running it again changes nothing", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r, w, _, after := setup(t)
		_, _, err := FixHeadAfterRewrite(t.Context(), w.Workspace, r.Repository)
		assert.NoError(err)

		from, to, err := FixHeadAfterRewrite(t.Context(), w.Workspace, r.Repository)
		assert.NoError(err)
		assert.Equal(from, to)
		assert.Equal(after[0], w.Head())
	})

	t.Run("Commits after the migration do not shift the answer", func(t *testing.T) {
		// The distance is counted from the root, not from the head, so
		// revisions added after the migration must not move a workspace that
		// was left behind.
		t.Parallel()
		assert := lib.NewAssert(t)
		r, w, before, after := setup(t)
		// Two more revisions land after the migration.
		head := r.Head()
		for _, name := range []string{"four", "five"} {
			head = r.AddRevision(head, []*lib.RevisionEntry{
				td.RevisionEntry(name+".txt", lib.RevisionEntryKindAdd),
			})
		}
		grown, err := lib.ReadRevisionChain(t.Context(), r.Repository)
		assert.NoError(err)
		assert.Equal(len(after)+2, len(grown))

		// The workspace still names the oldest revision of the old chain.
		assert.NoError(lib.WriteRef(t.Context(), w.Storage, "head", before[len(before)-1]))
		from, to, err := FixHeadAfterRewrite(t.Context(), w.Workspace, r.Repository)
		assert.NoError(err)
		assert.Equal(before[len(before)-1], from)
		// The replacement of the oldest revision, which is the oldest of the
		// current chain, not something counted back from the head.
		assert.Equal(grown[len(grown)-1], to)
		assert.Equal(after[len(after)-1], to)
	})

	t.Run("A workspace that never merged is left alone", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))
		w := wstd.NewTestWorkspace(t, r.Repository)

		from, to, err := FixHeadAfterRewrite(t.Context(), w.Workspace, r.Repository)
		assert.NoError(err)
		assert.Equal(true, from.IsRoot())
		assert.Equal(from, to)
	})
}
