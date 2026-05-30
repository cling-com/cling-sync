package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"slices"
	"testing"
)

func TestCheckHealth(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		blockId1, _, err := r.WriteBlock([]byte("abc"), NewBlockBuf())
		assert.NoError(err)
		blockId2, _, err := r.WriteBlock([]byte("de"), NewBlockBuf())
		assert.NoError(err)
		e1 := td.RevisionEntry("a.txt", RevisionEntryKindAdd)
		e1.Metadata.BlockIds = []BlockId{blockId1}
		e1.Metadata.Size = 3
		e1.Metadata.FileHash = td.SHA256("abc")
		e2 := td.RevisionEntry("b.txt", RevisionEntryKindUpdate)
		e2.Metadata.BlockIds = []BlockId{blockId2}
		e2.Metadata.Size = 2
		e2.Metadata.FileHash = td.SHA256("de")
		e3 := td.RevisionEntry("c.txt", RevisionEntryKindDelete)
		e3.Metadata.BlockIds = []BlockId{blockId1, blockId2}
		e3.Metadata.Size = 5
		e3.Metadata.FileHash = td.SHA256("abcde")
		assert.NoError(commit.Add(e2))
		assert.NoError(commit.Add(e1))
		assert.NoError(commit.Add(e3))
		rev1Id, err := commit.Commit(td.CommitInfo())
		assert.NoError(err)

		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(
			r.Repository,
			td.NewFS(t),
			HealthCheckOptions{Monitor: monitor, CheckBlocks: false, CheckOrphanedBlocks: false},
		)
		assert.NoError(err)
		assert.Calls([]MockCall{
			NewMockCall("OnRevisionStart", rev1Id),
			NewMockCall("OnRevisionEntry", e1),
			NewMockCall("OnRevisionEntry", e2),
			NewMockCall("OnRevisionEntry", e3),
		}, monitor.Calls)
	})

	t.Run("Verify blocks", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		blockId1, _, err := r.WriteBlock([]byte("abc"), NewBlockBuf())
		assert.NoError(err)
		blockId2, _, err := r.WriteBlock([]byte("de"), NewBlockBuf())
		assert.NoError(err)
		e1 := td.RevisionEntry("a.txt", RevisionEntryKindAdd)
		e1.Metadata.BlockIds = []BlockId{blockId1}
		e1.Metadata.Size = 3
		e1.Metadata.FileHash = td.SHA256("abc")
		e2 := td.RevisionEntry("b.txt", RevisionEntryKindUpdate)
		e2.Metadata.BlockIds = []BlockId{blockId2}
		e2.Metadata.Size = 2
		e2.Metadata.FileHash = td.SHA256("de")
		e3 := td.RevisionEntry("c.txt", RevisionEntryKindDelete)
		e3.Metadata.BlockIds = []BlockId{blockId1, blockId2}
		e3.Metadata.Size = 5
		e3.Metadata.FileHash = td.SHA256("abcde")
		assert.NoError(commit.Add(e2))
		assert.NoError(commit.Add(e1))
		assert.NoError(commit.Add(e3))
		rev1Id, err := commit.Commit(td.CommitInfo())
		assert.NoError(err)

		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(
			r.Repository,
			td.NewFS(t),
			HealthCheckOptions{Monitor: monitor, CheckBlocks: true, CheckOrphanedBlocks: false},
		)
		assert.NoError(err)
		assert.Equal(8, len(monitor.Calls))
		assert.Calls([]MockCall{
			NewMockCall("OnRevisionStart", rev1Id),
			NewMockCall("OnRevisionEntry", e1),
			NewMockCall("OnRevisionEntry", e2),
			NewMockCall("OnRevisionEntry", e3),
		}, monitor.Calls[:4])
		assert.Call(NewMockCall("OnBlockVerified", blockId1, 3), monitor.Calls[4:])
		assert.Call(NewMockCall("OnBlockVerified", blockId2, 2), monitor.Calls[4:])
	})

	t.Run("Verify blocks detects broken blocks", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		blockId1, _, err := r.WriteBlock([]byte("abc"), NewBlockBuf())
		assert.NoError(err)
		blockId2, _, err := r.WriteBlock([]byte("def"), NewBlockBuf())
		assert.NoError(err)
		blockId3, _, err := r.WriteBlock([]byte("ghi"), NewBlockBuf())
		assert.NoError(err)
		e := td.RevisionEntry("a.txt", RevisionEntryKindAdd)
		e.Metadata.BlockIds = []BlockId{blockId1, blockId2, blockId3}
		e.Metadata.Size = 9
		e.Metadata.FileHash = td.SHA256("abcdefghi")
		assert.NoError(commit.Add(e))
		_, err = commit.Commit(td.CommitInfo())
		assert.NoError(err)

		// Flip a bit in the second data block.
		path := r.Storage.blockPath(blockId2)
		data, err := ReadFile(r.Storage.FS, path)
		assert.NoError(err)
		data[len(data)/2] ^= 1
		assert.NoError(r.Storage.FS.Chmod(path, 0o600))
		assert.NoError(WriteFile(r.Storage.FS, path, data))

		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(
			r.Repository,
			td.NewFS(t),
			HealthCheckOptions{Monitor: monitor, CheckBlocks: true, CheckOrphanedBlocks: false},
		)
		assert.Error(err, "failed to verify block")
		assert.Error(err, blockId2.String())
	})

	t.Run("Missing block", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		e1 := td.RevisionEntry("a.txt", RevisionEntryKindAdd)
		blockId, _, err := r.WriteBlock([]byte{1, 2, 3}, NewBlockBuf())
		assert.NoError(err)
		e1.Metadata.BlockIds = []BlockId{blockId, td.BlockId("1")}
		assert.NoError(commit.Add(e1))
		_, err = commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
		assert.NoError(err)

		// When not checking for data blocks, nothing is detected.
		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(
			r.Repository,
			td.NewFS(t),
			HealthCheckOptions{Monitor: monitor, CheckBlocks: false, CheckOrphanedBlocks: false},
		)
		assert.NoError(err)

		// With --data the missing block surfaces as a read error.
		err = CheckHealth(
			r.Repository,
			td.NewFS(t),
			HealthCheckOptions{Monitor: monitor, CheckBlocks: true, CheckOrphanedBlocks: false},
		)
		assert.Error(err, "failed to verify block")
		assert.Error(err, "block not found")
	})

	t.Run("Duplicate path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// Normal commits reject duplicate entries, so write a malformed revision block directly.
		e1 := td.RevisionEntry("a.txt", RevisionEntryKindAdd)
		e2 := td.RevisionEntry("a.txt", RevisionEntryKindUpdate)
		chunk := RevisionEntryChunk{Entries: []*RevisionEntry{e1, e2}}
		chunkBuf := make([]byte, chunk.MarshallSize())
		chunkWriter := NewProtobufWriter(chunkBuf)
		assert.NoError(chunk.Marshall(chunkWriter))
		chunkBlockId, _, err := r.WriteBlock(chunkWriter.Bytes(), NewBlockBuf())
		assert.NoError(err)
		_, err = r.WriteRevision(&Revision{ //nolint:exhaustruct
			Timestamp:        NewTimestampNow(),
			ParentRevisionId: RevisionId{},
			BlockIds:         []BlockId{chunkBlockId},
		})
		assert.NoError(err)

		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(
			r.Repository,
			td.NewFS(t),
			HealthCheckOptions{Monitor: monitor, CheckBlocks: false, CheckOrphanedBlocks: false},
		)
		assert.Error(err, "not strictly sorted")
		assert.Error(err, "a.txt >= a.txt")
	})

	t.Run("Non-symlink with target", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// Build a malformed entry: regular file but has SymLinkTarget.
		e := td.RevisionEntry("a.txt", RevisionEntryKindAdd)
		e.Metadata.FileMode = 0o600
		target, err := NewPath("stray")
		assert.NoError(err)
		e.Metadata.SymLinkTarget = &target
		chunk := RevisionEntryChunk{Entries: []*RevisionEntry{e}}
		chunkBuf := make([]byte, chunk.MarshallSize())
		chunkWriter := NewProtobufWriter(chunkBuf)
		assert.NoError(chunk.Marshall(chunkWriter))
		chunkBlockId, _, err := r.WriteBlock(chunkWriter.Bytes(), NewBlockBuf())
		assert.NoError(err)
		_, err = r.WriteRevision(&Revision{ //nolint:exhaustruct
			Timestamp:        NewTimestampNow(),
			ParentRevisionId: RevisionId{},
			BlockIds:         []BlockId{chunkBlockId},
		})
		assert.NoError(err)

		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(r.Repository, td.NewFS(t), HealthCheckOptions{
			Monitor: monitor, CheckBlocks: false, CheckOrphanedBlocks: false,
		})
		assert.Error(err, "has SymLinkTarget but is not a symlink")
	})

	t.Run("Orphaned blocks", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// One referenced block and two orphaned blocks (written directly to storage,
		// never referenced by any revision).
		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		referenced, _, err := r.WriteBlock([]byte("hello"), NewBlockBuf())
		assert.NoError(err)
		orphan1, _, err := r.WriteBlock([]byte("orphan-1"), NewBlockBuf())
		assert.NoError(err)
		orphan2, _, err := r.WriteBlock([]byte("orphan-2"), NewBlockBuf())
		assert.NoError(err)
		e := td.RevisionEntry("a.txt", RevisionEntryKindAdd)
		e.Metadata.BlockIds = []BlockId{referenced}
		e.Metadata.Size = 5
		e.Metadata.FileHash = td.SHA256("hello")
		assert.NoError(commit.Add(e))
		rev1Id, err := commit.Commit(td.CommitInfo())
		assert.NoError(err)

		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(r.Repository, td.NewFS(t), HealthCheckOptions{
			Monitor: monitor, CheckBlocks: false, CheckOrphanedBlocks: true,
		})
		assert.NoError(err)

		// Orphans are emitted in BlockIdCompare order after the revision walk.
		sortedOrphans := []BlockId{orphan1, orphan2}
		slices.SortFunc(sortedOrphans, BlockIdCompare)
		assert.Calls([]MockCall{
			NewMockCall("OnRevisionStart", rev1Id),
			NewMockCall("OnRevisionEntry", e),
			NewMockCall("OnOrphanedBlock", sortedOrphans[0]),
			NewMockCall("OnOrphanedBlock", sortedOrphans[1]),
		}, monitor.Calls)
	})
}

func TestFormatDoesNotChangeUnexpectedly(t *testing.T) {
	t.Parallel()
	assert := NewAssert(t)
	want := "119c51dd05c7a50f321ec70ae5bbd51f6de311bcacf06e9436b0afea6c72e208"
	data, err := os.ReadFile("format.proto") //nolint:forbidigo
	assert.NoError(err)
	sum := sha256.Sum256(data)
	assert.Equal(
		want,
		hex.EncodeToString(sum[:]),
		"format.proto changed: Please check whether health.go still scans all blocks",
	)
}
