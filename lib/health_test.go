package lib

import (
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
		_, blockHeader1, err := r.WriteBlock([]byte("abc"))
		assert.NoError(err)
		_, blockHeader2, err := r.WriteBlock([]byte("de"))
		assert.NoError(err)
		e1 := td.RevisionEntry("a.txt", RevisionEntryAdd)
		e1.Metadata.BlockIds = []BlockId{blockHeader1.BlockId}
		e1.Metadata.Size = 3
		e1.Metadata.FileHash = td.SHA256("abc")
		e2 := td.RevisionEntry("b.txt", RevisionEntryUpdate)
		e2.Metadata.BlockIds = []BlockId{blockHeader2.BlockId}
		e2.Metadata.Size = 2
		e2.Metadata.FileHash = td.SHA256("de")
		e3 := td.RevisionEntry("c.txt", RevisionEntryDelete)
		e3.Metadata.BlockIds = []BlockId{blockHeader1.BlockId, blockHeader2.BlockId}
		e3.Metadata.Size = 5
		e3.Metadata.FileHash = td.SHA256("abcde")
		assert.NoError(commit.Add(e2))
		assert.NoError(commit.Add(e1))
		assert.NoError(commit.Add(e3))
		rev1Id, err := commit.Commit(td.CommitInfo())
		assert.NoError(err)

		// Check without data blocks.
		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(r.Repository, HealthCheckOptions{Monitor: monitor, DataBlocks: false})
		assert.NoError(err)
		assert.Calls([]MockCall{
			NewMockCall("OnRevisionStart", rev1Id),
			NewMockCall("OnBlockOk", assert.Any, false, 431),
			NewMockCall("OnRevisionEntry", e1),
			NewMockCall("OnRevisionEntry", e2),
			NewMockCall("OnRevisionEntry", e3),
		}, monitor.Calls)

		// Check with data blocks.
		monitor = td.NewHealthCheckMonitor()
		err = CheckHealth(r.Repository, HealthCheckOptions{Monitor: monitor, DataBlocks: true})
		assert.NoError(err)
		assert.Calls([]MockCall{
			NewMockCall("OnRevisionStart", rev1Id),
			NewMockCall("OnBlockOk", assert.Any, false, 431),
			NewMockCall("OnRevisionEntry", e1),
			NewMockCall("OnBlockOk", blockHeader1.BlockId, false, 3),
			NewMockCall("OnRevisionEntry", e2),
			NewMockCall("OnBlockOk", blockHeader2.BlockId, false, 2),
			NewMockCall("OnRevisionEntry", e3),
			NewMockCall("OnBlockOk", blockHeader1.BlockId, true, 3),
			NewMockCall("OnBlockOk", blockHeader2.BlockId, true, 2),
		}, monitor.Calls)
	})

	t.Run("Missing block", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		e1 := td.RevisionEntry("a.txt", RevisionEntryAdd)
		_, blockHeader, err := r.WriteBlock([]byte{1, 2, 3})
		assert.NoError(err)
		e1.Metadata.BlockIds = []BlockId{blockHeader.BlockId, td.BlockId("1")}
		assert.NoError(commit.Add(e1))
		_, err = commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
		assert.NoError(err)

		// When not checking for data blocks, nothing is detected.
		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(r.Repository, HealthCheckOptions{Monitor: monitor, DataBlocks: false})
		assert.NoError(err)

		// Now check for data blocks.
		err = CheckHealth(r.Repository, HealthCheckOptions{Monitor: monitor, DataBlocks: true})
		assert.Error(err, "failed to check block")
		assert.Error(err, "block not found")
	})

	t.Run("Invalid file size in metadata", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		e1 := td.RevisionEntry("a.txt", RevisionEntryAdd)
		_, blockHeader, err := r.WriteBlock([]byte{1, 2, 3})
		assert.NoError(err)
		e1.Metadata.BlockIds = []BlockId{blockHeader.BlockId}
		e1.Metadata.Size = 42
		assert.NoError(commit.Add(e1))
		_, err = commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
		assert.NoError(err)

		// When not checking for data blocks, file sizes are not checked.
		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(r.Repository, HealthCheckOptions{Monitor: monitor, DataBlocks: false})
		assert.NoError(err)

		// But when checking for data blocks, file sizes are checked.
		err = CheckHealth(r.Repository, HealthCheckOptions{Monitor: monitor, DataBlocks: true})
		assert.Error(err, "file size mismatch")
	})

	t.Run("Invalid file hash in metadata", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		commit, err := NewCommit(r.Repository, td.NewFS(t))
		assert.NoError(err)
		e1 := td.RevisionEntry("a.txt", RevisionEntryAdd)
		_, blockHeader, err := r.WriteBlock([]byte("abc"))
		assert.NoError(err)
		e1.Metadata.BlockIds = []BlockId{blockHeader.BlockId}
		e1.Metadata.Size = 3
		e1.Metadata.FileHash = td.SHA256("not abc")
		assert.NoError(commit.Add(e1))
		_, err = commit.Commit(&CommitInfo{Author: "test author", Message: "test message"})
		assert.NoError(err)

		// When not checking for data blocks, file hashes are not checked.
		monitor := td.NewHealthCheckMonitor()
		err = CheckHealth(r.Repository, HealthCheckOptions{Monitor: monitor, DataBlocks: false})
		assert.NoError(err)

		// But when checking for data blocks, file hashes are checked.
		err = CheckHealth(r.Repository, HealthCheckOptions{Monitor: monitor, DataBlocks: true})
		assert.Error(err, "file hash mismatch")
	})
}
