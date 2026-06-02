//nolint:exhaustruct
package lib

import (
	"io/fs"
	"slices"
	"testing"
)

func TestSyncRepository(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		monitor := &TestSyncMonitor{}

		entry1, blockId1 := testEntry(t, src, "a.txt", "abc")
		_, err := testCommit(t, src.Repository, entry1)
		assert.NoError(err)

		entry2, blockId2 := testEntry(t, src, "b.txt", "de")
		entry3, blockId3 := testEntry(t, src, "dir/c.txt", "fghi")
		rev2Id, err := testCommit(t, src.Repository, entry2, entry3)
		assert.NoError(err)

		err = SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t), td.RevisionChain(t, src),
			RepositorySyncOptions{Monitor: monitor, Workers: 8},
		)
		assert.NoError(err)

		dstHead, err := ReadRef(t.Context(), dst.Storage, "head")
		assert.NoError(err)
		assert.Equal(rev2Id, dstHead)
		assertSameHistory(t, src, dst)
		assertSameFS(t, src.FS, dst.FS)

		assert.Call(NewMockCall("OnBeforeCopy", 7, 0), monitor.Calls)
		assert.Call(NewMockCall("OnCopyBlock", blockId1, false, assert.Any), monitor.Calls)
		assert.Call(NewMockCall("OnCopyBlock", blockId2, false, assert.Any), monitor.Calls)
		assert.Call(NewMockCall("OnCopyBlock", blockId3, false, assert.Any), monitor.Calls)
		assert.Call(NewMockCall("OnBeforeUpdateDstHead", rev2Id), monitor.Calls)
		assert.Equal(7, monitor.CountCalls("OnCopyBlock"))
	})

	t.Run("Syncs missing blocks even when heads match", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		entry, blockId := testEntry(t, src, "a.txt", "abc")
		revId, err := testCommit(t, src.Repository, entry)
		assert.NoError(err)

		// Mirror src into dst but skip one data block, then point dst at src's
		// head. Matching heads must not be taken as proof that dst already holds
		// every block, so the sync still has to copy the missing one.
		buf := NewBlockBuf()
		err = src.Storage.ReadBlockIds(t.Context(), func(id BlockId) bool {
			if id == blockId {
				return true
			}
			data, err := src.Storage.ReadBlock(t.Context(), id, buf)
			assert.NoError(err)
			_, err = dst.Storage.WriteBlock(t.Context(), id, data)
			assert.NoError(err)
			return true
		})
		assert.NoError(err)
		assert.NoError(WriteRef(t.Context(), dst.Storage, "head", revId))

		monitor := &TestSyncMonitor{}
		err = SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t), td.RevisionChain(t, src),
			RepositorySyncOptions{Monitor: monitor, Workers: 8},
		)
		assert.NoError(err)

		assert.Call(NewMockCall("OnCopyBlock", blockId, false, assert.Any), monitor.Calls)
		assert.Equal(1, monitor.CountCalls("OnCopyBlock"))
		assert.Call(NewMockCall("OnBeforeUpdateDstHead", revId), monitor.Calls)
		assertSameHistory(t, src, dst)
	})

	t.Run("Copies only missing blocks", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		entry1, _ := testEntry(t, src, "a.txt", "abc")
		_, err := testCommit(t, src.Repository, entry1)
		assert.NoError(err)
		assert.NoError(
			SyncRepository(
				t.Context(),
				src.Storage,
				dst.Storage,
				td.NewFS(t),
				td.RevisionChain(t, src),
				RepositorySyncOptions{Monitor: &TestSyncMonitor{}, Workers: 8},
			),
		)
		entry2, blockId2 := testEntry(t, src, "b.txt", "def")
		rev2Id, err := testCommit(t, src.Repository, entry2)
		assert.NoError(err)

		monitor := &TestSyncMonitor{}
		err = SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t), td.RevisionChain(t, src),
			RepositorySyncOptions{Monitor: monitor, Workers: 8},
		)
		assert.NoError(err)

		dstHead, err := ReadRef(t.Context(), dst.Storage, "head")
		assert.NoError(err)
		assert.Equal(rev2Id, dstHead)

		assert.Call(NewMockCall("OnCopyBlock", blockId2, false, assert.Any), monitor.Calls)
		assert.Call(NewMockCall("OnBeforeUpdateDstHead", rev2Id), monitor.Calls)
		// Two new blocks were committed (data + revision). Only those should copy.
		assert.Equal(3, monitor.CountCalls("OnCopyBlock"))
	})

	t.Run("Does not copy duplicate data block twice", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		blockId, _, err := src.WriteBlock(t.Context(), []byte("shared"), NewBlockBuf())
		assert.NoError(err)
		entry1 := td.RevisionEntry("a.txt", RevisionEntryKindAdd)
		entry1.Metadata.BlockIds = []BlockId{blockId}
		entry1.Metadata.Size = 6
		entry1.Metadata.FileHash = td.SHA256("shared")
		entry2 := td.RevisionEntry("b.txt", RevisionEntryKindAdd)
		entry2.Metadata.BlockIds = []BlockId{blockId}
		entry2.Metadata.Size = 6
		entry2.Metadata.FileHash = td.SHA256("shared")
		commit, err := NewCommit(t.Context(), src.Repository, td.NewFS(t))
		assert.NoError(err)
		assert.NoError(commit.Add(entry1))
		assert.NoError(commit.Add(entry2))
		_, err = commit.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)

		monitor := &TestSyncMonitor{}
		err = SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t), td.RevisionChain(t, src),
			RepositorySyncOptions{Monitor: monitor, Workers: 8},
		)
		assert.NoError(err)

		assert.Equal(3, monitor.CountCalls("OnCopyBlock"))
		assert.Equal(1, monitor.CountBlockCopies(blockId))
	})

	t.Run("Fails when src head is missing from src storage", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		assert.NoError(WriteRef(t.Context(), src.Storage, "head", td.RevisionId("ghost")))

		monitor := &TestSyncMonitor{}
		err := SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t), nil,
			RepositorySyncOptions{Monitor: monitor, Workers: 8, SkipHeadCheck: true},
		)
		assert.Error(err, "not present in src storage")
		assert.Calls([]MockCall{}, monitor.Calls)
	})

	t.Run("Fails for incompatible repositories", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := td.NewTestRepository(t, td.NewFS(t))

		entry, _ := testEntry(t, src, "a.txt", "abc")
		_, err := testCommit(t, src.Repository, entry)
		assert.NoError(err)

		monitor := &TestSyncMonitor{}
		err = SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t), td.RevisionChain(t, src),
			RepositorySyncOptions{Monitor: monitor, Workers: 8},
		)
		assert.Error(err, "config")
		assert.Calls([]MockCall{}, monitor.Calls)
	})

	t.Run("Src head not first in chain should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		entry, _ := testEntry(t, src, "a.txt", "abc")
		_, err := testCommit(t, src.Repository, entry)
		assert.NoError(err)

		monitor := &TestSyncMonitor{}
		err = SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t),
			RevisionChain{td.RevisionId("bogus")},
			RepositorySyncOptions{Monitor: monitor, Workers: 8},
		)
		assert.Error(err, "is not the first revision")
		assert.Calls([]MockCall{}, monitor.Calls)
	})

	t.Run("Dst head not in chain should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		entry1, _ := testEntry(t, src, "a.txt", "abc")
		_, err := testCommit(t, src.Repository, entry1)
		assert.NoError(err)
		assert.NoError(SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t),
			td.RevisionChain(t, src), RepositorySyncOptions{Monitor: &TestSyncMonitor{}, Workers: 8},
		))
		entry2, _ := testEntry(t, src, "b.txt", "def")
		rev2Id, err := testCommit(t, src.Repository, entry2)
		assert.NoError(err)

		// The chain omits dst's head (the first revision).
		monitor := &TestSyncMonitor{}
		err = SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t),
			RevisionChain{rev2Id},
			RepositorySyncOptions{Monitor: monitor, Workers: 8},
		)
		assert.Error(err, "is not in src's revision chain")
		assert.Calls([]MockCall{}, monitor.Calls)
	})

	t.Run("Dst head missing from src storage should fail even when head check is skipped", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		srcEntry, _ := testEntry(t, src, "a.txt", "abc")
		_, err := testCommit(t, src.Repository, srcEntry)
		assert.NoError(err)

		// A revision committed only to dst so its block is absent from src.
		dstEntry, _ := testEntry(t, dst, "b.txt", "def")
		_, err = testCommit(t, dst.Repository, dstEntry)
		assert.NoError(err)

		err = SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t), nil,
			RepositorySyncOptions{Monitor: &TestSyncMonitor{}, Workers: 8, SkipHeadCheck: true},
		)
		assert.Error(err, "dst head")
	})

	t.Run("SkipHeadCheck skips chain validation", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		entry, _ := testEntry(t, src, "a.txt", "abc")
		srcHead, err := testCommit(t, src.Repository, entry)
		assert.NoError(err)

		// A nil chain would fail the head check, but it is skipped.
		err = SyncRepository(
			t.Context(), src.Storage, dst.Storage, td.NewFS(t), nil,
			RepositorySyncOptions{Monitor: &TestSyncMonitor{}, Workers: 8, SkipHeadCheck: true},
		)
		assert.NoError(err)
		dstHead, err := ReadRef(t.Context(), dst.Storage, "head")
		assert.NoError(err)
		assert.Equal(srcHead, dstHead)
	})
}

func testEntry(t *testing.T, r *TestRepository, path, content string) (*RevisionEntry, BlockId) {
	t.Helper()
	assert := NewAssert(t)
	entry := td.RevisionEntry(path, RevisionEntryKindAdd)
	blockId, _, err := r.WriteBlock(t.Context(), []byte(content), NewBlockBuf())
	assert.NoError(err)
	entry.Metadata.BlockIds = []BlockId{blockId}
	entry.Metadata.Size = int64(len(content))
	entry.Metadata.FileHash = td.SHA256(content)
	return entry, blockId
}

func assertSameHistory(t *testing.T, src, dst *TestRepository) {
	t.Helper()
	assert := NewAssert(t)

	srcRevisionId := src.Head()
	dstRevisionId := dst.Head()
	assert.Equal(srcRevisionId, dstRevisionId)

	buf := NewBlockBuf()
	for !srcRevisionId.IsRoot() {
		srcRevision, err := src.ReadRevision(t.Context(), srcRevisionId, buf)
		assert.NoError(err)
		dstRevision, err := dst.ReadRevision(t.Context(), dstRevisionId, buf)
		assert.NoError(err)
		assert.Equal(srcRevision, dstRevision)
		assert.Equal(src.RevisionInfos(srcRevisionId), dst.RevisionInfos(dstRevisionId))
		srcRevisionId = srcRevision.ParentRevisionId
		dstRevisionId = dstRevision.ParentRevisionId
	}

	assert.Equal(true, dstRevisionId.IsRoot())
}

func cloneRepository(t *testing.T, src *TestRepository) *TestRepository {
	t.Helper()
	assert := NewAssert(t)
	dst := td.NewTestRepository(t, td.NewFS(t))
	configData, err := ReadFile(src.FS, ".cling/repository.txt")
	assert.NoError(err)
	assert.NoError(WriteFile(dst.FS, ".cling/repository.txt", configData))
	return td.OpenRepository(t, dst.FS)
}

func assertSameFS(t *testing.T, src, dst FS) {
	t.Helper()
	assert := NewAssert(t)
	srcPaths := []string{}
	dstPaths := []string{}
	srcInfo := map[string]fs.FileInfo{}
	dstInfo := map[string]fs.FileInfo{}

	err := src.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		srcPaths = append(srcPaths, path)
		srcInfo[path] = info
		return nil
	})
	assert.NoError(err)
	err = dst.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		dstPaths = append(dstPaths, path)
		dstInfo[path] = info
		return nil
	})
	assert.NoError(err)
	slices.Sort(srcPaths)
	slices.Sort(dstPaths)
	assert.Equal(srcPaths, dstPaths)
	for _, path := range srcPaths {
		assert.Equal(srcInfo[path].Mode(), dstInfo[path].Mode(), path)
		if srcInfo[path].IsDir() {
			continue
		}
		if path == ".cling/repository/locks/head" {
			continue
		}
		srcData, err := ReadFile(src, path)
		assert.NoError(err, path)
		dstData, err := ReadFile(dst, path)
		assert.NoError(err, path)
		assert.Equal(srcData, dstData, path)
	}
}

type TestSyncMonitor struct {
	Calls []MockCall
}

func (m *TestSyncMonitor) CountCalls(name string) int {
	count := 0
	for _, call := range m.Calls {
		if call.Name == name {
			count += 1
		}
	}
	return count
}

func (m *TestSyncMonitor) CountBlockCopies(blockId BlockId) int {
	count := 0
	for _, call := range m.Calls {
		if call.Name == "OnCopyBlock" && len(call.Args) > 0 && areEqual(call.Args[0], blockId) {
			count += 1
		}
	}
	return count
}

func (m *TestSyncMonitor) OnSrcBlockIdsRead(n int) {
	m.Calls = append(m.Calls, NewMockCall("OnSrcBlockIdsRead", n))
}

func (m *TestSyncMonitor) OnDstBlockIdsRead(n int) {
	m.Calls = append(m.Calls, NewMockCall("OnDstBlockIdsRead", n))
}

func (m *TestSyncMonitor) OnBeforeCopy(srcBlocks, dstBlocks int) {
	m.Calls = append(m.Calls, NewMockCall("OnBeforeCopy", srcBlocks, dstBlocks))
}

func (m *TestSyncMonitor) OnCopyBlock(blockId BlockId, existed bool, length int) {
	m.Calls = append(m.Calls, NewMockCall("OnCopyBlock", blockId, existed, length))
}

func (m *TestSyncMonitor) OnBeforeUpdateDstHead(newHead RevisionId) {
	m.Calls = append(m.Calls, NewMockCall("OnBeforeUpdateDstHead", newHead))
}
