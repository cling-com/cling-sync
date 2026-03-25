//nolint:exhaustruct
package lib

import (
	"context"
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
		rev1Id, err := testCommit(t, src.Repository, entry1)
		assert.NoError(err)

		entry2, blockId2 := testEntry(t, src, "b.txt", "de")
		entry3, blockId3 := testEntry(t, src, "dir/c.txt", "fghi")
		rev2Id, err := testCommit(t, src.Repository, entry2, entry3)
		assert.NoError(err)

		err = SyncRepository(context.Background(), src.Repository, dst.Storage, RepositorySyncOptions{Monitor: monitor})
		assert.NoError(err)

		dstHead, err := ReadRef(dst.Storage, "head")
		assert.NoError(err)
		assert.Equal(rev2Id, dstHead)
		assertSameHistory(t, src, dst)
		assertSameFS(t, src.FS, dst.FS)

		assert.Call(NewMockCall("OnRevisionStart", rev1Id), monitor.Calls)
		assert.Call(NewMockCall("OnRevisionStart", rev2Id), monitor.Calls)
		assert.Call(NewMockCall("OnRevisionEntry", entry1), monitor.Calls)
		assert.Call(NewMockCall("OnRevisionEntry", entry2), monitor.Calls)
		assert.Call(NewMockCall("OnRevisionEntry", entry3), monitor.Calls)
		assert.Call(NewMockCall("OnCopyBlock", blockId1, false, assert.Any), monitor.Calls)
		assert.Call(NewMockCall("OnCopyBlock", blockId2, false, assert.Any), monitor.Calls)
		assert.Call(NewMockCall("OnCopyBlock", blockId3, false, assert.Any), monitor.Calls)
		assert.Call(NewMockCall("OnBeforeUpdateDstHead", rev2Id), monitor.Calls)
		assert.Equal(7, monitor.CountCalls("OnCopyBlock"))
	})

	t.Run("Noop when heads match", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		entry, _ := testEntry(t, src, "a.txt", "abc")
		_, err := testCommit(t, src.Repository, entry)
		assert.NoError(err)
		assert.NoError(
			SyncRepository(
				context.Background(),
				src.Repository,
				dst.Storage,
				RepositorySyncOptions{Monitor: &TestSyncMonitor{}},
			),
		)

		monitor := &TestSyncMonitor{}
		err = SyncRepository(context.Background(), src.Repository, dst.Storage, RepositorySyncOptions{Monitor: monitor})
		assert.NoError(err)
		assert.Calls([]MockCall{}, monitor.Calls)
	})

	t.Run("Syncs only newer revisions", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		entry1, _ := testEntry(t, src, "a.txt", "abc")
		rev1Id, err := testCommit(t, src.Repository, entry1)
		assert.NoError(err)
		entry2, blockId2 := testEntry(t, src, "b.txt", "def")
		rev2Id, err := testCommit(t, src.Repository, entry2)
		assert.NoError(err)

		assert.NoError(WriteRef(dst.Storage, "head", rev1Id))

		monitor := &TestSyncMonitor{}
		err = SyncRepository(context.Background(), src.Repository, dst.Storage, RepositorySyncOptions{Monitor: monitor})
		assert.NoError(err)

		dstHead, err := ReadRef(dst.Storage, "head")
		assert.NoError(err)
		assert.Equal(rev2Id, dstHead)

		assert.Call(NewMockCall("OnRevisionStart", rev2Id), monitor.Calls)
		assert.Call(NewMockCall("OnRevisionEntry", entry2), monitor.Calls)
		assert.Call(NewMockCall("OnCopyBlock", blockId2, false, assert.Any), monitor.Calls)
		assert.Call(NewMockCall("OnBeforeUpdateDstHead", rev2Id), monitor.Calls)
		assert.Equal(3, monitor.CountCalls("OnCopyBlock"))
	})

	t.Run("Does not copy duplicate data block twice", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		_, blockHeader, err := src.WriteBlock([]byte("shared"))
		assert.NoError(err)
		entry1 := td.RevisionEntry("a.txt", RevisionEntryAdd)
		entry1.Metadata.BlockIds = []BlockId{blockHeader.BlockId}
		entry1.Metadata.Size = 6
		entry1.Metadata.FileHash = td.SHA256("shared")
		entry2 := td.RevisionEntry("b.txt", RevisionEntryAdd)
		entry2.Metadata.BlockIds = []BlockId{blockHeader.BlockId}
		entry2.Metadata.Size = 6
		entry2.Metadata.FileHash = td.SHA256("shared")
		commit, err := NewCommit(src.Repository, td.NewFS(t))
		assert.NoError(err)
		assert.NoError(commit.Add(entry1))
		assert.NoError(commit.Add(entry2))
		_, err = commit.Commit(td.CommitInfo())
		assert.NoError(err)

		monitor := &TestSyncMonitor{}
		err = SyncRepository(context.Background(), src.Repository, dst.Storage, RepositorySyncOptions{Monitor: monitor})
		assert.NoError(err)

		assert.Equal(3, monitor.CountCalls("OnCopyBlock"))
		assert.Equal(1, monitor.CountBlockCopies(blockHeader.BlockId))
	})

	t.Run("Fails when destination head is not in source history", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		src := td.NewTestRepository(t, td.NewFS(t))
		dst := cloneRepository(t, src)

		entry, _ := testEntry(t, src, "a.txt", "abc")
		_, err := testCommit(t, src.Repository, entry)
		assert.NoError(err)
		assert.NoError(WriteRef(dst.Storage, "head", td.RevisionId("other")))

		monitor := &TestSyncMonitor{}
		err = SyncRepository(context.Background(), src.Repository, dst.Storage, RepositorySyncOptions{Monitor: monitor})
		assert.Error(err, "don't have a common revision")
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
		err = SyncRepository(context.Background(), src.Repository, dst.Storage, RepositorySyncOptions{Monitor: monitor})
		assert.Error(err, "config")
		assert.Calls([]MockCall{}, monitor.Calls)
	})
}

func testEntry(t *testing.T, r *TestRepository, path, content string) (*RevisionEntry, BlockId) {
	t.Helper()
	assert := NewAssert(t)
	entry := td.RevisionEntry(path, RevisionEntryAdd)
	_, blockHeader, err := r.WriteBlock([]byte(content))
	assert.NoError(err)
	entry.Metadata.BlockIds = []BlockId{blockHeader.BlockId}
	entry.Metadata.Size = int64(len(content))
	entry.Metadata.FileHash = td.SHA256(content)
	return entry, blockHeader.BlockId
}

func assertSameHistory(t *testing.T, src, dst *TestRepository) {
	t.Helper()
	assert := NewAssert(t)

	srcRevisionId := src.Head()
	dstRevisionId := dst.Head()
	assert.Equal(srcRevisionId, dstRevisionId)

	for !srcRevisionId.IsRoot() {
		srcRevision, err := src.ReadRevision(srcRevisionId)
		assert.NoError(err)
		dstRevision, err := dst.ReadRevision(dstRevisionId)
		assert.NoError(err)
		assert.Equal(srcRevision, dstRevision)
		assert.Equal(src.RevisionInfos(srcRevisionId), dst.RevisionInfos(dstRevisionId))
		srcRevisionId = srcRevision.Parent
		dstRevisionId = dstRevision.Parent
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

func (m *TestSyncMonitor) OnRevisionStart(revisionId RevisionId) {
	m.Calls = append(m.Calls, NewMockCall("OnRevisionStart", revisionId))
}

func (m *TestSyncMonitor) OnCopyBlock(blockId BlockId, existed bool, length int) {
	m.Calls = append(m.Calls, NewMockCall("OnCopyBlock", blockId, existed, length))
}

func (m *TestSyncMonitor) OnRevisionEntry(entry *RevisionEntry) {
	m.Calls = append(m.Calls, NewMockCall("OnRevisionEntry", entry))
}

func (m *TestSyncMonitor) OnBeforeUpdateDstHead(newHead RevisionId) {
	m.Calls = append(m.Calls, NewMockCall("OnBeforeUpdateDstHead", newHead))
}
