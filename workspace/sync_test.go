//go:build !wasm

//nolint:exhaustruct
package workspace

import (
	"os"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestValidateSyncTargetName(t *testing.T) {
	t.Parallel()
	assert := lib.NewAssert(t)

	for _, name := range []string{"a", "backup-1", "Z9", "x-y-z"} {
		assert.NoError(ValidateSyncTargetName(name), name)
	}
	for _, name := range []string{"", "with space", "with_underscore", "ünïcode", "a/b", "."} {
		err := ValidateSyncTargetName(name)
		assert.Error(err, name)
	}
}

func TestSyncTargetStore(t *testing.T) {
	t.Parallel()

	t.Run("Empty when nothing registered", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		w := newSyncTestWorkspace(t)

		targets, err := LoadSyncTargets(t.Context(), w)
		assert.NoError(err)
		assert.Equal(0, len(targets))
	})

	t.Run("Add, load, get, delete round trip", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srcPath := t.TempDir()
		src := td.NewTestRepository(t, lib.NewRealFS(srcPath))
		w, err := NewWorkspace(t.Context(), td.NewFS(t), td.NewFS(t), RemoteRepository(srcPath), lib.Path{})
		assert.NoError(err)
		alpha := cloneRepositoryAt(t, src)
		beta := cloneRepositoryAt(t, src)

		assert.NoError(AddSyncTarget(t.Context(), w, "alpha", alpha, nil))
		assert.NoError(AddSyncTarget(t.Context(), w, "beta", beta, nil))

		targets, err := LoadSyncTargets(t.Context(), w)
		assert.NoError(err)
		assert.Equal([]SyncTarget{
			{Name: "alpha", URI: alpha},
			{Name: "beta", URI: beta},
		}, targets)

		uri, found, err := GetSyncTarget(t.Context(), w, "alpha")
		assert.NoError(err)
		assert.Equal(true, found)
		assert.Equal(alpha, uri)

		_, found, err = GetSyncTarget(t.Context(), w, "ghost")
		assert.NoError(err)
		assert.Equal(false, found)

		assert.NoError(DeleteSyncTarget(t.Context(), w, "alpha"))
		targets, err = LoadSyncTargets(t.Context(), w)
		assert.NoError(err)
		assert.Equal([]SyncTarget{{Name: "beta", URI: beta}}, targets)
	})

	t.Run("Add rejects invalid name", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srcPath := t.TempDir()
		src := td.NewTestRepository(t, lib.NewRealFS(srcPath))
		w, err := NewWorkspace(t.Context(), td.NewFS(t), td.NewFS(t), RemoteRepository(srcPath), lib.Path{})
		assert.NoError(err)
		alpha := cloneRepositoryAt(t, src)
		assert.Error(AddSyncTarget(t.Context(), w, "with space", alpha, nil), "alphanumeric")
	})

	t.Run("Add rejects duplicate name", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srcPath := t.TempDir()
		src := td.NewTestRepository(t, lib.NewRealFS(srcPath))
		w, err := NewWorkspace(t.Context(), td.NewFS(t), td.NewFS(t), RemoteRepository(srcPath), lib.Path{})
		assert.NoError(err)
		alpha := cloneRepositoryAt(t, src)
		alpha2 := cloneRepositoryAt(t, src)
		assert.NoError(AddSyncTarget(t.Context(), w, "alpha", alpha, nil))
		assert.Error(AddSyncTarget(t.Context(), w, "alpha", alpha2, nil), "already exists")
	})

	t.Run("Add rejects mismatched repository config", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		w := newSyncTestWorkspace(t)
		// A repository with its own unrelated config.
		otherPath := t.TempDir()
		td.NewTestRepository(t, lib.NewRealFS(otherPath))
		assert.Error(AddSyncTarget(t.Context(), w, "other", otherPath, nil), "same configuration")
	})

	t.Run("Add rejects unreachable URI", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		w := newSyncTestWorkspace(t)
		assert.Error(AddSyncTarget(t.Context(), w, "ghost", "/nonexistent-cling-sync-target", nil), "storage not found")
	})

	t.Run("Delete rejects unknown name", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		w := newSyncTestWorkspace(t)
		assert.Error(DeleteSyncTarget(t.Context(), w, "ghost"), "does not exist")
	})
}

func TestRunSync(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srcPath := t.TempDir()
		src := td.NewTestRepository(t, lib.NewRealFS(srcPath))
		w, err := NewWorkspace(t.Context(), td.NewFS(t), td.NewFS(t), RemoteRepository(srcPath), lib.Path{})
		assert.NoError(err)
		dstPath := cloneRepositoryAt(t, src)
		assert.NoError(AddSyncTarget(t.Context(), w, "one", dstPath, nil))

		entry := td.RevisionEntry("a.txt", lib.RevisionEntryKindAdd)
		blockId, _, err := src.WriteBlock(t.Context(), []byte("hello"), lib.NewBlockBuf())
		assert.NoError(err)
		entry.Metadata.BlockIds = []lib.BlockId{blockId}
		entry.Metadata.Size = 5
		entry.Metadata.FileHash = td.SHA256("hello")
		commit, err := lib.NewCommit(t.Context(), src.Repository, td.NewFS(t))
		assert.NoError(err)
		assert.NoError(commit.Add(entry))
		srcHead, err := commit.Commit(t.Context(), td.CommitInfo())
		assert.NoError(err)

		mon := &countingMonitor{}
		assert.NoError(RunSync(t.Context(), w, "one", nil, td.RevisionChain(t, src), RunSyncOpts{
			Monitor: mon, Workers: 8,
		}))

		dstStorage, err := lib.NewFileStorage(lib.NewRealFS(dstPath), lib.StoragePurposeRepository)
		assert.NoError(err)
		dstHead, err := lib.ReadRef(t.Context(), dstStorage, "head")
		assert.NoError(err)
		assert.Equal(srcHead, dstHead)
		assert.Greater(mon.blocks, 0, "monitor should see blocks copied")
	})

	t.Run("Unknown name", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		w := newSyncTestWorkspace(t)
		mon := &countingMonitor{}
		err := RunSync(
			t.Context(),
			w,
			"ghost",
			nil,
			nil,
			RunSyncOpts{Monitor: mon, Workers: 8, SkipHeadCheck: true},
		)
		assert.Error(err, `no sync target named "ghost"`)
	})

	t.Run("Target storage removed between add and run", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srcPath := t.TempDir()
		src := td.NewTestRepository(t, lib.NewRealFS(srcPath))
		w, err := NewWorkspace(t.Context(), td.NewFS(t), td.NewFS(t), RemoteRepository(srcPath), lib.Path{})
		assert.NoError(err)
		dstPath := cloneRepositoryAt(t, src)
		assert.NoError(AddSyncTarget(t.Context(), w, "one", dstPath, nil))
		assert.NoError(os.RemoveAll(dstPath)) //nolint:forbidigo

		mon := &countingMonitor{}
		err = RunSync(
			t.Context(),
			w,
			"one",
			nil,
			nil,
			RunSyncOpts{Monitor: mon, Workers: 8, SkipHeadCheck: true},
		)
		assert.Error(err, "storage not found")
	})

	t.Run("Target head outside the source chain should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srcPath := t.TempDir()
		src := td.NewTestRepository(t, lib.NewRealFS(srcPath))
		w, err := NewWorkspace(t.Context(), td.NewFS(t), td.NewFS(t), RemoteRepository(srcPath), lib.Path{})
		assert.NoError(err)
		dstPath := cloneRepositoryAt(t, src)
		assert.NoError(AddSyncTarget(t.Context(), w, "one", dstPath, nil))

		commitFile(t, src, "hello")
		// The target has its own divergent revision, so its head is not part
		// of the source's chain.
		dst := td.OpenRepository(t, lib.NewRealFS(dstPath))
		commitFile(t, dst, "world")

		mon := &countingMonitor{}
		err = RunSync(
			t.Context(),
			w,
			"one",
			nil,
			td.RevisionChain(t, src),
			RunSyncOpts{Monitor: mon, Workers: 8},
		)
		assert.Error(err, "is not in src's revision chain")
	})
}

func newSyncTestWorkspace(t *testing.T) *Workspace {
	t.Helper()
	assert := lib.NewAssert(t)
	srcPath := t.TempDir()
	td.NewTestRepository(t, lib.NewRealFS(srcPath))
	w, err := NewWorkspace(t.Context(), td.NewFS(t), td.NewFS(t), RemoteRepository(srcPath), lib.Path{})
	assert.NoError(err)
	return w
}

func cloneRepositoryAt(t *testing.T, src *lib.TestRepository) string {
	t.Helper()
	assert := lib.NewAssert(t)
	dstPath := t.TempDir()
	dst := td.NewTestRepository(t, lib.NewRealFS(dstPath))
	configData, err := lib.ReadFile(src.FS, ".cling/repository.txt")
	assert.NoError(err)
	assert.NoError(lib.WriteFile(dst.FS, ".cling/repository.txt", configData))
	return dstPath
}

// commitFile commits a single file with `content` to the repository and
// returns the new head revision.
func commitFile(t *testing.T, repo *lib.TestRepository, content string) lib.RevisionId {
	t.Helper()
	assert := lib.NewAssert(t)
	entry := td.RevisionEntry("a.txt", lib.RevisionEntryKindAdd)
	blockId, _, err := repo.WriteBlock(t.Context(), []byte(content), lib.NewBlockBuf())
	assert.NoError(err)
	entry.Metadata.BlockIds = []lib.BlockId{blockId}
	entry.Metadata.Size = int64(len(content))
	entry.Metadata.FileHash = td.SHA256(content)
	commit, err := lib.NewCommit(t.Context(), repo.Repository, td.NewFS(t))
	assert.NoError(err)
	assert.NoError(commit.Add(entry))
	head, err := commit.Commit(t.Context(), td.CommitInfo())
	assert.NoError(err)
	return head
}

type countingMonitor struct {
	blocks int
}

func (m *countingMonitor) OnSrcBlockIdsRead(int) {}

func (m *countingMonitor) OnDstBlockIdsRead(int) {}

func (m *countingMonitor) OnBeforeCopy(int, int) {}

func (m *countingMonitor) OnCopyBlock(_ lib.BlockId, existed bool, _ int) {
	if !existed {
		m.blocks++
	}
}

func (m *countingMonitor) OnBeforeUpdateDstHead(lib.RevisionId) {}
