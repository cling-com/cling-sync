//go:build !wasm

package workspace

import (
	"io/fs"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	td   = lib.TestData{}      //nolint:gochecknoglobals
	wstd = WorkspaceTestData{} //nolint:gochecknoglobals
)

type WorkspaceTestData struct{}

func (wstd WorkspaceTestData) NewTestWorkspace(tb testing.TB, repository *lib.Repository) *TestWorkspace {
	tb.Helper()
	return wstd.NewTestWorkspaceWithPathPrefix(tb, repository, "")
}

func (wstd WorkspaceTestData) NewTestWorkspaceWithPathPrefix(
	tb testing.TB,
	repository *lib.Repository,
	pathPrefix string,
) *TestWorkspace {
	tb.Helper()
	assert := lib.NewAssert(tb)
	fs := td.NewFS(tb)
	prefix, err := ValidatePathPrefix(pathPrefix)
	assert.NoError(err)
	workspace, err := NewWorkspace(fs, td.NewFS(tb), RemoteRepository("test"), prefix)
	assert.NoError(err)
	return &TestWorkspace{workspace, td.NewTestFS(tb, fs), tb, assert}
}

func (wstd WorkspaceTestData) CpMonitor() *TestCpMonitor {
	return &TestCpMonitor{CpOnExistsAbort}
}

func (wstd WorkspaceTestData) CpMonitorOverwrite() *TestCpMonitor {
	return &TestCpMonitor{CpOnExistsOverwrite}
}

func (wstd WorkspaceTestData) StagingMonitor() *TestStagingMonitor {
	return &TestStagingMonitor{}
}

func (wstd WorkspaceTestData) CommitMonitor() *TestCommitMonitor {
	return &TestCommitMonitor{}
}

func (wstd WorkspaceTestData) StatusOptions() *StatusOptions {
	return &StatusOptions{nil, wstd.StagingMonitor()}
}

func (wstd WorkspaceTestData) MergeOptions() *MergeOptions {
	return &MergeOptions{wstd.StagingMonitor(), wstd.CpMonitor(), wstd.CommitMonitor(), "author", "message", true}
}

func (wstd WorkspaceTestData) LsOptions(revisionId lib.RevisionId) *LsOptions {
	return &LsOptions{RevisionId: revisionId} //nolint:exhaustruct
}

type TestCpMonitor struct {
	Exists CpOnExists
}

func (m *TestCpMonitor) OnStart(entry *lib.RevisionEntry, targetPath string) {
}

func (m *TestCpMonitor) OnWrite(entry *lib.RevisionEntry, targetPath string, blockId lib.BlockId, data []byte) {
}

func (m *TestCpMonitor) OnEnd(entry *lib.RevisionEntry, targetPath string) {
}

func (m *TestCpMonitor) OnError(entry *lib.RevisionEntry, targetPath string, err error) CpOnError {
	return CpOnErrorAbort
}

func (m *TestCpMonitor) OnExists(entry *lib.RevisionEntry, targetPath string) CpOnExists {
	return m.Exists
}

type TestStagingMonitor struct{}

func (m *TestStagingMonitor) OnStart(path lib.Path, dirEntry fs.DirEntry) {
}

func (m *TestStagingMonitor) OnEnd(path lib.Path, excluded bool, metadata *lib.FileMetadata) {
}

func (m *TestStagingMonitor) Close() {
}

type TestCommitMonitor struct{}

func (m *TestCommitMonitor) OnBeforeCommit() error {
	return nil
}

func (m *TestCommitMonitor) OnStart(entry *lib.RevisionEntry) {
}

func (m *TestCommitMonitor) OnAddBlock(
	entry *lib.RevisionEntry,
	header *lib.BlockHeader,
	existed bool,
	dataSize int64,
) {
}

func (m *TestCommitMonitor) OnEnd(entry *lib.RevisionEntry) {}

type TestWorkspace struct {
	*Workspace
	*lib.TestFS
	t      testing.TB
	assert lib.Assert
}

func (w *TestWorkspace) Head() lib.RevisionId {
	w.t.Helper()
	head, err := w.Workspace.Head()
	w.assert.NoError(err)
	return head
}
