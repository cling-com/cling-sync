//go:build !wasm

package workspace

import (
	"errors"
	"io"
	"io/fs"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	td   = lib.TestData{}      //nolint:gochecknoglobals
	wstd = WorkspaceTestData{} //nolint:gochecknoglobals
)

type TestStagingEntryInfo struct {
	Path string
	Mode fs.FileMode
	Hash lib.Sha256
}

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
	return wstd.NewTestWorkspaceExtra(tb, repository, pathPrefix, td.NewFS(tb))
}

func (wstd WorkspaceTestData) NewTestWorkspaceExtra(
	tb testing.TB,
	repository *lib.Repository,
	pathPrefix string,
	fs lib.FS,
) *TestWorkspace {
	tb.Helper()
	assert := lib.NewAssert(tb)
	prefix, err := ValidatePathPrefix(pathPrefix)
	assert.NoError(err)
	workspace, err := NewWorkspace(fs, td.NewFS(tb), RemoteRepository("test"), prefix)
	assert.NoError(err)
	return &TestWorkspace{workspace, td.NewTestFS(tb, fs), tb, assert}
}

func (wstd WorkspaceTestData) CpMonitor() *TestCpMonitor {
	return NewTestCpMonitor(CpOnExistsAbort)
}

func (wstd WorkspaceTestData) CpMonitorOverwrite() *TestCpMonitor {
	return NewTestCpMonitor(CpOnExistsOverwrite)
}

func (wstd WorkspaceTestData) StagingMonitor() *TestStagingMonitor {
	return &TestStagingMonitor{}
}

func (wstd WorkspaceTestData) CommitMonitor() *TestCommitMonitor {
	return &TestCommitMonitor{}
}

func (wstd WorkspaceTestData) StatusOptions() *StatusOptions {
	return &StatusOptions{nil, wstd.StagingMonitor(), lib.RestorableMetadataAll, false}
}

func (wstd WorkspaceTestData) MergeOptions() *MergeOptions {
	return &MergeOptions{
		wstd.StagingMonitor(),
		wstd.CpMonitor(),
		wstd.CommitMonitor(),
		"author",
		"message",
		lib.RestorableMetadataAll,
		false,
	}
}

func (wstd WorkspaceTestData) LsOptions(revisionId lib.RevisionId) *LsOptions {
	return &LsOptions{RevisionId: revisionId} //nolint:exhaustruct
}

func (wstd WorkspaceTestData) CpOptions(revisionId lib.RevisionId) *CpOptions {
	return &CpOptions{
		revisionId,
		wstd.CpMonitor(),
		nil,
		lib.RestorableMetadataAll,
	}
}

func (wstd WorkspaceTestData) StagingEntryInfos(temp *lib.Temp[StagingEntry]) []TestStagingEntryInfo {
	infos := []TestStagingEntryInfo{}
	r := temp.Reader(nil)
	for {
		entry, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		infos = append(infos, TestStagingEntryInfo{
			Path: entry.RepoPath.String(),
			Mode: entry.Metadata.ModeAndPerm.AsFileMode(),
			Hash: entry.Metadata.FileHash,
		})
	}
	return infos
}

type TestCpMonitor struct {
	Exists        CpOnExists
	OnStartCalls  []*lib.RevisionEntry
	OnWriteCalls  []*lib.RevisionEntry
	OnExistsCalls []*lib.RevisionEntry
	OnEndCalls    []*lib.RevisionEntry
	OnErrorCalls  []*lib.RevisionEntry
}

func NewTestCpMonitor(exists CpOnExists) *TestCpMonitor {
	return &TestCpMonitor{Exists: exists} //nolint:exhaustruct
}

func (m *TestCpMonitor) OnStart(entry *lib.RevisionEntry, targetPath string) {
	m.OnStartCalls = append(m.OnStartCalls, entry)
}

func (m *TestCpMonitor) OnWrite(entry *lib.RevisionEntry, targetPath string, blockId lib.BlockId, data []byte) {
	m.OnWriteCalls = append(m.OnWriteCalls, entry)
}

func (m *TestCpMonitor) OnEnd(entry *lib.RevisionEntry, targetPath string) {
	m.OnEndCalls = append(m.OnEndCalls, entry)
}

func (m *TestCpMonitor) OnError(entry *lib.RevisionEntry, targetPath string, err error) CpOnError {
	m.OnErrorCalls = append(m.OnErrorCalls, entry)
	return CpOnErrorAbort
}

func (m *TestCpMonitor) OnExists(entry *lib.RevisionEntry, targetPath string) CpOnExists {
	m.OnExistsCalls = append(m.OnExistsCalls, entry)
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
