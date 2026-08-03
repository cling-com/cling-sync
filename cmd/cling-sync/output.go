//nolint:forbidigo
package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/cling-com/cling-sync/lib"
	ws "github.com/cling-com/cling-sync/workspace"
	"golang.org/x/term"
)

// Ask the user to confirm an action. Anything but `y` or `yes` is a no.
func Confirm(question string) (bool, error) {
	if !IsTerm(os.Stdin) {
		return false, lib.Errorf("cannot ask for confirmation because stdin is not a terminal, use --yes")
	}
	fmt.Printf("%s [y/N] ", question)
	answer, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to read answer")
	}
	answer = strings.ToLower(strings.TrimSpace(answer))
	return answer == "y" || answer == "yes", nil
}

func IsTerm(f *os.File) bool {
	return term.IsTerminal(int(f.Fd())) //nolint:gosec
}

func PrintErr(msg string, args ...any) {
	s := "\nError: "
	if IsTerm(os.Stdout) {
		s = fmt.Sprintf("\x1b[31m%s\x1b[0m", s)
	}
	fmt.Fprintf(os.Stderr, s+msg+"\n", args...)
}

func CLIMonitorMode(verbose, noProgress bool) ws.DefaultMonitorMode {
	switch {
	case verbose:
		return ws.DefaultMonitorModeVerbose
	case noProgress:
		return ws.DefaultMonitorModeSilent
	default:
		return ws.DefaultMonitorModeProgress
	}
}

type (
	cliStagingMonitor       struct{ *ws.DefaultStagingMonitor }
	cliImportStagingMonitor struct{ *cliStagingMonitor }
	cliCommitMonitor        struct{ *ws.DefaultCommitMonitor }
	cliHealthCheckMonitor   struct{ *ws.DefaultHealthCheckMonitor }
	cliSnapshotMonitor      struct {
		*ws.DefaultRevisionSnapshotMonitor
	}
)

type cliCpMonitor struct {
	*ws.DefaultCpMonitor
	emitPlain bool
}

type cliSyncRepoMonitor struct {
	*ws.DefaultSyncRepoMonitor
	targetName string
	emitPlain  bool
}

func NewCpMonitor(mode ws.DefaultMonitorMode, cpOnExists ws.CpOnExists, ignoreErrors bool) *cliCpMonitor {
	monitor := &cliCpMonitor{DefaultCpMonitor: nil, emitPlain: false}
	monitor.DefaultCpMonitor = ws.NewDefaultCpMonitor(mode, nil, monitor.emit, cpOnExists, ignoreErrors)
	return monitor
}

func NewStatusMonitor(mode ws.DefaultMonitorMode) *cliStagingMonitor {
	monitor := &cliStagingMonitor{DefaultStagingMonitor: nil}
	monitor.DefaultStagingMonitor = ws.NewDefaultStagingMonitor(mode, nil, monitor.emit)
	return monitor
}

// Every command that reads a revision snapshot needs one: building the snapshot
// runs before any other monitor reports anything.
func NewSnapshotMonitor(mode ws.DefaultMonitorMode) *cliSnapshotMonitor {
	monitor := &cliSnapshotMonitor{DefaultRevisionSnapshotMonitor: nil}
	monitor.DefaultRevisionSnapshotMonitor = ws.NewDefaultRevisionSnapshotMonitor(mode, monitor.emit)
	return monitor
}

func NewResetMonitors(mode ws.DefaultMonitorMode) (*cliSnapshotMonitor, *cliStagingMonitor, *cliCpMonitor) {
	return NewSnapshotMonitor(mode), NewStatusMonitor(mode), NewCpMonitor(mode, ws.CpOnExistsAbort, false)
}

func NewMergeMonitors(
	mode ws.DefaultMonitorMode,
) (*cliSnapshotMonitor, *cliStagingMonitor, *cliCpMonitor, *cliCommitMonitor) {
	snapshot := NewSnapshotMonitor(mode)
	staging := NewStatusMonitor(mode)
	cp := NewCpMonitor(mode, ws.CpOnExistsAbort, false)
	commit := &cliCommitMonitor{DefaultCommitMonitor: nil}
	commit.DefaultCommitMonitor = ws.NewDefaultCommitMonitor(mode, nil, commit.emit)
	return snapshot, staging, cp, commit
}

func NewImportMonitors(mode ws.DefaultMonitorMode) (*cliSnapshotMonitor, *cliImportStagingMonitor, *cliCommitMonitor) {
	staging := &cliImportStagingMonitor{cliStagingMonitor: NewStatusMonitor(mode)}
	commit := &cliCommitMonitor{DefaultCommitMonitor: nil}
	commit.DefaultCommitMonitor = ws.NewDefaultCommitMonitor(mode, nil, commit.emit)
	return NewSnapshotMonitor(mode), staging, commit
}

// A symlink target is only meaningful relative to the workspace it lives in,
// and an imported directory is not one.
func (m *cliImportStagingMonitor) OnStart(path lib.Path, dirEntry fs.DirEntry) error {
	if dirEntry.Type()&fs.ModeSymlink != 0 {
		return lib.Errorf("cannot import symlink %s, exclude it or import its target instead", path)
	}
	return m.cliStagingMonitor.OnStart(path, dirEntry) //nolint:wrapcheck
}

func NewHeathCheckMonitor(mode ws.DefaultMonitorMode) *cliHealthCheckMonitor {
	monitor := &cliHealthCheckMonitor{DefaultHealthCheckMonitor: nil}
	monitor.DefaultHealthCheckMonitor = ws.NewDefaultHealthCheckMonitor(mode, monitor.emit)
	return monitor
}

func NewSyncRepoMonitor(targetName string, mode ws.DefaultMonitorMode) *cliSyncRepoMonitor {
	monitor := &cliSyncRepoMonitor{DefaultSyncRepoMonitor: nil, targetName: targetName, emitPlain: false}
	monitor.DefaultSyncRepoMonitor = ws.NewDefaultSyncRepoMonitor(mode, monitor.emit, targetName)
	return monitor
}

func (m *cliCpMonitor) OnError(entry *lib.RevisionEntry, targetPath string, err error) ws.CpOnError {
	m.emitPlain = true
	defer func() { m.emitPlain = false }()
	return m.DefaultCpMonitor.OnError(entry, targetPath, err)
}

func (m *cliCpMonitor) emit(text string) {
	if m.Mode == ws.DefaultMonitorModeProgress && !m.emitPlain {
		clearLine()
		fmt.Fprintf(os.Stderr, "\r%s", text)
		return
	}
	clearLineIfProgress(m.Mode)
	fmt.Printf("%s\n", text)
}

func (m *cliCpMonitor) close() {
	clearLineIfProgress(m.Mode)
}

func (m *cliStagingMonitor) emit(text string) {
	if m.Mode == ws.DefaultMonitorModeProgress {
		clearLine()
		fmt.Fprintf(os.Stderr, "\r%s", text)
		return
	}
	fmt.Printf("%s\n", text)
}

func (m *cliStagingMonitor) close() {
	clearLineIfProgress(m.Mode)
}

func (m *cliCommitMonitor) emit(text string) {
	if m.Mode == ws.DefaultMonitorModeProgress {
		clearLine()
		fmt.Fprintf(os.Stderr, "\r%s", text)
		return
	}
	fmt.Printf("%s\n", text)
}

func (m *cliCommitMonitor) close() {
	clearLineIfProgress(m.Mode)
}

func (m *cliSnapshotMonitor) emit(text string) {
	if m.Mode == ws.DefaultMonitorModeProgress {
		clearLine()
		fmt.Fprintf(os.Stderr, "\r%s", text)
		return
	}
	fmt.Printf("%s\n", text)
}

func (m *cliSnapshotMonitor) close() {
	clearLineIfProgress(m.Mode)
}

func (m *cliHealthCheckMonitor) emit(text string) {
	if m.Mode == ws.DefaultMonitorModeProgress {
		clearLine()
		fmt.Fprintf(os.Stderr, "\r%s", text)
		return
	}
	fmt.Printf("%s\n", text)
}

func (m *cliHealthCheckMonitor) close() {
	clearLineIfProgress(m.Mode)
}

func (m *cliSyncRepoMonitor) OnBeforeCopy(srcBlocks, dstBlocks int) {
	m.emitPlain = true
	defer func() { m.emitPlain = false }()
	m.DefaultSyncRepoMonitor.OnBeforeCopy(srcBlocks, dstBlocks)
}

func (m *cliSyncRepoMonitor) OnBeforeUpdateDstHead(newHead lib.RevisionId) {
	m.emitPlain = true
	defer func() { m.emitPlain = false }()
	m.DefaultSyncRepoMonitor.OnBeforeUpdateDstHead(newHead)
}

func (m *cliSyncRepoMonitor) done(err error) {
	m.emitPlain = true
	defer func() { m.emitPlain = false }()
	if err != nil {
		m.emit(fmt.Sprintf("%s: failed to sync: %s", m.targetName, err))
		return
	}
	m.emit(fmt.Sprintf("%s: synced %d blocks", m.targetName, m.Blocks))
}

func (m *cliSyncRepoMonitor) emit(text string) {
	if m.Mode == ws.DefaultMonitorModeProgress && !m.emitPlain {
		clearLine()
		fmt.Fprintf(os.Stderr, "\r%s", text)
		return
	}
	clearLineIfProgress(m.Mode)
	fmt.Printf("%s\n", text)
}

func clearLineIfProgress(mode ws.DefaultMonitorMode) {
	if mode != ws.DefaultMonitorModeProgress {
		return
	}
	clearLine()
}

func clearLine() {
	cols, _, err := term.GetSize(int(os.Stderr.Fd())) //nolint:gosec
	if err != nil {
		fmt.Fprint(os.Stderr, "\n")
		return
	}
	fmt.Fprint(os.Stderr, "\r"+strings.Repeat(" ", cols)+"\r")
}
