//nolint:forbidigo
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
	ws "github.com/flunderpero/cling-sync/workspace"
	"golang.org/x/term"
)

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
	cliStagingMonitor     struct{ *ws.DefaultStagingMonitor }
	cliCommitMonitor      struct{ *ws.DefaultCommitMonitor }
	cliHealthCheckMonitor struct{ *ws.DefaultHealthCheckMonitor }
)

type cliCpMonitor struct {
	*ws.DefaultCpMonitor
	emitPlain bool
}

type cliSyncRepoMonitor struct {
	*ws.DefaultSyncRepoMonitor
	emitPlain bool
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

func NewResetMonitors(mode ws.DefaultMonitorMode) (*cliStagingMonitor, *cliCpMonitor) {
	return NewStatusMonitor(mode), NewCpMonitor(mode, ws.CpOnExistsAbort, false)
}

func NewMergeMonitors(mode ws.DefaultMonitorMode) (*cliStagingMonitor, *cliCpMonitor, *cliCommitMonitor) {
	staging := NewStatusMonitor(mode)
	cp := NewCpMonitor(mode, ws.CpOnExistsAbort, false)
	commit := &cliCommitMonitor{DefaultCommitMonitor: nil}
	commit.DefaultCommitMonitor = ws.NewDefaultCommitMonitor(mode, nil, commit.emit)
	return staging, cp, commit
}

func NewHeathCheckMonitor(mode ws.DefaultMonitorMode) *cliHealthCheckMonitor {
	monitor := &cliHealthCheckMonitor{DefaultHealthCheckMonitor: nil}
	monitor.DefaultHealthCheckMonitor = ws.NewDefaultHealthCheckMonitor(mode, monitor.emit)
	return monitor
}

func NewSyncRepoMonitor(mode ws.DefaultMonitorMode) *cliSyncRepoMonitor {
	monitor := &cliSyncRepoMonitor{DefaultSyncRepoMonitor: nil, emitPlain: false}
	monitor.DefaultSyncRepoMonitor = ws.NewDefaultSyncRepoMonitor(mode, monitor.emit)
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

func (m *cliSyncRepoMonitor) OnBeforeUpdateDstHead(newHead lib.RevisionId) {
	m.emitPlain = true
	defer func() { m.emitPlain = false }()
	m.DefaultSyncRepoMonitor.OnBeforeUpdateDstHead(newHead)
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

func (m *cliSyncRepoMonitor) close() {
	clearLineIfProgress(m.Mode)
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
