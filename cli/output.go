//nolint:forbidigo
package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
	"github.com/flunderpero/cling-sync/workspace"
	"golang.org/x/term"
)

func IsTerm(f *os.File) bool {
	return term.IsTerminal(int(f.Fd()))
}

func PrintErr(msg string, args ...any) {
	s := "\nError: "
	if IsTerm(os.Stdout) {
		s = fmt.Sprintf("\x1b[31m%s\x1b[0m", s)
	}
	fmt.Fprintf(os.Stderr, s+msg+"\n", args...)
}

type StagingMonitor struct {
	Verbose              bool
	IgnoreErrors         bool
	NoProgress           bool
	WorkspaceDir         string
	paths                int
	excluded             int
	rawBytesAdded        int64
	compressedBytesAdded int64
	totalFileSizes       int64
	errors               int
	ignoredError         bool
	startTime            time.Time
	clearLine            string
}

func NewStagingMonitor(workspaceDir string, verbose, ignoreErrors, noProgress bool) *StagingMonitor {
	clearLine := "\n"
	cols, _, err := term.GetSize(int(os.Stderr.Fd()))
	if err == nil {
		clearLine = fmt.Sprintf("\r%s\r", strings.Repeat(" ", cols))
	}
	return &StagingMonitor{ //nolint:exhaustruct
		Verbose:      verbose,
		IgnoreErrors: ignoreErrors,
		NoProgress:   noProgress,
		WorkspaceDir: workspaceDir,
		clearLine:    clearLine,
	}
}

func (m *StagingMonitor) OnStart(path string, dirEntry os.DirEntry) {
	if m.startTime.IsZero() {
		m.startTime = time.Now()
	}
	m.ignoredError = false
	m.paths++
	m.progress()
	if !m.Verbose {
		return
	}
	path, _ = filepath.Rel(m.WorkspaceDir, path)
	if dirEntry.IsDir() {
		fmt.Printf("Dir  %s\n", path)
		return
	}
	fmt.Printf("File %s\n", path)
}

func (m *StagingMonitor) OnAddBlock(path string, header *lib.BlockHeader, existed bool, dataSize int64) {
	if !existed {
		m.rawBytesAdded += dataSize
		m.compressedBytesAdded += int64(header.EncryptedDataSize) - lib.TotalCipherOverhead
	}
	blockId := header.BlockId
	m.progress()
	if !m.Verbose {
		return
	}
	if existed {
		fmt.Printf("  block (reused)   %s (%s)\n", blockId, formatBytes(dataSize))
		return
	}
	if header.Flags&lib.BlockFlagDeflate == lib.BlockFlagDeflate {
		fmt.Printf(
			"  block (created)  %s (%s, compressed: %.2f)\n",
			blockId,
			formatBytes(dataSize),
			float64(header.EncryptedDataSize-lib.TotalCipherOverhead)/float64(dataSize),
		)
		return
	}
	fmt.Printf("  block (created)  %s (%s)\n", blockId, formatBytes(dataSize))
}

func (m *StagingMonitor) OnError(path string, err error) workspace.StagingOnError {
	m.errors++
	if !m.IgnoreErrors {
		return workspace.StagingOnErrorAbort
	}
	m.ignoredError = true
	if m.isProgress() {
		fmt.Fprintf(os.Stderr, "\r")
	}
	if m.Verbose {
		fmt.Printf("  ignoring error\n    %s\n", strings.ReplaceAll(err.Error(), "\n", "\n    "))
	} else {
		fmt.Printf("Path %s\n  %s\n", path, strings.ReplaceAll(err.Error(), "\n", "\n  "))
	}
	m.progress()
	return workspace.StagingOnErrorIgnore
}

func (m *StagingMonitor) OnEnd(path string, excluded bool, metadata *lib.FileMetadata) {
	if excluded {
		if m.Verbose {
			fmt.Printf("  excluded\n")
		}
		m.excluded++
		m.progress()
		return
	}
	if metadata != nil {
		m.totalFileSizes += metadata.Size
	}
	m.progress()
	if !m.Verbose || m.ignoredError {
		return
	}
	if metadata != nil && metadata.ModeAndPerm.IsDir() {
		fmt.Printf("  included (directory)\n")
		return
	}
	fmt.Printf("  included (%s, %s)\n", formatBytes(metadata.Size), hex.EncodeToString(metadata.FileHash[:]))
}

// Clear the progress line.
func (m *StagingMonitor) Close() {
	if !m.isProgress() {
		return
	}
	fmt.Fprint(os.Stderr, m.clearLine)
}

func (m *StagingMonitor) progress() {
	if !m.isProgress() {
		return
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Paths: %d", (m.paths - m.excluded)))
	if m.excluded > 0 {
		sb.WriteString(fmt.Sprintf(" (+ %d excluded)", m.excluded))
	}
	if m.errors > 0 {
		sb.WriteString(fmt.Sprintf(", %d errors", m.errors))
	}
	mbs := (float64(m.totalFileSizes) / float64(time.Since(m.startTime).Seconds()))
	sb.WriteString(fmt.Sprintf(", %s scanned at %s/s", formatBytes(m.totalFileSizes), formatBytes(int64(mbs))))
	if m.rawBytesAdded > 0 {
		compressed := float64(m.compressedBytesAdded) / float64(m.rawBytesAdded)
		sb.WriteString(fmt.Sprintf(", %s added, compression: %.2f", formatBytes(m.rawBytesAdded), compressed))
	}
	fmt.Fprintf(os.Stderr, "%s\r%s", m.clearLine, sb.String())
}

func (m *StagingMonitor) isProgress() bool {
	return !m.Verbose && !m.NoProgress && IsTerm(os.Stderr)
}

func formatBytes(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%c", float64(b)/float64(div), "KMGTPE"[exp])
}
