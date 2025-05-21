//nolint:forbidigo
package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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
}

func NewStagingMonitor(workspaceDir string, verbose, ignoreErrors, noProgress bool) *StagingMonitor {
	return &StagingMonitor{ //nolint:exhaustruct
		Verbose:      verbose,
		IgnoreErrors: ignoreErrors,
		NoProgress:   noProgress,
		WorkspaceDir: workspaceDir,
	}
}

func (m *StagingMonitor) OnStart(path string, dirEntry os.DirEntry) {
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

func (m *StagingMonitor) OnAddBlock(path string, header *lib.BlockHeader, existed bool, dataSize int) {
	if !existed {
		m.rawBytesAdded += int64(dataSize)
		m.compressedBytesAdded += int64(header.EncryptedDataSize) - lib.TotalCipherOverhead
	}
	blockId := header.BlockId
	m.progress()
	if !m.Verbose {
		return
	}
	if existed {
		fmt.Printf("  block (reused)   %s (%d bytes)\n", blockId, dataSize)
		return
	}
	if header.Flags&lib.BlockFlagDeflate == lib.BlockFlagDeflate {
		fmt.Printf(
			"  block (created)  %s (%d bytes, compressed: %.2f)\n",
			blockId,
			dataSize,
			float64(header.EncryptedDataSize-lib.TotalCipherOverhead)/float64(dataSize),
		)
		return
	}
	fmt.Printf("  block (created)  %s (%d bytes)\n", blockId, dataSize)
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
	fmt.Printf("  included (%d bytes, %s)\n", metadata.Size, hex.EncodeToString(metadata.FileHash[:]))
}

// Clear the progress line.
func (m *StagingMonitor) Close() {
	if !m.isProgress() {
		return
	}
	cols, _, err := term.GetSize(int(os.Stderr.Fd()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n")
	}
	fmt.Fprintf(os.Stderr, "\r%s\r", strings.Repeat(" ", cols))
}

func (m *StagingMonitor) progress() {
	if !m.isProgress() {
		return
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Scanning paths: %d", (m.paths - m.excluded)))
	if m.excluded > 0 {
		sb.WriteString(fmt.Sprintf(" (+ %d excluded)", m.excluded))
	}
	if m.errors > 0 {
		sb.WriteString(fmt.Sprintf(", Errors: %d", m.errors))
	}
	sb.WriteString(fmt.Sprintf(", Total file sizes: %d bytes", m.totalFileSizes))
	if m.rawBytesAdded > 0 {
		sb.WriteString(
			fmt.Sprintf(
				", %d bytes added, total compression factor: %.2f",
				m.rawBytesAdded,
				float64(m.compressedBytesAdded)/float64(m.rawBytesAdded),
			),
		)
	}
	fmt.Fprintf(os.Stderr, "\r%s", sb.String())
}

func (m *StagingMonitor) isProgress() bool {
	return !m.Verbose && !m.NoProgress && IsTerm(os.Stderr)
}
