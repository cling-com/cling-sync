//nolint:forbidigo
package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
	ws "github.com/flunderpero/cling-sync/workspace"
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

type CommitMonitor struct {
	Verbose              bool
	NoProgress           bool
	startTime            time.Time
	paths                int
	rawBytesAdded        int64
	compressedBytesAdded int64
	rawBytesReused       int64
}

func NewCommitMonitor(verbose, noProgress bool) *CommitMonitor {
	return &CommitMonitor{ //nolint:exhaustruct
		Verbose:    verbose,
		NoProgress: noProgress,
	}
}

func (m *CommitMonitor) OnBeforeCommit() error {
	if m.Verbose {
		fmt.Println("Committing")
	}
	return nil
}

func (m *CommitMonitor) OnStart(entry *lib.RevisionEntry) {
	if m.startTime.IsZero() {
		m.startTime = time.Now()
	}
	m.paths += 1
	m.progress()
	if !m.Verbose {
		return
	}
	fmt.Printf("%s\n", entry.Path.FSString())
}

func (m *CommitMonitor) OnAddBlock(entry *lib.RevisionEntry, header *lib.BlockHeader, existed bool, dataSize int64) {
	if existed {
		m.rawBytesReused += dataSize
	} else {
		m.rawBytesAdded += dataSize
		m.compressedBytesAdded += int64(header.EncryptedDataSize) - lib.TotalCipherOverhead
	}
	m.progress()
	if !m.Verbose {
		return
	}
	if existed {
		fmt.Printf("  block  %s %6s (old)\n", header.BlockId, ws.FormatBytes(dataSize))
		return
	}
	if header.Flags&lib.BlockFlagDeflate == lib.BlockFlagDeflate {
		fmt.Printf(
			"  block  %s %6s (new) (compressed: %.2f)\n",
			header.BlockId,
			ws.FormatBytes(dataSize),
			float64(header.EncryptedDataSize-lib.TotalCipherOverhead)/float64(dataSize),
		)
		return
	}
	fmt.Printf("  block  %s %6s (new)\n", header.BlockId, ws.FormatBytes(dataSize))
}

func (m *CommitMonitor) OnEnd(entry *lib.RevisionEntry) {
	m.progress()
	if !m.Verbose {
		return
	}
	if entry.Metadata.ModeAndPerm.IsDir() {
		fmt.Printf("  %-6s (directory)\n", entry.Type)
		return
	}
	fmt.Printf(
		"  %-6s %s %6s\n",
		entry.Type,
		hex.EncodeToString(entry.Metadata.FileHash[:]),
		ws.FormatBytes(entry.Metadata.Size),
	)
}

func (m *CommitMonitor) progress() {
	if m.Verbose || m.NoProgress || !IsTerm(os.Stderr) {
		return
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("adding %d paths", m.paths))
	mbs := (float64(m.rawBytesAdded) / float64(time.Since(m.startTime).Seconds()))
	sb.WriteString(fmt.Sprintf(" (%s at %s/s)", ws.FormatBytes(m.rawBytesAdded), ws.FormatBytes(int64(mbs))))
	clearLine()
	fmt.Fprintf(os.Stderr, "\r%s", sb.String())
}

type StagingMonitor struct {
	Verbose        bool
	NoProgress     bool
	paths          int
	excluded       int
	totalFileSizes int64
	startTime      time.Time
}

func NewStagingMonitor(verbose, noProgress bool) *StagingMonitor {
	return &StagingMonitor{ //nolint:exhaustruct
		Verbose:    verbose,
		NoProgress: noProgress,
	}
}

func (m *StagingMonitor) OnStart(path string, dirEntry os.DirEntry) {
	if m.startTime.IsZero() {
		m.startTime = time.Now()
	}
	m.paths++
	m.progress()
	if !m.Verbose {
		return
	}
	fmt.Printf("%s\n", path)
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
	if !m.Verbose {
		return
	}
	if metadata != nil && metadata.ModeAndPerm.IsDir() {
		fmt.Printf("  done  (directory)\n")
		return
	}
	fmt.Printf("  done  %s %6s\n", hex.EncodeToString(metadata.FileHash[:]), ws.FormatBytes(metadata.Size))
}

// Clear the progress line.
func (m *StagingMonitor) Close() {
	if !m.isProgress() {
		return
	}
	clearLine()
}

func (m *StagingMonitor) progress() {
	if !m.isProgress() {
		return
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("scanned %d paths", (m.paths - m.excluded)))
	if m.excluded > 0 {
		sb.WriteString(fmt.Sprintf(" (%d excluded)", m.excluded))
	}
	mbs := (float64(m.totalFileSizes) / float64(time.Since(m.startTime).Seconds()))
	sb.WriteString(fmt.Sprintf(" (%s at %s/s)", ws.FormatBytes(m.totalFileSizes), ws.FormatBytes(int64(mbs))))
	clearLine()
	fmt.Fprintf(os.Stderr, "\r%s", sb.String())
}

func (m *StagingMonitor) isProgress() bool {
	return !m.Verbose && !m.NoProgress && IsTerm(os.Stderr)
}

type CpMonitor struct {
	Verbose      bool
	IgnoreErrors bool
	NoProgress   bool
	cpOnExists   ws.CpOnExists
	paths        int
	excluded     int
	bytesWritten int64
	errors       int
	ignoredError bool
	startTime    time.Time
}

func NewCpMonitor(cpOnExists ws.CpOnExists, verbose, ignoreErrors, noProgress bool) *CpMonitor {
	return &CpMonitor{ //nolint:exhaustruct
		Verbose:      verbose,
		IgnoreErrors: ignoreErrors,
		NoProgress:   noProgress,
		cpOnExists:   cpOnExists,
	}
}

func (m *CpMonitor) OnStart(entry *lib.RevisionEntry, targetPath string) {
	if m.startTime.IsZero() {
		m.startTime = time.Now()
	}
	m.ignoredError = false
	m.paths++
	m.progress()
	if !m.Verbose {
		return
	}
	fmt.Printf("%s\n", targetPath)
}

func (m *CpMonitor) OnExists(entry *lib.RevisionEntry, targetPath string) ws.CpOnExists {
	if m.Verbose {
		if m.cpOnExists == ws.CpOnExistsIgnore {
			fmt.Printf("  skipping existing\n")
		}
	}
	return m.cpOnExists
}

func (m *CpMonitor) OnError(entry *lib.RevisionEntry, targetPath string, err error) ws.CpOnError {
	m.errors++
	if !m.IgnoreErrors {
		return ws.CpOnErrorAbort
	}
	m.ignoredError = true
	if m.isProgress() {
		fmt.Fprintf(os.Stderr, "\r")
	}
	if m.Verbose {
		fmt.Printf("  ignoring error\n    %s\n", strings.ReplaceAll(err.Error(), "\n", "\n    "))
	} else {
		fmt.Printf("%s\n  %s\n", targetPath, strings.ReplaceAll(err.Error(), "\n", "\n  "))
	}
	m.progress()
	return ws.CpOnErrorIgnore
}

func (m *CpMonitor) OnWrite(entry *lib.RevisionEntry, targetPath string, blockId lib.BlockId, data []byte) {
	m.progress()
	m.bytesWritten += int64(len(data))
	if !m.Verbose {
		return
	}
	fmt.Printf("  block %s %6s\n", blockId, ws.FormatBytes(int64(len(data))))
}

func (m *CpMonitor) OnEnd(entry *lib.RevisionEntry, targetPath string) {
	m.progress()
	if !m.Verbose {
		return
	}
	if entry.Metadata.ModeAndPerm.IsDir() {
		fmt.Printf("  done  (directory)\n")
		return
	}
	fmt.Printf("  done  %s %6s\n", hex.EncodeToString(entry.Metadata.FileHash[:]), ws.FormatBytes(entry.Metadata.Size))
}

// Clear the progress line.
func (m *CpMonitor) Close() {
	if !m.isProgress() {
		return
	}
	clearLine()
}

func (m *CpMonitor) progress() {
	if !m.isProgress() {
		return
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d files copied", (m.paths - m.excluded)))
	if m.excluded > 0 {
		sb.WriteString(fmt.Sprintf(" (+ %d excluded)", m.excluded))
	}
	if m.errors > 0 {
		sb.WriteString(fmt.Sprintf(", %d errors", m.errors))
	}
	mbs := (float64(m.bytesWritten) / float64(time.Since(m.startTime).Seconds()))
	sb.WriteString(fmt.Sprintf(" (%s at %s/s)", ws.FormatBytes(m.bytesWritten), ws.FormatBytes(int64(mbs))))
	clearLine()
	fmt.Fprintf(os.Stderr, "\r%s", sb.String())
}

func (m *CpMonitor) isProgress() bool {
	return !m.Verbose && !m.NoProgress && IsTerm(os.Stderr)
}

func clearLine() {
	cols, _, err := term.GetSize(int(os.Stderr.Fd()))
	if err != nil {
		fmt.Fprint(os.Stderr, "\n")
		return
	}
	fmt.Fprint(os.Stderr, "\r"+strings.Repeat(" ", cols)+"\r")
}
