package workspace

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

type DefaultMonitorMode int

const (
	DefaultMonitorModeSilent DefaultMonitorMode = iota
	DefaultMonitorModeProgress
	DefaultMonitorModeVerbose
)

type MonitorEmit func(text string)

type defaultMonitorBase struct {
	Mode   DefaultMonitorMode
	cancel func() error
	emit   MonitorEmit
}

func newDefaultMonitorBase(
	mode DefaultMonitorMode,
	cancel func() error,
	emit MonitorEmit,
) defaultMonitorBase {
	if cancel == nil {
		cancel = func() error { return nil }
	}
	if emit == nil {
		emit = func(string) {}
	}
	return defaultMonitorBase{Mode: mode, cancel: cancel, emit: emit}
}

// Preparing emits a placeholder while an operation stays silent before its first real output.
func (m *defaultMonitorBase) Preparing() {
	if m.Mode == DefaultMonitorModeSilent {
		return
	}
	m.emit("preparing...")
}

type DefaultCommitMonitor struct {
	defaultMonitorBase
	StartTime            time.Time
	Paths                int
	RawBytesAdded        int64
	CompressedBytesAdded int64
	RawBytesReused       int64
}

func NewDefaultCommitMonitor(
	mode DefaultMonitorMode,
	cancel func() error,
	emit MonitorEmit,
) *DefaultCommitMonitor {
	return &DefaultCommitMonitor{
		defaultMonitorBase:   newDefaultMonitorBase(mode, cancel, emit),
		StartTime:            time.Time{},
		Paths:                0,
		RawBytesAdded:        0,
		CompressedBytesAdded: 0,
		RawBytesReused:       0,
	}
}

func (m *DefaultCommitMonitor) OnBeforeCommit() error {
	if err := m.cancel(); err != nil {
		return err
	}
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit("Committing")
	}
	return nil
}

func (m *DefaultCommitMonitor) OnStart(entry *lib.RevisionEntry) error {
	if err := m.cancel(); err != nil {
		return err
	}
	if m.StartTime.IsZero() {
		m.StartTime = time.Now()
	}
	m.Paths++
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit(entry.Path.String())
	}
	return nil
}

func (m *DefaultCommitMonitor) OnAddBlock(
	entry *lib.RevisionEntry,
	blockId lib.BlockId,
	dataSize int,
	dataBytesWritten *int,
) error {
	if err := m.cancel(); err != nil {
		return err
	}
	existed := dataBytesWritten == nil
	isCompressed := dataBytesWritten != nil && dataSize != *dataBytesWritten
	if existed {
		m.RawBytesReused += int64(dataSize)
	} else {
		m.RawBytesAdded += int64(dataSize)
		m.CompressedBytesAdded += int64(*dataBytesWritten)
	}
	m.emitProgress()
	if m.Mode != DefaultMonitorModeVerbose {
		return nil
	}
	if existed {
		m.emit(fmt.Sprintf("  block  %s %6s (old)", blockId, FormatBytes(int64(dataSize))))
		return nil
	}
	if isCompressed {
		m.emit(
			fmt.Sprintf(
				"  block  %s %6s (new) (compressed: %.2f)",
				blockId,
				FormatBytes(int64(dataSize)),
				float64(*dataBytesWritten)/float64(dataSize),
			),
		)
		return nil
	}
	m.emit(fmt.Sprintf("  block  %s %6s (new)", blockId, FormatBytes(int64(dataSize))))
	return nil
}

func (m *DefaultCommitMonitor) OnEnd(entry *lib.RevisionEntry) error {
	if err := m.cancel(); err != nil {
		return err
	}
	m.emitProgress()
	if m.Mode != DefaultMonitorModeVerbose {
		return nil
	}
	if entry.Metadata.FileMode.IsDir() {
		m.emit(fmt.Sprintf("  %-6s (directory)", entry.Kind))
		return nil
	}
	m.emit(
		fmt.Sprintf(
			"  %-6s %s %6s",
			entry.Kind,
			hex.EncodeToString(entry.Metadata.FileHash[:]),
			FormatBytes(entry.Metadata.Size),
		),
	)
	return nil
}

func (m *DefaultCommitMonitor) emitProgress() {
	if m.Mode != DefaultMonitorModeProgress || m.StartTime.IsZero() {
		return
	}
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	m.emit(
		fmt.Sprintf(
			"adding %d paths (%s at %s/s)",
			m.Paths,
			FormatBytes(m.RawBytesAdded),
			FormatBytes(int64(float64(m.RawBytesAdded)/elapsed)),
		),
	)
}

type DefaultStagingMonitor struct {
	defaultMonitorBase
	StartTime      time.Time
	Paths          int
	Excluded       int
	TotalFileSizes int64
}

func NewDefaultStagingMonitor(
	mode DefaultMonitorMode,
	cancel func() error,
	emit MonitorEmit,
) *DefaultStagingMonitor {
	return &DefaultStagingMonitor{
		defaultMonitorBase: newDefaultMonitorBase(mode, cancel, emit),
		StartTime:          time.Time{},
		Paths:              0,
		Excluded:           0,
		TotalFileSizes:     0,
	}
}

func (m *DefaultStagingMonitor) OnStart(path lib.Path, dirEntry fs.DirEntry) error {
	if err := m.cancel(); err != nil {
		return err
	}
	if m.StartTime.IsZero() {
		m.StartTime = time.Now()
	}
	m.Paths++
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit(path.String())
	}
	return nil
}

func (m *DefaultStagingMonitor) OnEnd(path lib.Path, excluded bool, metadata *lib.PathMetadata) error {
	if err := m.cancel(); err != nil {
		return err
	}
	if excluded {
		m.Excluded++
		m.emitProgress()
		if m.Mode == DefaultMonitorModeVerbose {
			m.emit("  excluded")
		}
		return nil
	}
	if metadata != nil {
		m.TotalFileSizes += metadata.Size
	}
	if m.Mode == DefaultMonitorModeVerbose {
		if metadata != nil && metadata.FileMode.IsDir() {
			m.emit("  done  (directory)")
		} else if metadata != nil {
			m.emit(
				fmt.Sprintf("  done  %s %6s", hex.EncodeToString(metadata.FileHash[:]), FormatBytes(metadata.Size)),
			)
		}
	}
	m.emitProgress()
	return nil
}

func (m *DefaultStagingMonitor) emitProgress() {
	if m.Mode != DefaultMonitorModeProgress || m.StartTime.IsZero() {
		return
	}
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	text := fmt.Sprintf("scanned %d paths", m.Paths-m.Excluded)
	if m.Excluded > 0 {
		text += fmt.Sprintf(" (%d excluded)", m.Excluded)
	}
	text += fmt.Sprintf(
		" (%s at %s/s)",
		FormatBytes(m.TotalFileSizes),
		FormatBytes(int64(float64(m.TotalFileSizes)/elapsed)),
	)
	m.emit(text)
}

type DefaultCpMonitor struct {
	defaultMonitorBase
	ignoreErrors bool
	cpOnExists   CpOnExists
	StartTime    time.Time
	Paths        int
	Excluded     int
	BytesWritten int64
	Errors       int
}

func NewDefaultCpMonitor(
	mode DefaultMonitorMode,
	cancel func() error,
	emit MonitorEmit,
	cpOnExists CpOnExists,
	ignoreErrors bool,
) *DefaultCpMonitor {
	return &DefaultCpMonitor{
		defaultMonitorBase: newDefaultMonitorBase(mode, cancel, emit),
		ignoreErrors:       ignoreErrors,
		cpOnExists:         cpOnExists,
		StartTime:          time.Time{},
		Paths:              0,
		Excluded:           0,
		BytesWritten:       0,
		Errors:             0,
	}
}

func (m *DefaultCpMonitor) OnStart(entry *lib.RevisionEntry, targetPath string) error {
	if err := m.cancel(); err != nil {
		return err
	}
	if m.StartTime.IsZero() {
		m.StartTime = time.Now()
	}
	m.Paths++
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit(targetPath)
	}
	return nil
}

func (m *DefaultCpMonitor) OnExists(entry *lib.RevisionEntry, targetPath string) CpOnExists {
	if m.Mode == DefaultMonitorModeVerbose && m.cpOnExists == CpOnExistsIgnore {
		m.emit("  skipping existing")
	}
	return m.cpOnExists
}

func (m *DefaultCpMonitor) OnWrite(
	entry *lib.RevisionEntry,
	targetPath string,
	blockID lib.BlockId,
	data []byte,
) error {
	if err := m.cancel(); err != nil {
		return err
	}
	m.BytesWritten += int64(len(data))
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit(fmt.Sprintf("  block %s %6s", blockID, FormatBytes(int64(len(data)))))
	}
	return nil
}

func (m *DefaultCpMonitor) OnEnd(entry *lib.RevisionEntry, targetPath string) error {
	if err := m.cancel(); err != nil {
		return err
	}
	m.emitProgress()
	if m.Mode != DefaultMonitorModeVerbose {
		return nil
	}
	if entry.Metadata.FileMode.IsDir() {
		m.emit("  done  (directory)")
		return nil
	}
	m.emit(
		fmt.Sprintf("  done  %s %6s", hex.EncodeToString(entry.Metadata.FileHash[:]), FormatBytes(entry.Metadata.Size)),
	)
	return nil
}

func (m *DefaultCpMonitor) OnError(entry *lib.RevisionEntry, targetPath string, err error) CpOnError {
	m.Errors++
	if !m.ignoreErrors {
		return CpOnErrorAbort
	}
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit("  ignoring error\n    " + strings.ReplaceAll(err.Error(), "\n", "\n    "))
	} else {
		m.emit(targetPath + "\n  " + strings.ReplaceAll(err.Error(), "\n", "\n  "))
	}
	m.emitProgress()
	return CpOnErrorIgnore
}

func (m *DefaultCpMonitor) emitProgress() {
	if m.Mode != DefaultMonitorModeProgress || m.StartTime.IsZero() {
		return
	}
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	text := fmt.Sprintf("%d files copied", m.Paths-m.Excluded)
	if m.Excluded > 0 {
		text += fmt.Sprintf(" (+ %d excluded)", m.Excluded)
	}
	if m.Errors > 0 {
		text += fmt.Sprintf(", %d errors", m.Errors)
	}
	text += fmt.Sprintf(
		" (%s at %s/s)",
		FormatBytes(m.BytesWritten),
		FormatBytes(int64(float64(m.BytesWritten)/elapsed)),
	)
	m.emit(text)
}

type DefaultHealthCheckMonitor struct {
	defaultMonitorBase
	StartTime      time.Time
	EndTime        time.Time
	Revisions      int
	Paths          int
	Blocks         int
	BlockBytes     int64
	OrphanedBlocks []lib.BlockId
}

func NewDefaultHealthCheckMonitor(mode DefaultMonitorMode, emit MonitorEmit) *DefaultHealthCheckMonitor {
	return &DefaultHealthCheckMonitor{
		defaultMonitorBase: newDefaultMonitorBase(mode, nil, emit),
		StartTime:          time.Time{},
		EndTime:            time.Time{},
		Revisions:          0,
		Paths:              0,
		Blocks:             0,
		BlockBytes:         0,
		OrphanedBlocks:     nil,
	}
}

func (m *DefaultHealthCheckMonitor) OnRevisionStart(revisionID lib.RevisionId) {
	if m.StartTime.IsZero() {
		m.StartTime = time.Now()
	}
	m.Revisions++
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit(fmt.Sprintf("revision %s", revisionID))
	}
}

func (m *DefaultHealthCheckMonitor) OnRevisionEntry(entry *lib.RevisionEntry) {
	m.Paths++
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit(fmt.Sprintf("  path     %s (%s)", entry.Path, entry.Kind))
	}
}

func (m *DefaultHealthCheckMonitor) OnBlockVerified(blockID lib.BlockId, length int) {
	m.Blocks++
	m.BlockBytes += int64(length)
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit("  block    " + blockID.String())
	}
}

func (m *DefaultHealthCheckMonitor) OnOrphanedBlock(blockID lib.BlockId) {
	m.OrphanedBlocks = append(m.OrphanedBlocks, blockID)
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit("  orphan   " + blockID.String())
	}
}

func (m *DefaultHealthCheckMonitor) Finish() {
	m.EndTime = time.Now()
}

func (m *DefaultHealthCheckMonitor) Duration() time.Duration {
	if m.StartTime.IsZero() {
		return 0
	}
	end := m.EndTime
	if end.IsZero() {
		end = time.Now()
	}
	return end.Sub(m.StartTime)
}

func (m *DefaultHealthCheckMonitor) Report(
	checkedBlocks bool,
	checkedOrphanedBlocks bool,
	orphanedBlocksFile string,
) (string, error) {
	check := func(b bool) string {
		if b {
			return "ok"
		}
		return "--"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Repository is healthy\n")
	fmt.Fprintf(&b, "  [ok] revision chain is intact\n")
	fmt.Fprintf(&b, "  [ok] metadata blocks are readable\n")
	fmt.Fprintf(&b, "  [ok] paths in each revision are sorted\n")
	fmt.Fprintf(&b, "  [%s] data blocks are valid\n", check(checkedBlocks))
	orphanLine := "--"
	if checkedOrphanedBlocks {
		if len(m.OrphanedBlocks) > 0 {
			orphanLine = "!!"
		} else {
			orphanLine = "ok"
		}
	}
	fmt.Fprintf(&b, "  [%s] no orphaned blocks in storage\n", orphanLine)
	fmt.Fprintf(&b, "\nStatistics:\n")
	fmt.Fprintf(&b, "  %d revisions\n", m.Revisions)
	fmt.Fprintf(&b, "  %d path entries in all revisions\n", m.Paths)
	if checkedBlocks {
		fmt.Fprintf(&b, "  %d blocks\n", m.Blocks)
		fmt.Fprintf(&b, "  %s (%dB) read from storage\n", FormatBytes(m.BlockBytes), m.BlockBytes)
	}
	if checkedOrphanedBlocks {
		file := ""
		if len(m.OrphanedBlocks) > 0 && orphanedBlocksFile != "" {
			file = fmt.Sprintf(" (%s)", orphanedBlocksFile)
		}
		fmt.Fprintf(&b, "  %d orphaned blocks%s\n", len(m.OrphanedBlocks), file)
		if len(m.OrphanedBlocks) > 0 {
			fmt.Fprint(&b, "  Note: a concurrent commit may have added  blocks that aren't\n")
			fmt.Fprint(&b, "        yet referenced by a revision. Re-run after it completes.\n")
		}
	}
	fmt.Fprintf(&b, "\nTiming:\n")
	fmt.Fprintf(&b, "  start    %s\n", m.StartTime.Format(time.RFC3339))
	fmt.Fprintf(&b, "  end      %s\n", m.EndTime.Format(time.RFC3339))
	fmt.Fprintf(&b, "  duration %s\n", m.Duration().Round(time.Millisecond))
	if checkedOrphanedBlocks && len(m.OrphanedBlocks) > 0 && orphanedBlocksFile != "" {
		if err := m.writeOrphanedBlocksFile(orphanedBlocksFile); err != nil {
			return "", err
		}
	}
	return b.String(), nil
}

//nolint:forbidigo,errcheck
func (m *DefaultHealthCheckMonitor) writeOrphanedBlocksFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return lib.WrapErrorf(err, "failed to create %s", path)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	fmt.Fprintf(w, "# start    %s\n", m.StartTime.Format(time.RFC3339))
	fmt.Fprintf(w, "# end      %s\n", m.EndTime.Format(time.RFC3339))
	fmt.Fprintf(w, "# duration %s\n", m.Duration().Round(time.Millisecond))
	fmt.Fprintln(w)
	for _, id := range m.OrphanedBlocks {
		if _, err := fmt.Fprintln(w, id.String()); err != nil {
			return lib.WrapErrorf(err, "failed to write %s", path)
		}
	}
	if err := w.Flush(); err != nil {
		return lib.WrapErrorf(err, "failed to flush %s", path)
	}
	if err := f.Close(); err != nil {
		return lib.WrapErrorf(err, "failed to close %s", path)
	}
	return nil
}

func (m *DefaultHealthCheckMonitor) emitProgress() {
	if m.Mode != DefaultMonitorModeProgress || m.StartTime.IsZero() {
		return
	}
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	m.emit(
		fmt.Sprintf(
			"%d revisions, %d path entries, %d blocks, %d orphans, %s at %s/s",
			m.Revisions,
			m.Paths,
			m.Blocks,
			len(m.OrphanedBlocks),
			FormatBytes(m.BlockBytes),
			FormatBytes(int64(float64(m.BlockBytes)/elapsed)),
		),
	)
}

type DefaultSyncRepoMonitor struct {
	defaultMonitorBase
	TargetName string
	StartTime  time.Time
	SrcBlocks  int
	DstBlocks  int
	Blocks     int
	Bytes      int64
}

func NewDefaultSyncRepoMonitor(mode DefaultMonitorMode, emit MonitorEmit, targetName string) *DefaultSyncRepoMonitor {
	return &DefaultSyncRepoMonitor{
		defaultMonitorBase: newDefaultMonitorBase(mode, nil, emit),
		TargetName:         targetName,
		StartTime:          time.Time{},
		SrcBlocks:          0,
		DstBlocks:          0,
		Blocks:             0,
		Bytes:              0,
	}
}

func (m *DefaultSyncRepoMonitor) OnSrcBlockIdsRead(blocksTotal int) {
	if m.Mode != DefaultMonitorModeProgress {
		return
	}
	m.emitWithTargetPrefix(fmt.Sprintf("read %d source blocks", blocksTotal))
}

func (m *DefaultSyncRepoMonitor) OnDstBlockIdsRead(blocksTotal int) {
	if m.Mode != DefaultMonitorModeProgress {
		return
	}
	m.emitWithTargetPrefix(fmt.Sprintf("read %d target blocks", blocksTotal))
}

func (m *DefaultSyncRepoMonitor) OnBeforeCopy(srcBlocks, dstBlocks int) {
	m.StartTime = time.Now()
	m.SrcBlocks = srcBlocks
	m.DstBlocks = dstBlocks
	m.Blocks = 0
	m.Bytes = 0
	m.emitWithTargetPrefix(fmt.Sprintf("source has %d blocks, target has %d", srcBlocks, dstBlocks))
}

func (m *DefaultSyncRepoMonitor) OnBeforeUpdateDstHead(newHead lib.RevisionId) {
	m.emitWithTargetPrefix(fmt.Sprintf("updating target repository head to %s", newHead))
}

func (m *DefaultSyncRepoMonitor) OnCopyBlock(blockID lib.BlockId, existed bool, length int) {
	if !existed {
		m.Blocks++
		m.Bytes += int64(length)
	}
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emitWithTargetPrefix("block " + blockID.String())
	}
}

func (m *DefaultSyncRepoMonitor) Preparing() {
	if m.Mode == DefaultMonitorModeSilent {
		return
	}
	m.emitWithTargetPrefix("preparing...")
}

func (m *DefaultSyncRepoMonitor) emitProgress() {
	if m.Mode != DefaultMonitorModeProgress || m.StartTime.IsZero() {
		return
	}
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	m.emitWithTargetPrefix(
		fmt.Sprintf(
			"%d/%d blocks copied, %s at %s/s",
			m.Blocks,
			m.SrcBlocks-m.DstBlocks,
			FormatBytes(m.Bytes),
			FormatBytes(int64(float64(m.Bytes)/elapsed)),
		),
	)
}

func (m *DefaultSyncRepoMonitor) emitWithTargetPrefix(text string) {
	m.emit(m.TargetName + ": " + text)
}
