package workspace

import (
	"encoding/hex"
	"fmt"
	"io/fs"
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
	header *lib.BlockHeader,
	existed bool,
	dataSize int64,
) error {
	if err := m.cancel(); err != nil {
		return err
	}
	if existed {
		m.RawBytesReused += dataSize
	} else {
		m.RawBytesAdded += dataSize
		m.CompressedBytesAdded += int64(header.EncryptedDataSize) - lib.TotalCipherOverhead
	}
	m.emitProgress()
	if m.Mode != DefaultMonitorModeVerbose {
		return nil
	}
	if existed {
		m.emit(fmt.Sprintf("  block  %s %6s (old)", header.BlockId, FormatBytes(dataSize)))
		return nil
	}
	if header.Flags&lib.BlockFlagDeflate == lib.BlockFlagDeflate {
		m.emit(
			fmt.Sprintf(
				"  block  %s %6s (new) (compressed: %.2f)",
				header.BlockId,
				FormatBytes(dataSize),
				float64(header.EncryptedDataSize-lib.TotalCipherOverhead)/float64(dataSize),
			),
		)
		return nil
	}
	m.emit(fmt.Sprintf("  block  %s %6s (new)", header.BlockId, FormatBytes(dataSize)))
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
	if entry.Metadata.ModeAndPerm.IsDir() {
		m.emit(fmt.Sprintf("  %-6s (directory)", entry.Type))
		return nil
	}
	m.emit(
		fmt.Sprintf(
			"  %-6s %s %6s",
			entry.Type,
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

func (m *DefaultStagingMonitor) OnEnd(path lib.Path, excluded bool, metadata *lib.FileMetadata) error {
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
		if metadata != nil && metadata.ModeAndPerm.IsDir() {
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
	if entry.Metadata.ModeAndPerm.IsDir() {
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
	Revisions      int
	Paths          int
	MetadataBytes  int64
	MetadataBlocks int
	DataBytes      int64
	DataBlocks     int
	RevisionEntry  *lib.RevisionEntry
}

func NewDefaultHealthCheckMonitor(mode DefaultMonitorMode, emit MonitorEmit) *DefaultHealthCheckMonitor {
	return &DefaultHealthCheckMonitor{
		defaultMonitorBase: newDefaultMonitorBase(mode, nil, emit),
		StartTime:          time.Time{},
		Revisions:          0,
		Paths:              0,
		MetadataBytes:      0,
		MetadataBlocks:     0,
		DataBytes:          0,
		DataBlocks:         0,
		RevisionEntry:      nil,
	}
}

func (m *DefaultHealthCheckMonitor) OnRevisionStart(revisionID lib.RevisionId) {
	m.RevisionEntry = nil
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
	m.RevisionEntry = entry
	m.Paths++
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit(fmt.Sprintf("  path     %s (%s)", entry.Path, entry.Type))
	}
}

func (m *DefaultHealthCheckMonitor) OnBlockOk(blockID lib.BlockId, duplicate bool, length int) {
	if !duplicate {
		if m.RevisionEntry == nil {
			m.MetadataBytes += int64(length)
			m.MetadataBlocks++
		} else {
			m.DataBytes += int64(length)
			m.DataBlocks++
		}
	}
	m.emitProgress()
	if m.Mode != DefaultMonitorModeVerbose {
		return
	}
	prefix := ""
	if m.RevisionEntry != nil {
		prefix = "  "
	}
	m.emit(prefix + "  block  " + blockID.String())
}

func (m *DefaultHealthCheckMonitor) emitProgress() {
	if m.Mode != DefaultMonitorModeProgress || m.StartTime.IsZero() {
		return
	}
	totalBytes := m.MetadataBytes + m.DataBytes
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	m.emit(
		fmt.Sprintf(
			"%d revisions, %d path entries, %d unique blocks, %s at %s/s",
			m.Revisions,
			m.Paths,
			m.DataBlocks+m.MetadataBlocks,
			FormatBytes(totalBytes),
			FormatBytes(int64(float64(totalBytes)/elapsed)),
		),
	)
}

type DefaultSyncRepoMonitor struct {
	defaultMonitorBase
	StartTime     time.Time
	Revisions     int
	Paths         int
	Blocks        int
	Bytes         int64
	RevisionEntry *lib.RevisionEntry
}

func NewDefaultSyncRepoMonitor(mode DefaultMonitorMode, emit MonitorEmit) *DefaultSyncRepoMonitor {
	return &DefaultSyncRepoMonitor{
		defaultMonitorBase: newDefaultMonitorBase(mode, nil, emit),
		StartTime:          time.Time{},
		Revisions:          0,
		Paths:              0,
		Blocks:             0,
		Bytes:              0,
		RevisionEntry:      nil,
	}
}

func (m *DefaultSyncRepoMonitor) OnBeforeUpdateDstHead(newHead lib.RevisionId) {
	m.emit(fmt.Sprintf("Updating target repository head to %s", newHead))
}

func (m *DefaultSyncRepoMonitor) OnRevisionStart(revisionID lib.RevisionId) {
	m.RevisionEntry = nil
	if m.StartTime.IsZero() {
		m.StartTime = time.Now()
	}
	m.Revisions++
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit(fmt.Sprintf("revision %s", revisionID))
	}
}

func (m *DefaultSyncRepoMonitor) OnRevisionEntry(entry *lib.RevisionEntry) {
	m.RevisionEntry = entry
	m.Paths++
	m.emitProgress()
	if m.Mode == DefaultMonitorModeVerbose {
		m.emit(fmt.Sprintf("  path     %s (%s)", entry.Path, entry.Type))
	}
}

func (m *DefaultSyncRepoMonitor) OnCopyBlock(blockID lib.BlockId, existed bool, length int) {
	if !existed {
		m.Blocks++
		m.Bytes += int64(length)
	}
	m.emitProgress()
	if m.Mode != DefaultMonitorModeVerbose {
		return
	}
	prefix := ""
	if m.RevisionEntry != nil {
		prefix = "  "
	}
	m.emit(prefix + "  block  " + blockID.String())
}

func (m *DefaultSyncRepoMonitor) emitProgress() {
	if m.Mode != DefaultMonitorModeProgress || m.StartTime.IsZero() {
		return
	}
	elapsed := time.Since(m.StartTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	m.emit(
		fmt.Sprintf(
			"%d revisions, %d path entries, %d unique blocks, %s at %s/s",
			m.Revisions,
			m.Paths,
			m.Blocks,
			FormatBytes(m.Bytes),
			FormatBytes(int64(float64(m.Bytes)/elapsed)),
		),
	)
}
