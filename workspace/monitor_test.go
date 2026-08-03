//go:build !wasm

package workspace

import (
	"testing"
	"time"

	"github.com/cling-com/cling-sync/lib"
)

func TestMonitorPreparing(t *testing.T) {
	t.Parallel()
	collect := func() (*[]string, MonitorEmit) {
		var lines []string
		return &lines, func(text string) { lines = append(lines, text) }
	}

	t.Run("Progress mode should emit a placeholder", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		lines, emit := collect()
		NewDefaultStagingMonitor(DefaultMonitorModeProgress, nil, emit).Preparing()
		assert.Equal([]string{"preparing..."}, *lines)
	})

	t.Run("Verbose mode should emit a placeholder", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		lines, emit := collect()
		NewDefaultStagingMonitor(DefaultMonitorModeVerbose, nil, emit).Preparing()
		assert.Equal([]string{"preparing..."}, *lines)
	})

	t.Run("Silent mode should emit nothing", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		lines, emit := collect()
		NewDefaultStagingMonitor(DefaultMonitorModeSilent, nil, emit).Preparing()
		assert.Equal(0, len(*lines))
	})

	t.Run("Sync-repo should prefix the target name", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		lines, emit := collect()
		NewDefaultSyncRepoMonitor(DefaultMonitorModeProgress, emit, "backup").Preparing()
		assert.Equal([]string{"backup: preparing..."}, *lines)
	})

	t.Run("Sync-repo silent mode should emit nothing", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		lines, emit := collect()
		NewDefaultSyncRepoMonitor(DefaultMonitorModeSilent, emit, "backup").Preparing()
		assert.Equal(0, len(*lines))
	})
}

func TestDefaultRevisionSnapshotMonitor(t *testing.T) {
	t.Parallel()
	collect := func() (*[]string, MonitorEmit) {
		var lines []string
		return &lines, func(text string) { lines = append(lines, text) }
	}

	t.Run("Progress mode should report revisions and entries", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		lines, emit := collect()
		mon := NewDefaultRevisionSnapshotMonitor(DefaultMonitorModeProgress, emit)
		mon.progressInterval = 0
		mon.OnRevisionStart(td.RevisionId("1"))
		mon.OnRevisionEntry(td.RevisionEntry("a.txt", lib.RevisionEntryKindAdd))
		assert.Equal([]string{
			"read 1 revisions, 0 path entries",
			"read 1 revisions, 1 path entries",
		}, *lines)
	})

	t.Run("Verbose mode should name every revision but no entry", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		lines, emit := collect()
		mon := NewDefaultRevisionSnapshotMonitor(DefaultMonitorModeVerbose, emit)
		mon.OnRevisionStart(td.RevisionId("1"))
		mon.OnRevisionEntry(td.RevisionEntry("a.txt", lib.RevisionEntryKindAdd))
		assert.Equal([]string{"revision " + td.RevisionId("1").String()}, *lines)
	})

	t.Run("Silent mode should emit nothing", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		lines, emit := collect()
		mon := NewDefaultRevisionSnapshotMonitor(DefaultMonitorModeSilent, emit)
		mon.progressInterval = 0
		mon.OnRevisionStart(td.RevisionId("1"))
		mon.OnRevisionEntry(td.RevisionEntry("a.txt", lib.RevisionEntryKindAdd))
		assert.Equal(0, len(*lines))
	})
}

// Every monitor draws its progress line in place, so every one of them
// throttles it. A case builds one monitor bound to `emit` and returns the
// embedded interval knob plus a call that produces exactly one progress line.
func progressMonitorCases() []struct {
	name string
	new  func(emit MonitorEmit) (*defaultMonitorBase, func())
} {
	entry := td.RevisionEntry("a.txt", lib.RevisionEntryKindAdd)
	return []struct {
		name string
		new  func(emit MonitorEmit) (*defaultMonitorBase, func())
	}{
		{"snapshot", func(emit MonitorEmit) (*defaultMonitorBase, func()) {
			m := NewDefaultRevisionSnapshotMonitor(DefaultMonitorModeProgress, emit)
			return &m.defaultMonitorBase, func() { m.OnRevisionEntry(entry) }
		}},
		{"commit", func(emit MonitorEmit) (*defaultMonitorBase, func()) {
			m := NewDefaultCommitMonitor(DefaultMonitorModeProgress, nil, emit)
			return &m.defaultMonitorBase, func() { _ = m.OnStart(entry) }
		}},
		{"staging", func(emit MonitorEmit) (*defaultMonitorBase, func()) {
			m := NewDefaultStagingMonitor(DefaultMonitorModeProgress, nil, emit)
			return &m.defaultMonitorBase, func() { _ = m.OnStart(entry.Path, nil) }
		}},
		{"cp", func(emit MonitorEmit) (*defaultMonitorBase, func()) {
			m := NewDefaultCpMonitor(DefaultMonitorModeProgress, nil, emit, CpOnExistsAbort, false)
			return &m.defaultMonitorBase, func() { _ = m.OnStart(entry, "a.txt") }
		}},
		{"health-check", func(emit MonitorEmit) (*defaultMonitorBase, func()) {
			m := NewDefaultHealthCheckMonitor(DefaultMonitorModeProgress, emit)
			return &m.defaultMonitorBase, func() { m.OnRevisionStart(td.RevisionId("1")) }
		}},
		{"sync-repo", func(emit MonitorEmit) (*defaultMonitorBase, func()) {
			m := NewDefaultSyncRepoMonitor(DefaultMonitorModeProgress, emit, "backup")
			// Only `OnBeforeCopy` starts the clock, and it emits unthrottled.
			m.StartTime = time.Now()
			return &m.defaultMonitorBase, func() { m.OnCopyBlock(td.BlockId("1"), false, 1) }
		}},
	}
}

func TestMonitorProgressThrottle(t *testing.T) {
	t.Parallel()
	collect := func() (*[]string, MonitorEmit) {
		var lines []string
		return &lines, func(text string) { lines = append(lines, text) }
	}
	for _, tc := range progressMonitorCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			t.Run("Only the first tick of an interval should draw a line", func(t *testing.T) {
				t.Parallel()
				assert := lib.NewAssert(t)
				lines, emit := collect()
				base, tick := tc.new(emit)
				base.progressInterval = time.Hour
				tick()
				tick()
				tick()
				assert.Equal(1, len(*lines))
			})

			t.Run("A zero interval should draw every tick", func(t *testing.T) {
				t.Parallel()
				assert := lib.NewAssert(t)
				lines, emit := collect()
				base, tick := tc.new(emit)
				base.progressInterval = 0
				tick()
				tick()
				tick()
				assert.Equal(3, len(*lines))
			})
		})
	}
}
