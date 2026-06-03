//go:build !wasm

package workspace

import (
	"testing"

	"github.com/flunderpero/cling-sync/lib"
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
