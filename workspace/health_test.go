package workspace

import (
	"io/fs"
	"testing"

	"github.com/cling-com/cling-sync/lib"
)

type testHealthCheckMonitor struct {
	blocks int
	bytes  int64
}

func (m *testHealthCheckMonitor) OnBlockVerified(_ lib.BlockId, length int) {
	m.blocks++
	m.bytes += int64(length)
}

func newTestHealthCheckMonitor() *testHealthCheckMonitor {
	return &testHealthCheckMonitor{blocks: 0, bytes: 0}
}

func TestCheckHealth(t *testing.T) {
	t.Parallel()
	revision := lib.WriteBlockOpts{RevisionBlockHint: true}

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)
		_, err = sut.WriteBlock(t.Context(), td.BlockId("1"), []byte("data"), revision)
		assert.NoError(err)

		monitor := newTestHealthCheckMonitor()
		assert.NoError(CheckHealth(t.Context(), cache, source, false, monitor))
		assert.Equal(1, monitor.blocks)
		assert.Equal(int64(4), monitor.bytes)

		monitor = newTestHealthCheckMonitor()
		assert.NoError(CheckHealth(t.Context(), cache, source, true, monitor))
		assert.Equal(1, monitor.blocks)
		assert.Equal(int64(4), monitor.bytes)
	})

	t.Run("A cached block missing in the repository should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		_, err := cache.WriteBlock(t.Context(), td.BlockId("1"), []byte("data"), lib.WriteBlockOpts{})
		assert.NoError(err)

		err = CheckHealth(t.Context(), cache, source, false, newTestHealthCheckMonitor())
		assert.ErrorIs(err, ErrCacheUnhealthy)
		assert.Error(err, "does not exist in the repository")
		err = CheckHealth(t.Context(), cache, source, true, newTestHealthCheckMonitor())
		assert.ErrorIs(err, ErrCacheUnhealthy)
		assert.Error(err, "does not exist in the repository")
	})

	t.Run("A cached block that differs should only fail with checkData", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		_, err := cache.WriteBlock(t.Context(), td.BlockId("1"), []byte("stale"), lib.WriteBlockOpts{})
		assert.NoError(err)
		_, err = source.WriteBlock(t.Context(), td.BlockId("1"), []byte("fresh"), lib.WriteBlockOpts{})
		assert.NoError(err)

		err = CheckHealth(t.Context(), cache, source, false, newTestHealthCheckMonitor())
		assert.NoError(err, "without checkData only existence is checked")
		err = CheckHealth(t.Context(), cache, source, true, newTestHealthCheckMonitor())
		assert.ErrorIs(err, ErrCacheUnhealthy)
		assert.Error(err, "differs from the repository")
	})

	t.Run("A cache over the limit should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		writeCacheConfig(t, cache, "10")
		data := []byte("aaaaaaaaaab")
		_, err := source.WriteBlock(t.Context(), td.BlockId("1"), data, lib.WriteBlockOpts{})
		assert.NoError(err)
		_, err = cache.WriteBlock(t.Context(), td.BlockId("1"), data, lib.WriteBlockOpts{})
		assert.NoError(err)

		err = CheckHealth(t.Context(), cache, source, false, newTestHealthCheckMonitor())
		assert.ErrorIs(err, ErrCacheUnhealthy)
		assert.Error(err, "exceeding the limit")
	})

	t.Run("Accounting outside the 1% slack should fail, inside not", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		writeCacheConfig(t, cache, "1000")
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)
		_, err = sut.WriteBlock(t.Context(), td.BlockId("1"), []byte("aaaaaaaaaa"), revision)
		assert.NoError(err)

		// The cache holds 10 bytes, the slack is 1000/100 = 10.
		assert.NoError(lib.WriteFile(cache.FS, cacheStatsPath, []byte("20")))
		err = CheckHealth(t.Context(), cache, source, false, newTestHealthCheckMonitor())
		assert.NoError(err, "a difference of exactly the slack should pass")

		assert.NoError(lib.WriteFile(cache.FS, cacheStatsPath, []byte("21")))
		err = CheckHealth(t.Context(), cache, source, false, newTestHealthCheckMonitor())
		assert.ErrorIs(err, ErrCacheUnhealthy)
		assert.Error(err, "cache accounting says 21")
	})

	t.Run("A CacheStorage as source should panic", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		sut, err := NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.NoError(err)

		panicked := false
		func() {
			defer func() { panicked = recover() != nil }()
			_ = CheckHealth(t.Context(), cache, sut, false, newTestHealthCheckMonitor())
		}()
		assert.Equal(true, panicked, "checking through the cache would defeat the check")
	})

	t.Run("A deleted cache directory should be healthy and empty", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		assert.NoError(cache.FS.RemoveAll(cache.ObjectsDir()))

		monitor := newTestHealthCheckMonitor()
		assert.NoError(CheckHealth(t.Context(), cache, source, false, monitor))
		assert.Equal(0, monitor.blocks)
	})
}

func TestClearCache(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)
		revision := lib.WriteBlockOpts{RevisionBlockHint: true}
		_, err = sut.WriteBlock(t.Context(), td.BlockId("1"), []byte("data"), revision)
		assert.NoError(err)

		assert.NoError(ClearCache(cache))
		ok, err := cache.HasBlock(t.Context(), td.BlockId("1"))
		assert.NoError(err)
		assert.Equal(false, ok)
		_, err = cache.FS.Stat(cacheStatsPath)
		assert.ErrorIs(err, fs.ErrNotExist)

		// The cache keeps working after a clear. A new instance, because the
		// CLI runs the clear as its own invocation.
		sut, err = NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)
		_, err = sut.WriteBlock(t.Context(), td.BlockId("2"), []byte("data2"), revision)
		assert.NoError(err)
		assert.Equal("5", readCacheStats(t, sut))
	})
}
