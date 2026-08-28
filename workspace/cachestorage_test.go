package workspace

import (
	"context"
	"io/fs"
	"testing"

	"github.com/cling-com/cling-sync/lib"
)

func TestCacheStorageReadBlock(t *testing.T) {
	t.Parallel()
	t.Run("A read without the revision hint should bypass the cache", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		blockId := td.BlockId("1")
		_, err = source.WriteBlock(t.Context(), blockId, []byte("data"), lib.WriteBlockOpts{})
		assert.NoError(err)

		data, err := sut.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{})
		assert.NoError(err)
		assert.Equal([]byte("data"), data)
		ok, err := cache.HasBlock(t.Context(), blockId)
		assert.NoError(err)
		assert.Equal(false, ok, "the cache should not have been populated")
	})

	t.Run("A read with the revision hint should be answered from the cache", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		blockId := td.BlockId("1")
		_, err = cache.WriteBlock(t.Context(), blockId, []byte("cached"), lib.WriteBlockOpts{})
		assert.NoError(err)

		data, err := sut.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{RevisionBlockHint: true})
		assert.NoError(err)
		assert.Equal([]byte("cached"), data)
	})

	t.Run("A cache miss should fall back to the source and populate the cache", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		blockId := td.BlockId("1")
		_, err = source.WriteBlock(t.Context(), blockId, []byte("data"), lib.WriteBlockOpts{})
		assert.NoError(err)

		data, err := sut.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{RevisionBlockHint: true})
		assert.NoError(err)
		assert.Equal([]byte("data"), data)
		cached, err := cache.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{})
		assert.NoError(err)
		assert.Equal([]byte("data"), cached)
	})

	t.Run("A block missing everywhere should report the source error", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		opts := lib.ReadBlockOpts{RevisionBlockHint: true}
		_, err = sut.ReadBlock(t.Context(), td.BlockId("1"), lib.NewBlockBuf(), opts)
		assert.ErrorIs(err, lib.ErrBlockNotFound)
	})

	t.Run("A failing cache read should fall back to the source and repair the cache", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		blockId := td.BlockId("1")
		_, err = cache.WriteBlock(t.Context(), blockId, []byte("stale"), lib.WriteBlockOpts{})
		assert.NoError(err)
		_, err = source.WriteBlock(t.Context(), blockId, []byte("fresh"), lib.WriteBlockOpts{})
		assert.NoError(err)
		// Make the cache read fail with an error other than `ErrBlockNotFound`.
		assert.NoError(cache.FS.Chmod(cache.BlockPath(blockId), 0o000))

		data, err := sut.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{RevisionBlockHint: true})
		assert.NoError(err)
		assert.Equal([]byte("fresh"), data)
		cached, err := cache.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{})
		assert.NoError(err)
		assert.Equal([]byte("fresh"), cached, "the unreadable cache entry should have been replaced")
	})

	t.Run("A deleted cache directory should repopulate", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		// Cache a block, then delete the whole cache directory.
		blockId := td.BlockId("1")
		_, err = source.WriteBlock(t.Context(), blockId, []byte("data"), lib.WriteBlockOpts{})
		assert.NoError(err)
		hinted := lib.ReadBlockOpts{RevisionBlockHint: true}
		_, err = sut.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), hinted)
		assert.NoError(err)
		assert.NoError(cache.FS.RemoveAll(cache.ObjectsDir()))

		// Reads fall back to the source and the cache fills up again.
		data, err := sut.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), hinted)
		assert.NoError(err)
		assert.Equal([]byte("data"), data)
		cached, err := cache.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{})
		assert.NoError(err)
		assert.Equal([]byte("data"), cached)
	})

	t.Run("A canceled context should not fall back to the source", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)

		blockId := td.BlockId("1")
		_, err := source.WriteBlock(t.Context(), blockId, []byte("data"), lib.WriteBlockOpts{})
		assert.NoError(err)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		sourceMonitor := td.MonitorStorage(source)
		sut, err := NewCacheStorage(t.Context(), cache, sourceMonitor)
		assert.NoError(err)

		_, err = sut.ReadBlock(ctx, blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{RevisionBlockHint: true})
		assert.ErrorIs(err, context.Canceled)
		assert.Equal([]string{}, sourceMonitor.BlockOps, "the source should not have been asked")
	})

	t.Run("An authoritative read should bypass the cache and repair it", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		blockId := td.BlockId("1")
		_, err = cache.WriteBlock(t.Context(), blockId, []byte("stale"), lib.WriteBlockOpts{})
		assert.NoError(err)
		_, err = source.WriteBlock(t.Context(), blockId, []byte("fresh"), lib.WriteBlockOpts{})
		assert.NoError(err)

		opts := lib.ReadBlockOpts{RevisionBlockHint: true, AuthoritativeHint: true}
		data, err := sut.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), opts)
		assert.NoError(err)
		assert.Equal([]byte("fresh"), data)
		cached, err := cache.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{})
		assert.NoError(err)
		assert.Equal([]byte("fresh"), cached, "the stale cache entry should have been replaced")
	})
}

func TestCacheStorageWriteBlock(t *testing.T) {
	t.Parallel()
	t.Run("A write without the revision hint should only go to the source", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		blockId := td.BlockId("1")
		existed, err := sut.WriteBlock(t.Context(), blockId, []byte("data"), lib.WriteBlockOpts{})
		assert.NoError(err)
		assert.Equal(false, existed)
		ok, err := source.HasBlock(t.Context(), blockId)
		assert.NoError(err)
		assert.Equal(true, ok)
		ok, err = cache.HasBlock(t.Context(), blockId)
		assert.NoError(err)
		assert.Equal(false, ok)
	})

	t.Run("A write with the revision hint should also populate the cache", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		blockId := td.BlockId("1")
		opts := lib.WriteBlockOpts{RevisionBlockHint: true}
		existed, err := sut.WriteBlock(t.Context(), blockId, []byte("data"), opts)
		assert.NoError(err)
		assert.Equal(false, existed)
		cached, err := cache.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), lib.ReadBlockOpts{})
		assert.NoError(err)
		assert.Equal([]byte("data"), cached)
	})
}

func TestCacheStorageHasBlock(t *testing.T) {
	t.Parallel()
	t.Run("HasBlock should consult only the source", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		_, err = cache.WriteBlock(t.Context(), td.BlockId("1"), []byte("cached"), lib.WriteBlockOpts{})
		assert.NoError(err)
		ok, err := sut.HasBlock(t.Context(), td.BlockId("1"))
		assert.NoError(err)
		assert.Equal(false, ok, "a block only in the cache must not count as present")

		_, err = source.WriteBlock(t.Context(), td.BlockId("2"), []byte("data"), lib.WriteBlockOpts{})
		assert.NoError(err)
		ok, err = sut.HasBlock(t.Context(), td.BlockId("2"))
		assert.NoError(err)
		assert.Equal(true, ok)
	})
}

func TestCacheStorageDelegation(t *testing.T) {
	t.Parallel()
	t.Run("Control files should be delegated to the source", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		err = sut.WriteControlFile(t.Context(), lib.ControlFileSectionRefs, "head", []byte("ref"))
		assert.NoError(err)
		data, err := source.ReadControlFile(t.Context(), lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("ref"), data)
		ok, err := cache.HasControlFile(t.Context(), lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(false, ok, "a cached head reference would serve stale revisions")
	})
}

func writeCacheConfig(tb testing.TB, cache *lib.FileStorage, maxBytes string) {
	tb.Helper()
	assert := lib.NewAssert(tb)
	config := []byte("[cache]\nmax-bytes = \"" + maxBytes + "\"\n")
	assert.NoError(cache.WriteControlFile(tb.Context(), lib.ControlFileSectionConf, cacheConfigName, config))
}

func readCacheStats(tb testing.TB, sut *CacheStorage) string {
	tb.Helper()
	assert := lib.NewAssert(tb)
	data, err := lib.ReadFile(sut.cache.FS, cacheStatsPath)
	assert.NoError(err)
	return string(data)
}

func TestCacheStorageLimit(t *testing.T) {
	t.Parallel()
	t.Run("A missing config should default the limit", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		sut, err := NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.NoError(err)
		assert.Equal(int64(DefaultCacheMaxBytes), sut.maxBytes)
	})

	t.Run("A malformed max-bytes should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		writeCacheConfig(t, cache, "many")
		_, err := NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.Error(err, "invalid cache.max-bytes")
	})

	t.Run("max-bytes 0 should disable the limit and the stats file", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		writeCacheConfig(t, cache, "0")
		sut, err := NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.NoError(err)

		opts := lib.WriteBlockOpts{RevisionBlockHint: true}
		_, err = sut.WriteBlock(t.Context(), td.BlockId("1"), []byte("data"), opts)
		assert.NoError(err)
		ok, err := cache.HasBlock(t.Context(), td.BlockId("1"))
		assert.NoError(err)
		assert.Equal(true, ok)
		_, err = cache.FS.Stat(cacheStatsPath)
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("A write at the limit should be cached, one over should evict first", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		writeCacheConfig(t, cache, "10")
		sut, err := NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.NoError(err)

		// Exactly at the limit: cached.
		revision := lib.WriteBlockOpts{RevisionBlockHint: true}
		_, err = sut.WriteBlock(t.Context(), td.BlockId("A"), []byte("aaaaaaaaaa"), revision)
		assert.NoError(err)
		ok, err := cache.HasBlock(t.Context(), td.BlockId("A"))
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal("10", readCacheStats(t, sut))

		// One byte over: the only cached block is evicted, then the write fits.
		_, err = sut.WriteBlock(t.Context(), td.BlockId("B"), []byte("b"), lib.WriteBlockOpts{RevisionBlockHint: true})
		assert.NoError(err)
		ok, err = cache.HasBlock(t.Context(), td.BlockId("A"))
		assert.NoError(err)
		assert.Equal(false, ok, "the previous block should have been evicted")
		ok, err = cache.HasBlock(t.Context(), td.BlockId("B"))
		assert.NoError(err)
		assert.Equal(true, ok)
		assert.Equal("1", readCacheStats(t, sut))
	})

	t.Run("A block larger than the limit should never be cached", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		writeCacheConfig(t, cache, "10")
		sut, err := NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.NoError(err)

		revision := lib.WriteBlockOpts{RevisionBlockHint: true}
		_, err = sut.WriteBlock(t.Context(), td.BlockId("1"), []byte("aaaaaaaaaab"), revision)
		assert.NoError(err, "the source write must still succeed")
		ok, err := cache.HasBlock(t.Context(), td.BlockId("1"))
		assert.NoError(err)
		assert.Equal(false, ok)
		assert.Equal("0", readCacheStats(t, sut))
	})

	t.Run("Replacing a corrupt cached block should keep the stats exact", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		source := td.NewFileStorage(t, lib.StoragePurposeRepository)
		writeCacheConfig(t, cache, "1000")
		sut, err := NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)

		// Cache a 10 byte block.
		blockId := td.BlockId("1")
		_, err = source.WriteBlock(t.Context(), blockId, []byte("aaaaaaaaaa"), lib.WriteBlockOpts{})
		assert.NoError(err)
		revision := lib.WriteBlockOpts{RevisionBlockHint: true}
		_, err = sut.WriteBlock(t.Context(), blockId, []byte("aaaaaaaaaa"), revision)
		assert.NoError(err)
		assert.Equal("10", readCacheStats(t, sut))

		// Corrupt the cached copy. It would not decrypt, so `Repository`
		// retries with an authoritative read, which replaces the entry.
		assert.NoError(cache.FS.Chmod(cache.BlockPath(blockId), 0o600))
		assert.NoError(lib.WriteFile(cache.FS, cache.BlockPath(blockId), []byte("XXXXXXXXXX")))

		// The next run repairs it. The replacement must not count it twice.
		sut, err = NewCacheStorage(t.Context(), cache, source)
		assert.NoError(err)
		opts := lib.ReadBlockOpts{RevisionBlockHint: true, AuthoritativeHint: true}
		data, err := sut.ReadBlock(t.Context(), blockId, lib.NewBlockBuf(), opts)
		assert.NoError(err)
		assert.Equal([]byte("aaaaaaaaaa"), data)
		assert.Equal("10", readCacheStats(t, sut))
	})

	t.Run("Eviction should delete atomic-write temp files without counting them", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		writeCacheConfig(t, cache, "10")
		sut, err := NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.NoError(err)

		revision := lib.WriteBlockOpts{RevisionBlockHint: true}
		_, err = sut.WriteBlock(t.Context(), td.BlockId("A"), []byte("aaaaaaaaaa"), revision)
		assert.NoError(err)
		// Simulate a crash that left an atomic-write temp file in the shard.
		tempPath := lib.AtomicWriteTempFilename(cache.BlockPath(td.BlockId("A")))
		assert.NoError(lib.WriteFile(cache.FS, tempPath, []byte("temp!")))

		_, err = sut.WriteBlock(t.Context(), td.BlockId("B"), []byte("b"), revision)
		assert.NoError(err)
		assert.Equal("1", readCacheStats(t, sut))
		_, err = cache.FS.Stat(tempPath)
		assert.ErrorIs(err, fs.ErrNotExist, "the temp file should have been cleaned up")
	})

	t.Run("A missing or corrupt stats file should be rebuilt by walking the cache", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cache := td.NewFileStorage(t, lib.StoragePurposeWorkspace)
		writeCacheConfig(t, cache, "1000")
		revision := lib.WriteBlockOpts{RevisionBlockHint: true}
		sut, err := NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.NoError(err)
		_, err = sut.WriteBlock(t.Context(), td.BlockId("1"), []byte("aaa"), revision)
		assert.NoError(err)

		assert.NoError(cache.FS.Remove(cacheStatsPath))
		sut, err = NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.NoError(err)
		_, err = sut.WriteBlock(t.Context(), td.BlockId("2"), []byte("bbbb"), revision)
		assert.NoError(err)
		assert.Equal("7", readCacheStats(t, sut))

		assert.NoError(lib.WriteFile(cache.FS, cacheStatsPath, []byte("torn")))
		sut, err = NewCacheStorage(t.Context(), cache, td.NewFileStorage(t, lib.StoragePurposeRepository))
		assert.NoError(err)
		_, err = sut.WriteBlock(t.Context(), td.BlockId("3"), []byte("ccccc"), revision)
		assert.NoError(err)
		assert.Equal("12", readCacheStats(t, sut))
	})
}
