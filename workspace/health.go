package workspace

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"strconv"
	"strings"

	"github.com/cling-com/cling-sync/lib"
)

// Wrapped by every problem `CheckHealth` finds, so a caller can tell a
// broken cache (always fixed by `ClearCache`) from an operational failure.
var ErrCacheUnhealthy = lib.Errorf("workspace cache is unhealthy")

// Observe the workspace health check.
type HealthCheckMonitor interface {
	OnBlockVerified(blockId lib.BlockId, length int)
}

// Verify the health of the workspace, which currently means its block cache.
//
// Every cached block must exist in the repository, and with `checkData` its
// bytes must match (no decryption needed, cached blocks are byte-identical).
// The cache must not exceed its configured limit and `cache/object-stats`
// must roughly match the actual size. The first problem found is returned as
// an error wrapping `ErrCacheUnhealthy`. `source` must be the repository's
// direct storage, e.g. a `FileStorage` but never a `CacheStorage`.
//
//nolint:funlen
func CheckHealth(
	ctx context.Context,
	cache *lib.FileStorage,
	source lib.Storage,
	checkData bool,
	monitor HealthCheckMonitor,
) error {
	if _, ok := source.(*CacheStorage); ok {
		panic("CheckHealth: source must be the direct storage, not a CacheStorage")
	}
	maxBytes, err := readCacheMaxBytes(ctx, cache)
	if err != nil {
		return err
	}
	// A deleted cache directory is a healthy cache, the cache is transient.
	if _, err := cache.FS.Stat(cache.ObjectsDir()); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	totalBytes := int64(0)
	var blockErr error
	buf, sourceBuf := lib.NewBlockBuf(), lib.NewBlockBuf()
	walkErr := cache.ReadBlockIds(ctx, func(blockId lib.BlockId) bool {
		stat, err := cache.FS.Stat(cache.BlockPath(blockId))
		if err != nil {
			blockErr = lib.WrapErrorf(err, "failed to stat cached block %s", blockId)
			return false
		}
		totalBytes += stat.Size()
		monitor.OnBlockVerified(blockId, int(stat.Size()))
		if !checkData {
			ok, err := source.HasBlock(ctx, blockId)
			if err != nil {
				blockErr = lib.WrapErrorf(err, "failed to check block %s in the repository", blockId)
				return false
			}
			if !ok {
				blockErr = lib.WrapErrorf(ErrCacheUnhealthy,
					"cached block %s does not exist in the repository", blockId)
			}
			return blockErr == nil
		}
		cached, err := cache.ReadBlock(ctx, blockId, buf, lib.ReadBlockOpts{})
		if err != nil {
			blockErr = lib.WrapErrorf(err, "failed to read cached block %s", blockId)
			return false
		}
		data, err := source.ReadBlock(ctx, blockId, sourceBuf, lib.ReadBlockOpts{})
		if errors.Is(err, lib.ErrBlockNotFound) {
			blockErr = lib.WrapErrorf(ErrCacheUnhealthy,
				"cached block %s does not exist in the repository", blockId)
			return false
		}
		if err != nil {
			blockErr = lib.WrapErrorf(err, "failed to read block %s from the repository", blockId)
			return false
		}
		if !bytes.Equal(cached, data) {
			blockErr = lib.WrapErrorf(ErrCacheUnhealthy, "cached block %s differs from the repository", blockId)
		}
		return blockErr == nil
	})
	if walkErr != nil {
		return lib.WrapErrorf(walkErr, "failed to walk the block cache")
	}
	if blockErr != nil {
		return blockErr
	}
	if maxBytes == 0 {
		// With the limit disabled no stats are kept, so there is nothing
		// more to check.
		return nil
	}
	if totalBytes > maxBytes {
		return lib.WrapErrorf(ErrCacheUnhealthy,
			"cache holds %d bytes, exceeding the limit of %d", totalBytes, maxBytes)
	}
	statsData, err := lib.ReadFile(cache.FS, cacheStatsPath)
	if err != nil {
		// A missing or unreadable stats file rebuilds itself.
		return nil //nolint:nilerr
	}
	statsBytes, err := strconv.ParseInt(strings.TrimSpace(string(statsData)), 10, 64)
	if err != nil || statsBytes < 0 {
		// So does a malformed one.
		return nil //nolint:nilerr
	}
	// The accounting is best-effort, so allow the 1% the eviction works in.
	if diff := statsBytes - totalBytes; diff > maxBytes/100 || diff < -maxBytes/100 {
		return lib.WrapErrorf(ErrCacheUnhealthy,
			"cache accounting says %d bytes but the cache holds %d", statsBytes, totalBytes)
	}
	return nil
}

// Remove all cached blocks and the stats file.
//
// The cache is transient, so this is always safe. It is the remedy for
// everything `CheckHealth` reports.
func ClearCache(cache *lib.FileStorage) error {
	if err := cache.FS.RemoveAll(cache.ObjectsDir()); err != nil {
		return lib.WrapErrorf(err, "failed to remove %s", cache.ObjectsDir())
	}
	if err := cache.FS.MkdirAll(cache.ObjectsDir()); err != nil {
		return lib.WrapErrorf(err, "failed to recreate %s", cache.ObjectsDir())
	}
	if err := cache.FS.Remove(cacheStatsPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return lib.WrapErrorf(err, "failed to remove %s", cacheStatsPath)
	}
	return nil
}
