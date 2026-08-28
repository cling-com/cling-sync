package workspace

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cling-com/cling-sync/lib"
)

// DefaultCacheMaxBytes bounds the cache when `conf/object-cache` does not say
// otherwise.
//
// A `max-bytes` of 0 disables the limit and all bookkeeping.
const DefaultCacheMaxBytes = 1_000_000_000

const (
	cacheConfigName    = "object-cache"
	cacheStatsPath     = workspaceCacheDir + "/object-stats"
	cacheEvictMinFiles = 10
)

// CacheStorage answers block reads from `cache` before falling back to `source`.
//
// Only accesses hinted as revision data (`RevisionBlockHint`) touch the
// cache, so data blocks never fill it up. The cache is transient: a failed
// cache read falls back to `source` and repairs the cache, and a read with
// the authoritative hint bypasses the cache entirely (the retry
// `lib.Repository.ReadBlock` performs when a block fails to decrypt).
// Everything besides block reads and writes is delegated to `source`.
//
// The cache size is limited best-effort to `cache.max-bytes` of
// `conf/object-cache`. The size is tracked in `cache/object-stats`, written
// without regard for concurrent processes and rebuilt by a walk when missing
// or unreadable. A write that would exceed the limit first evicts random
// blocks and is skipped if it still does not fit.
type CacheStorage struct {
	// The cache is a `*lib.FileStorage` rather than a `lib.Storage` because
	// repairing it means replacing a block file, and no `lib.Storage` is ever
	// allowed to touch a block again once written. It must be the workspace's
	// own storage (`lib.StoragePurposeWorkspace`).
	cache       *lib.FileStorage
	source      lib.Storage
	maxBytes    int64
	statsBytes  int64
	statsLoaded bool
}

func NewCacheStorage(ctx context.Context, cache *lib.FileStorage, source lib.Storage) (*CacheStorage, error) {
	maxBytes, err := readCacheMaxBytes(ctx, cache)
	if err != nil {
		return nil, err
	}
	return &CacheStorage{cache: cache, source: source, maxBytes: maxBytes}, nil //nolint:exhaustruct
}

var _ lib.Storage = (*CacheStorage)(nil)

// Read the cache limit from the `conf/object-cache` control file.
//
// The file is user-authored, so a missing file or key means the default while
// a malformed one is an error the user has to see.
func readCacheMaxBytes(ctx context.Context, cache *lib.FileStorage) (int64, error) {
	data, err := cache.ReadControlFile(ctx, lib.ControlFileSectionConf, cacheConfigName)
	if errors.Is(err, lib.ErrControlFileNotFound) {
		return DefaultCacheMaxBytes, nil
	}
	if err != nil {
		return 0, lib.WrapErrorf(err, "failed to read %s", cacheConfigName)
	}
	toml, err := lib.ReadToml(bytes.NewReader(data))
	if err != nil {
		return 0, lib.WrapErrorf(err, "failed to read %s", cacheConfigName)
	}
	value, ok := toml.GetValue("cache", "max-bytes")
	if !ok {
		return DefaultCacheMaxBytes, nil
	}
	maxBytes, err := strconv.ParseInt(value, 10, 64)
	if err != nil || maxBytes < 0 {
		return 0, lib.Errorf("invalid cache.max-bytes %q in conf/%s", value, cacheConfigName)
	}
	return maxBytes, nil
}

func (s *CacheStorage) ReadBlock(
	ctx context.Context,
	blockId lib.BlockId,
	buf lib.BlockBuf,
	opts lib.ReadBlockOpts,
) ([]byte, error) {
	if !opts.RevisionBlockHint {
		return s.source.ReadBlock(ctx, blockId, buf, opts) //nolint:wrapcheck
	}
	if opts.AuthoritativeHint {
		data, err := s.source.ReadBlock(ctx, blockId, buf, opts)
		if err != nil {
			return nil, err //nolint:wrapcheck
		}
		s.populate(ctx, blockId, data)
		return data, nil
	}
	data, err := s.cache.ReadBlock(ctx, blockId, buf, opts)
	if err == nil {
		return data, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, lib.WrapErrorf(ctxErr, "canceled while reading block %s", blockId)
	}
	data, err = s.source.ReadBlock(ctx, blockId, buf, opts)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}
	s.populate(ctx, blockId, data)
	return data, nil
}

func (s *CacheStorage) WriteBlock(
	ctx context.Context,
	blockId lib.BlockId,
	data []byte,
	opts lib.WriteBlockOpts,
) (bool, error) {
	exists, err := s.source.WriteBlock(ctx, blockId, data, opts)
	if err != nil {
		return false, err //nolint:wrapcheck
	}
	if opts.RevisionBlockHint {
		s.populate(ctx, blockId, data)
	}
	return exists, nil
}

// Consult only `source`: a cached block proves nothing about the source, and
// a false positive would let a caller skip a required upload.
func (s *CacheStorage) HasBlock(ctx context.Context, blockId lib.BlockId) (bool, error) {
	return s.source.HasBlock(ctx, blockId) //nolint:wrapcheck
}

// Populate the cache, best-effort: the cache must never fail an operation the
// source satisfied.
//
// Remove any existing file first, because a possibly corrupt entry has to be
// replaced and `WriteBlock` skips blocks that already exist.
func (s *CacheStorage) populate(ctx context.Context, blockId lib.BlockId, data []byte) {
	if s.maxBytes > 0 && !s.loadStats() {
		return
	}
	path := s.cache.BlockPath(blockId)
	if stat, err := s.cache.FS.Stat(path); err == nil {
		_ = s.cache.FS.Remove(path)
		s.statsBytes -= stat.Size()
	}
	if s.maxBytes == 0 {
		_, _ = s.cache.WriteBlock(ctx, blockId, data, lib.WriteBlockOpts{RevisionBlockHint: true})
		return
	}
	if s.statsBytes+int64(len(data)) > s.maxBytes {
		s.evict()
	}
	if s.statsBytes+int64(len(data)) <= s.maxBytes {
		if _, err := s.cache.WriteBlock(ctx, blockId, data, lib.WriteBlockOpts{RevisionBlockHint: true}); err == nil {
			s.statsBytes += int64(len(data))
		}
	}
	s.writeStats()
}

// Evict blocks, shard by random shard, until 1% of the limit and at least
// `cacheEvictMinFiles` files are freed.
//
// Block ids are uniform, so the victims are an unbiased random sample.
func (s *CacheStorage) evict() {
	shards, err := s.cache.FS.ReadDir(s.cache.ObjectsDir())
	if err != nil {
		return
	}
	rand.Shuffle(len(shards), func(i, j int) { shards[i], shards[j] = shards[j], shards[i] })
	freedBytes, freedFiles := int64(0), 0
	done := func() bool { return freedBytes >= s.maxBytes/100 && freedFiles >= cacheEvictMinFiles }
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		_ = s.cache.FS.WalkDir(filepath.Join(s.cache.ObjectsDir(), shard.Name()),
			func(path string, d fs.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return err
				}
				if lib.IsAtomicWriteTempFile(path) {
					// A crash leftover. Delete it, but keep it out of the
					// accounting because `loadStats` never counted it.
					_ = s.cache.FS.Remove(path)
					return nil
				}
				if done() {
					return fs.SkipAll
				}
				info, err := d.Info()
				if err != nil {
					return err //nolint:wrapcheck
				}
				if s.cache.FS.Remove(path) == nil {
					s.statsBytes -= info.Size()
					freedBytes += info.Size()
					freedFiles++
				}
				return nil
			})
		if done() {
			return
		}
	}
}

// Load the cached size from `cache/object-stats`, rebuilding the file by
// walking the cache when it is missing or unreadable.
func (s *CacheStorage) loadStats() bool {
	if s.statsLoaded {
		return true
	}
	if data, err := lib.ReadFile(s.cache.FS, cacheStatsPath); err == nil {
		if bytes, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64); err == nil && bytes >= 0 {
			s.statsBytes, s.statsLoaded = bytes, true
			return true
		}
	}
	total := int64(0)
	err := s.cache.FS.WalkDir(s.cache.ObjectsDir(), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || lib.IsAtomicWriteTempFile(path) {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err //nolint:wrapcheck
		}
		total += info.Size()
		return nil
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false
	}
	s.statsBytes, s.statsLoaded = total, true
	s.writeStats()
	return true
}

func (s *CacheStorage) writeStats() {
	if s.cache.FS.MkdirAll(workspaceCacheDir) != nil {
		return
	}
	_ = lib.WriteFile(s.cache.FS, cacheStatsPath, []byte(strconv.FormatInt(s.statsBytes, 10)))
}

func (s *CacheStorage) Init(ctx context.Context, config lib.Toml, headerComment string) error {
	return s.source.Init(ctx, config, headerComment) //nolint:wrapcheck
}

func (s *CacheStorage) Open(ctx context.Context) (lib.Toml, error) {
	return s.source.Open(ctx) //nolint:wrapcheck
}

func (s *CacheStorage) ReadBlockIds(ctx context.Context, yield func(lib.BlockId) bool) error {
	return s.source.ReadBlockIds(ctx, yield) //nolint:wrapcheck
}

func (s *CacheStorage) ReadControlFile(
	ctx context.Context,
	section lib.ControlFileSection,
	name string,
) ([]byte, error) {
	return s.source.ReadControlFile(ctx, section, name) //nolint:wrapcheck
}

func (s *CacheStorage) WriteControlFile(
	ctx context.Context,
	section lib.ControlFileSection,
	name string,
	data []byte,
) error {
	return s.source.WriteControlFile(ctx, section, name, data) //nolint:wrapcheck
}

func (s *CacheStorage) HasControlFile(ctx context.Context, section lib.ControlFileSection, name string) (bool, error) {
	return s.source.HasControlFile(ctx, section, name) //nolint:wrapcheck
}

func (s *CacheStorage) DeleteControlFile(ctx context.Context, section lib.ControlFileSection, name string) error {
	return s.source.DeleteControlFile(ctx, section, name) //nolint:wrapcheck
}

func (s *CacheStorage) Lock(ctx context.Context, name string) (func() error, error) {
	return s.source.Lock(ctx, name) //nolint:wrapcheck
}

func (s *CacheStorage) ForceUnlock(ctx context.Context, name string) error {
	return s.source.ForceUnlock(ctx, name) //nolint:wrapcheck
}
