package workspace

import (
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

const (
	cacheDir           = workspaceDir + "/cache"
	cacheFinalDir      = cacheDir + "/staging"
	cacheTempDirPrefix = ".staging-tmp-"
)

var ErrSymLinkTargetEscapes = lib.Errorf("symlink target escapes path root")

type StagingEntryMonitor interface {
	OnStart(path lib.Path, dirEntry fs.DirEntry) error
	OnEnd(path lib.Path, excluded bool, metadata *lib.PathMetadata) error
}

type Staging struct {
	PathFilter lib.PathFilter
	pathPrefix lib.Path
	tempWriter *lib.TempWriter[*StagingEntry]
	temp       *lib.Temp[*StagingEntry]
	tmpFS      lib.FS
}

// Build a `Staging` from the `src` directory.
// `.cling` is always ignored.
// If `pathPrefix` is not empty, it will be prepended to all paths *after* the
// `pathFilter` is applied.
func NewStaging( //nolint:funlen
	src lib.FS,
	pathPrefix lib.Path,
	pathFilter lib.PathFilter,
	useCache bool,
	tmp lib.FS,
	mon StagingEntryMonitor,
) (*Staging, error) {
	revisionEntryWriter := NewStagingCacheWriter(tmp, lib.DefaultTempChunkSize)
	cache, err := NewStagingCache(src, useCache)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create staging cache")
	}
	defer cache.Cleanup() //nolint:errcheck
	staging := &Staging{pathFilter, pathPrefix, revisionEntryWriter, nil, tmp}
	err = lib.WalkDirIgnore(src, ".", func(path_ string, d fs.DirEntry, err error) (retErr error) {
		if err != nil {
			return err
		}
		if path_ == "." {
			return nil
		}
		if lib.IsAtomicWriteTempFile(path_) {
			_ = src.Remove(path_)
			return nil
		}
		localPath, err := lib.NewPath(path_)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create path from %s", path_)
		}
		if localPath.Base().String() == ".cling" {
			return filepath.SkipDir
		}
		fileInfo, err := d.Info()
		if err != nil {
			return lib.WrapErrorf(err, "failed to get file info for %s", localPath)
		}
		isSymlink := d.Type()&fs.ModeSymlink != 0
		if !d.Type().IsRegular() && !d.Type().IsDir() && !isSymlink {
			// This filetype is not supported - we ignore it silently.
			return nil
		}
		if err := mon.OnStart(localPath, d); err != nil {
			return lib.WrapErrorf(err, "staging monitor start failed for %s", localPath)
		}
		// From here on, `OnEnd` runs unconditionally. If both the staging work
		// and `OnEnd` error, the `OnEnd` error wins (more recent failure).
		var excluded bool
		var entryMD *lib.PathMetadata
		defer func() {
			if endErr := mon.OnEnd(localPath, excluded, entryMD); endErr != nil {
				retErr = lib.WrapErrorf(endErr, "staging monitor end failed for %s", localPath)
			}
		}()
		// Eager exclusion so we don't hash excluded files or recurse into
		// excluded directories.
		if pathFilter != nil && !pathFilter.Include(localPath, d.IsDir()) {
			excluded = true
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		repoPath := pathPrefix.Join(localPath)
		var entry *StagingEntry
		if isSymlink {
			target, err := src.ReadLink(localPath.String())
			if err != nil {
				return lib.WrapErrorf(err, "failed to read symlink target for %s", localPath)
			}
			if filepath.IsAbs(target) {
				return lib.WrapErrorf(ErrSymLinkTargetEscapes, "absolute target %q at %s", target, localPath)
			}
			joined := filepath.ToSlash(filepath.Clean(filepath.Join(filepath.Dir(localPath.String()), target)))
			resolved, err := lib.NewPath(joined)
			if err != nil {
				return lib.WrapErrorf(
					ErrSymLinkTargetEscapes,
					"target %q at %s escapes workspace root",
					target,
					localPath,
				)
			}
			repoTarget := pathPrefix.Join(resolved)
			entry, err = NewStagingEntry(repoPath, fileInfo, fileInfo.Size(), lib.Sha256{}, nil)
			if err != nil {
				return lib.WrapErrorf(err, "failed to build staging entry for %s", localPath)
			}
			entry.Metadata.SymLinkTarget = &repoTarget
		} else {
			entry, err = cache.Handle(localPath, repoPath, fileInfo)
			if err != nil {
				return lib.WrapErrorf(err, "failed to stage %s", localPath)
			}
		}
		entryMD = &entry.Metadata
		if err := staging.add(entry); err != nil {
			return lib.WrapErrorf(err, "failed to add %s to staging (as %s)", localPath, repoPath)
		}
		return nil
	})
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to walk directory %s", src)
	}
	if err := cache.Finalize(); err != nil {
		return nil, lib.WrapErrorf(err, "failed to close cache")
	}
	return staging, nil
}

func (s *Staging) Finalize() (*lib.Temp[*StagingEntry], error) {
	if s.temp == nil {
		t, err := s.tempWriter.Finalize()
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to finalize staging temp writer")
		}
		s.temp = t
	}
	return s.temp, nil
}

// Merge the staging snapshot with the revision snapshot.
// The resulting `RevisionTemp` will contain all entries that transition from the
// revision snapshot to the staging snapshot.
// If `suppressDeletes` is `true`, paths that are in the revision snapshot but
// not in staging do not produce `Delete` entries. Used when the diff baseline
// is the repository head rather than the workspace head (attach-non-empty).
func (s *Staging) MergeWithSnapshot( //nolint:funlen
	snapshot *lib.Temp[*lib.RevisionEntry],
	restorableMetadataFlag lib.RestorableMetadataFlag,
	suppressDeletes bool,
) (*lib.Temp[*lib.RevisionEntry], error) {
	stgTemp, err := s.Finalize()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to finalize staging temp writer")
	}
	revFilter := s.PathFilter
	if !s.pathPrefix.IsEmpty() {
		include := s.pathPrefix.AsFilter()
		if revFilter != nil {
			revFilter = &lib.AllPathFilter{Filters: []lib.PathFilter{revFilter, include}}
		} else {
			revFilter = include
		}
	}
	revReader := snapshot.Reader(lib.RevisionEntryPathFilter(revFilter))
	stgReader := stgTemp.Reader(StagingEntryPathFilter(s.PathFilter))
	final, err := s.tmpFS.MkSub("final")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create commit directory")
	}
	finalWriter := lib.NewRevisionEntryTempWriter(final, lib.MaxBlockDataSize)
	add := func(path lib.Path, kind lib.RevisionEntryKind, md lib.PathMetadata) error {
		if suppressDeletes && kind == lib.RevisionEntryKindDelete {
			return nil
		}
		re := lib.RevisionEntry{Kind: kind, Path: path, Metadata: md}
		if err := finalWriter.Add(&re); err != nil {
			return lib.WrapErrorf(err, "failed to write revision entry for path %s", path)
		}
		return nil
	}
	var stg *StagingEntry
	var rev *lib.RevisionEntry
	buf := lib.NewBlockBuf()
	symlinkPointsOutsideWorkspacePrefix := func(e *lib.RevisionEntry) bool {
		if e == nil || !e.Metadata.FileMode.IsSymlink() || e.Metadata.SymLinkTarget == nil {
			return false
		}
		_, inside := e.Metadata.SymLinkTarget.TrimBase(s.pathPrefix)
		return !inside
	}
	for {
		if stg == nil {
			// Read the next staging entry.
			stg, err = stgReader.Read(buf)
			if errors.Is(err, io.EOF) {
				// Write a delete for all remaining revision snapshot entries.
				for {
					if rev != nil && !symlinkPointsOutsideWorkspacePrefix(rev) {
						if err := add(rev.Path, lib.RevisionEntryKindDelete, rev.Metadata); err != nil {
							return nil, err
						}
					}
					rev, err = revReader.Read(buf)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return nil, lib.WrapErrorf(err, "failed to read revision snapshot")
					}
				}
				break
			}
			if err != nil {
				return nil, lib.WrapErrorf(err, "failed to read staging snapshot")
			}
		}
		if rev == nil {
			// Read the next revision snapshot entry.
			rev, err = revReader.Read(buf)
			if errors.Is(err, io.EOF) {
				// Write an add for all remaining staging entries.
				for {
					if stg != nil { // The current one might be nil.
						if err := add(stg.RepoPath, lib.RevisionEntryKindAdd, stg.Metadata); err != nil {
							return nil, err
						}
					}
					stg, err = stgReader.Read(buf)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return nil, lib.WrapErrorf(err, "failed to read staging snapshot")
					}
				}
				break
			}
			if err != nil {
				return nil, lib.WrapErrorf(err, "failed to read revision snapshot")
			}
		}
		c := strings.Compare(
			lib.PathCompareString(stg.RepoPath, stg.Metadata.FileMode.IsDir()),
			lib.PathCompareString(rev.Path, rev.Metadata.FileMode.IsDir()),
		)
		if c == 0 { //nolint:gocritic
			if !stg.Metadata.IsEqualRestorableAttributes(rev.Metadata, restorableMetadataFlag) {
				// Write an update.
				if err := add(stg.RepoPath, lib.RevisionEntryKindUpdate, stg.Metadata); err != nil {
					return nil, err
				}
			}
			stg = nil
			rev = nil
		} else if c < 0 {
			// Write an add.
			if err := add(stg.RepoPath, lib.RevisionEntryKindAdd, stg.Metadata); err != nil {
				return nil, err
			}
			stg = nil
			continue
		} else {
			if !symlinkPointsOutsideWorkspacePrefix(rev) {
				if err := add(rev.Path, lib.RevisionEntryKindDelete, rev.Metadata); err != nil {
					return nil, err
				}
			}
			rev = nil
			continue
		}
	}
	temp, err := finalWriter.Finalize()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to finalize commit")
	}
	return temp, nil
}

func (s *Staging) add(stagingEntry *StagingEntry) error {
	if s.tempWriter == nil {
		return lib.Errorf("staging is closed")
	}
	if s.PathFilter != nil && !s.PathFilter.Include(stagingEntry.RepoPath, stagingEntry.Metadata.FileMode.IsDir()) {
		return nil
	}
	if err := s.tempWriter.Add(stagingEntry); err != nil {
		return err //nolint:wrapcheck
	}
	return nil
}

type StagingCache struct {
	src          lib.FS
	cacheTempDir string
	cacheWriter  *lib.TempWriter[*StagingEntry]
	cache        *lib.TempCache[*StagingEntry]
}

func NewStagingCache(src lib.FS, useCache bool) (*StagingCache, error) {
	rand, err := lib.RandStr(32)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to generate random string for cache temp dir")
	}
	cacheTempDir := filepath.Join(cacheDir, cacheTempDirPrefix+rand)
	var cacheWriter *lib.TempWriter[*StagingEntry]
	var cache *lib.TempCache[*StagingEntry]
	cacheTempFS, err := src.MkSub(cacheTempDir)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create cache tmp dir")
	}
	cacheWriter = NewStagingCacheWriter(cacheTempFS, lib.MaxBlockDataSize)
	if useCache {
		cacheFS, err := src.Sub(cacheFinalDir)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, lib.WrapErrorf(err, "failed to open cache dir")
		}
		if err == nil {
			cache, err = OpenStagingCache(cacheFS, 10) // todo: Choose a reasonable max chunks in cache.
			if err != nil {
				return nil, lib.WrapErrorf(err, "failed to open cache")
			}
		}
	}
	return &StagingCache{
		src:          src,
		cacheTempDir: cacheTempDir,
		cacheWriter:  cacheWriter,
		cache:        cache,
	}, nil
}

// Return the metadata either from the cache or compute it.
// Update the cache.
func (c *StagingCache) Handle(localPath lib.Path, repoPath lib.Path, fileInfo fs.FileInfo) (*StagingEntry, error) {
	var fileMetadata *lib.PathMetadata
	var stagingEntry *StagingEntry
	var err error
	if c.cache != nil {
		existingEntry, ok, err := c.cache.Get(lib.PathCompareString(repoPath, fileInfo.IsDir()))
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to get entry from cache for %s", localPath)
		}
		if ok && existingEntry.Metadata.Size == fileInfo.Size() {
			newEntry, err := NewStagingEntry(
				repoPath,
				fileInfo,
				existingEntry.Metadata.Size,
				existingEntry.Metadata.FileHash,
				existingEntry.Metadata.BlockIds,
			)
			if err != nil {
				return nil, lib.WrapErrorf(err, "failed to create cache entry for %s", localPath)
			}
			if !newEntry.HasChanged(existingEntry) {
				stagingEntry = newEntry
				md := lib.NewPathMetadataFromFileInfo(
					fileInfo,
					existingEntry.Metadata.FileHash,
					existingEntry.Metadata.BlockIds,
				)
				fileMetadata = &md
			}
		}
	}
	if fileMetadata == nil {
		md, err := computeFileHash(c.src, localPath, fileInfo)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to get metadata for %s", localPath)
		}
		fileMetadata = &md
	}
	if stagingEntry == nil {
		stagingEntry, err = NewStagingEntry(
			repoPath,
			fileInfo,
			fileMetadata.Size,
			fileMetadata.FileHash,
			fileMetadata.BlockIds,
		)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to create cache entry for %s", localPath)
		}
	}
	if err := c.cacheWriter.Add(stagingEntry); err != nil {
		return nil, lib.WrapErrorf(err, "failed to add cache entry for %s", localPath)
	}
	return stagingEntry, nil
}

func (c *StagingCache) Finalize() error {
	if _, err := c.cacheWriter.Finalize(); err != nil {
		return lib.WrapErrorf(err, "failed to finalize cache writer")
	}
	// Move the cache to the final location.
	if err := c.src.RemoveAll(cacheFinalDir); err != nil {
		return lib.WrapErrorf(err, "failed to remove cache dir")
	}
	if err := c.src.Rename(c.cacheTempDir, cacheFinalDir); err != nil {
		return lib.WrapErrorf(err, "failed to move temp cache dir %s to %s", c.cacheTempDir, cacheFinalDir)
	}
	return nil
}

// Remove the current and all temp cache directories if they are alder than one day.
func (c *StagingCache) Cleanup() error {
	if err := c.src.RemoveAll(c.cacheTempDir); err != nil {
		return lib.WrapErrorf(err, "failed to remove cache temp dir %s", c.cacheTempDir)
	}
	files, err := c.src.ReadDir(cacheDir)
	if err != nil {
		return lib.WrapErrorf(err, "failed to find stale cache dirs")
	}
	for _, f := range files {
		if strings.HasPrefix(f.Name(), cacheTempDirPrefix) {
			fileInfo, err := f.Info()
			if err != nil {
				return lib.WrapErrorf(err, "failed to get file info for %s", f.Name())
			}
			if time.Since(fileInfo.ModTime()) > time.Hour*24 {
				if err := c.src.RemoveAll(filepath.Join(cacheDir, f.Name())); err != nil {
					return lib.WrapErrorf(err, "failed to remove stale cache dir %s", f.Name())
				}
			}
		}
	}
	return nil
}

func computeFileHash(fs lib.FS, path lib.Path, fileInfo fs.FileInfo) (lib.PathMetadata, error) {
	if fileInfo.IsDir() {
		return lib.NewPathMetadataFromFileInfo(fileInfo, lib.Sha256{}, nil), nil
	}
	f, err := fs.OpenRead(path.String())
	if err != nil {
		return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
	}
	defer f.Close() //nolint:errcheck
	fileHash := sha256.New()
	if _, err := io.Copy(fileHash, f); err != nil {
		return lib.PathMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
	}
	return lib.NewPathMetadataFromFileInfo(fileInfo, lib.Sha256(fileHash.Sum(nil)), nil), nil
}
