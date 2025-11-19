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
	StagingCacheVersion uint16 = 1
	cacheDir                   = workspaceDir + "/cache"
	cacheFinalDir              = cacheDir + "/staging"
	cacheTempDirPrefix         = ".staging-tmp-"
)

type StagingEntryMonitor interface {
	OnStart(path lib.Path, dirEntry fs.DirEntry)
	OnEnd(path lib.Path, excluded bool, metadata *lib.FileMetadata)
}

type Staging struct {
	PathFilter lib.PathFilter
	pathPrefix lib.Path
	tempWriter *lib.TempWriter[StagingEntry]
	temp       *lib.Temp[StagingEntry]
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
	err = lib.WalkDirIgnore(src, ".", func(path_ string, d fs.DirEntry, err error) error {
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
		if !d.Type().IsRegular() && !d.Type().IsDir() {
			// todo: handle symlinks
			return nil
		}
		mon.OnStart(localPath, d)
		// Even though files are filtered out in Staging.Add, we still
		// want to eagerly exclude them to avoid unnecessary work (file hash).
		// Especially, we want to skip directories if they are excluded.
		if pathFilter != nil && !pathFilter.Include(localPath, d.IsDir()) {
			mon.OnEnd(localPath, true, nil)
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		repoPath := pathPrefix.Join(localPath)
		stagingEntry, err := cache.Handle(localPath, repoPath, fileInfo)
		if err != nil {
			// todo: We should report the error to the monitor.
			mon.OnEnd(localPath, false, nil)
			return lib.WrapErrorf(err, "failed to get metadata for %s", localPath)
		}
		if err := staging.add(stagingEntry); err != nil {
			// todo: We should report the error to the monitor.
			mon.OnEnd(localPath, false, stagingEntry.Metadata)
			return lib.WrapErrorf(err, "failed to add path %s to staging (as %s)", localPath, repoPath)
		}
		mon.OnEnd(localPath, false, stagingEntry.Metadata)
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

func (s *Staging) Finalize() (*lib.Temp[StagingEntry], error) {
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
//
// Parameters:
//
//	compareOwnership: If `true`, ownership of the file is compared.
func (s *Staging) MergeWithSnapshot( //nolint:funlen
	snapshot *lib.Temp[lib.RevisionEntry],
	restorableMetadataFlag lib.RestorableMetadataFlag,
) (*lib.Temp[lib.RevisionEntry], error) {
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
	add := func(path lib.Path, typ lib.RevisionEntryType, md *lib.FileMetadata) error {
		re, err := lib.NewRevisionEntry(path, typ, md)
		if err != nil {
			return lib.WrapErrorf(err, "failed to create revision entry for path %s", path)
		}
		if err := finalWriter.Add(&re); err != nil {
			return lib.WrapErrorf(err, "failed to write revision entry for path %s", path)
		}
		return nil
	}
	var stg *StagingEntry
	var rev *lib.RevisionEntry
	for {
		if stg == nil {
			// Read the next staging entry.
			stg, err = stgReader.Read()
			if errors.Is(err, io.EOF) {
				// Write a delete for all remaining revision snapshot entries.
				for {
					if rev != nil { // The current one might be nil.
						// Write a delete.
						if err := add(rev.Path, lib.RevisionEntryDelete, rev.Metadata); err != nil {
							return nil, err
						}
					}
					rev, err = revReader.Read()
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
			rev, err = revReader.Read()
			if errors.Is(err, io.EOF) {
				// Write an add for all remaining staging entries.
				for {
					if stg != nil { // The current one might be nil.
						if err := add(stg.RepoPath, lib.RevisionEntryAdd, stg.Metadata); err != nil {
							return nil, err
						}
					}
					stg, err = stgReader.Read()
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
			lib.PathCompareString(stg.RepoPath, stg.Metadata.ModeAndPerm.IsDir()),
			lib.PathCompareString(rev.Path, rev.Metadata.ModeAndPerm.IsDir()),
		)
		if c == 0 { //nolint:gocritic
			if !stg.Metadata.IsEqualRestorableAttributes(rev.Metadata, restorableMetadataFlag) {
				// Write an update.
				if err := add(stg.RepoPath, lib.RevisionEntryUpdate, stg.Metadata); err != nil {
					return nil, err
				}
			}
			stg = nil
			rev = nil
		} else if c < 0 {
			// Write an add.
			if err := add(stg.RepoPath, lib.RevisionEntryAdd, stg.Metadata); err != nil {
				return nil, err
			}
			stg = nil
			continue
		} else {
			// Write a delete.
			if err := add(rev.Path, lib.RevisionEntryDelete, rev.Metadata); err != nil {
				return nil, err
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
	if s.PathFilter != nil && !s.PathFilter.Include(stagingEntry.RepoPath, stagingEntry.Metadata.ModeAndPerm.IsDir()) {
		return nil
	}
	if err := s.tempWriter.Add(stagingEntry); err != nil {
		return err //nolint:wrapcheck
	}
	return nil
}

type StagingEntry struct {
	RepoPath  lib.Path
	Metadata  *lib.FileMetadata
	CTimeSec  int64
	CTimeNSec int32
	Size      int64
	Inode     uint64
}

func NewStagingEntry(
	path lib.Path,
	fileInfo fs.FileInfo,
	fileSize int64,
	fileHash lib.Sha256,
	blockIds []lib.BlockId,
) (*StagingEntry, error) {
	stat, err := lib.EnhancedStat(fileInfo)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get metadata for %s", path)
	}
	if fileInfo.IsDir() {
		if fileSize != 0 {
			return nil, lib.Errorf("file size mismatch: %d vs 0", fileSize)
		}
	} else {
		if fileInfo.Size() != fileSize {
			return nil, lib.Errorf("file size mismatch: %d vs %d", fileInfo.Size(), fileSize)
		}
	}
	md := lib.NewFileMetadataFromFileInfo(fileInfo, fileHash, blockIds)
	return &StagingEntry{
		RepoPath:  path,
		Metadata:  &md,
		CTimeSec:  stat.CTimeSec,
		CTimeNSec: stat.CTimeNSec,
		Size:      fileSize,
		Inode:     stat.Inode,
	}, nil
}

func (e *StagingEntry) HasChanged(other *StagingEntry) bool {
	return e.CTimeSec != other.CTimeSec || e.CTimeNSec != other.CTimeNSec || e.Inode != other.Inode ||
		e.Size != other.Size
}

func MarshalStagingEntry(e *StagingEntry, w io.Writer) error {
	bw := lib.NewBinaryWriter(w)
	bw.Write(StagingCacheVersion)
	bw.WriteString(e.RepoPath.String())
	if err := lib.MarshalFileMetadata(e.Metadata, w); err != nil {
		return lib.WrapErrorf(err, "failed to marshal file metadata for %s", e.RepoPath)
	}
	bw.Write(e.CTimeSec)
	bw.Write(e.CTimeNSec)
	bw.Write(e.Size)
	bw.Write(e.Inode)
	return bw.Err
}

func UnmarshalStagingEntry(r io.Reader) (*StagingEntry, error) {
	entry := &StagingEntry{} //nolint:exhaustruct
	br := lib.NewBinaryReader(r)
	var version uint16
	br.Read(&version)
	if br.Err == nil && version != StagingCacheVersion {
		return nil, lib.Errorf("unsupported staging cache version: %d", version)
	}
	pathStr := br.ReadString()
	path, err := lib.NewPath(pathStr)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to unmarshal path")
	}
	entry.RepoPath = path
	md, err := lib.UnmarshalFileMetadata(r)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to unmarshal file metadata")
	}
	entry.Metadata = md
	br.Read(&entry.CTimeSec)
	br.Read(&entry.CTimeNSec)
	br.Read(&entry.Size)
	br.Read(&entry.Inode)
	return entry, br.Err
}

func MarshalledStagingCacheSize(e *StagingEntry) int {
	return 2 + // Version
		2 + e.RepoPath.Len() + // Path
		e.Metadata.MarshalledSize() + // Metadata
		8 + // CTimeSec
		4 + // CTimeNSec
		8 + // Size
		8 // Inode
}

func StagingEntryPathFilter(pathFilter lib.PathFilter) func(e *StagingEntry) bool {
	if pathFilter == nil {
		return nil
	}
	return func(e *StagingEntry) bool {
		return pathFilter.Include(e.RepoPath, e.Metadata.ModeAndPerm.IsDir())
	}
}

func StagingEntryPathCompare(a, b *StagingEntry) int {
	return strings.Compare(lib.PathCompareString(a.RepoPath, a.Metadata.ModeAndPerm.IsDir()),
		lib.PathCompareString(b.RepoPath, b.Metadata.ModeAndPerm.IsDir()))
}

func NewStagingCacheWriter(fs lib.FS, maxChunkSize int) *lib.TempWriter[StagingEntry] {
	return lib.NewTempWriter(
		StagingEntryPathCompare,
		MarshalStagingEntry,
		MarshalledStagingCacheSize,
		UnmarshalStagingEntry,
		fs,
		maxChunkSize,
	)
}

func OpenStagingCache(fs lib.FS, maxChunksInCache int) (*lib.TempCache[StagingEntry], error) {
	temp, err := lib.OpenTemp(fs, UnmarshalStagingEntry)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open temp")
	}
	cache, err := lib.NewTempCache(temp, StagingCacheKey, maxChunksInCache)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create new TempCache")
	}
	return cache, nil
}

type StagingCache struct {
	src          lib.FS
	cacheTempDir string
	cacheWriter  *lib.TempWriter[StagingEntry]
	cache        *lib.TempCache[StagingEntry]
}

func NewStagingCache(src lib.FS, useCache bool) (*StagingCache, error) {
	rand, err := lib.RandStr(32)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to generate random string for cache temp dir")
	}
	cacheTempDir := filepath.Join(cacheDir, cacheTempDirPrefix+rand)
	var cacheWriter *lib.TempWriter[StagingEntry]
	var cache *lib.TempCache[StagingEntry]
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

func StagingCacheKey(stagingEntry *StagingEntry) string {
	return lib.PathCompareString(stagingEntry.RepoPath, stagingEntry.Metadata.ModeAndPerm.IsDir())
}

// Return the metadata either from the cache or compute it.
// Update the cache.
func (c *StagingCache) Handle(localPath lib.Path, repoPath lib.Path, fileInfo fs.FileInfo) (*StagingEntry, error) {
	var fileMetadata *lib.FileMetadata
	var stagingEntry *StagingEntry
	var err error
	if c.cache != nil {
		existingEntry, ok, err := c.cache.Get(lib.PathCompareString(repoPath, fileInfo.IsDir()))
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to get entry from cache for %s", localPath)
		}
		if ok && existingEntry.Metadata.Size == fileInfo.Size() {
			stagingEntry, err = NewStagingEntry(
				repoPath,
				fileInfo,
				existingEntry.Metadata.Size,
				existingEntry.Metadata.FileHash,
				existingEntry.Metadata.BlockIds,
			)
			if err != nil {
				return nil, lib.WrapErrorf(err, "failed to create cache entry for %s", localPath)
			}
			if !stagingEntry.HasChanged(existingEntry) {
				md := lib.NewFileMetadataFromFileInfo(
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

func computeFileHash(fs lib.FS, path lib.Path, fileInfo fs.FileInfo) (lib.FileMetadata, error) {
	if fileInfo.IsDir() {
		return lib.NewFileMetadataFromFileInfo(fileInfo, lib.Sha256{}, nil), nil
	}
	f, err := fs.OpenRead(path.String())
	if err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to open file %s", path)
	}
	defer f.Close() //nolint:errcheck
	fileHash := sha256.New()
	if _, err := io.Copy(fileHash, f); err != nil {
		return lib.FileMetadata{}, lib.WrapErrorf(err, "failed to read file %s", path)
	}
	return lib.NewFileMetadataFromFileInfo(fileInfo, lib.Sha256(fileHash.Sum(nil)), nil), nil
}
