package lib

import (
	"errors"
	"io"
)

type HealthCheckMonitor interface {
	OnRevisionStart(revisionId RevisionId)
	OnRevisionEntry(entry *RevisionEntry)
	OnBlockVerified(blockId BlockId, length int)
	OnOrphanedBlock(blockId BlockId)
}

type HealthCheckOptions struct {
	Monitor HealthCheckMonitor
	// Read and decrypt every block referenced by any revision.
	CheckBlocks bool
	// Report every block in storage that is not referenced by any revision.
	CheckOrphanedBlocks bool
}

// CheckHealth verifies the integrity of `repository`.
//
// It always traverses the entire revision chain (head to root), checking that
// every revision can be read and that every revision's path entries are
// strictly sorted. Additional checks can be enabled via `opts`.
func CheckHealth(repository *Repository, tempFS FS, opts HealthCheckOptions) error {
	var seenWriter *TempWriter[BlockId]
	if opts.CheckBlocks || opts.CheckOrphanedBlocks {
		seenFS, err := tempFS.MkSub("seen")
		if err != nil {
			return WrapErrorf(err, "failed to create temp directory for seen block ids")
		}
		seenWriter = NewTempWriterWithIgnoreDuplicates[BlockId](
			BlockIdCompare,
			blockIdChunkMarshaller{},
			seenFS,
			DefaultTempChunkSize,
		)
	}
	if err := walkRevisions(repository, opts.Monitor, seenWriter); err != nil {
		return err
	}
	if seenWriter == nil {
		return nil
	}
	seen, err := seenWriter.Finalize()
	if err != nil {
		return WrapErrorf(err, "failed to sort seen block ids")
	}
	defer seen.Remove() //nolint:errcheck
	if opts.CheckOrphanedBlocks {
		if err := checkOrphanedBlocks(repository, tempFS, opts.Monitor, seen); err != nil {
			return err
		}
	}
	if opts.CheckBlocks {
		if err := checkBlocks(repository, opts.Monitor, seen); err != nil {
			return err
		}
	}
	return nil
}

//nolint:funlen
func walkRevisions(repository *Repository, monitor HealthCheckMonitor, seen *TempWriter[BlockId]) error {
	revisionId, err := repository.Head()
	if err != nil {
		return WrapErrorf(err, "failed to get head revision")
	}
	blockBuf := NewBlockBuf()
	for !revisionId.IsRoot() {
		monitor.OnRevisionStart(revisionId)
		if seen != nil {
			// The revision is itself stored as a block whose id equals revisionId.
			if err := seen.Add(BlockId(revisionId)); err != nil {
				return WrapErrorf(err, "failed to record revision block %s", revisionId)
			}
		}
		revision, err := repository.ReadRevision(revisionId, blockBuf)
		if err != nil {
			return WrapErrorf(err, "failed to read revision %s", revisionId)
		}
		if seen != nil {
			for _, blockId := range revision.BlockIds {
				if err := seen.Add(blockId); err != nil {
					return WrapErrorf(err, "failed to record block id %s of revision %s", blockId, revisionId)
				}
			}
		}
		reader := NewRevisionReader(repository, &revision)
		var lastEntry *RevisionEntry
		entryCount := 0
		for {
			entry, err := reader.Read(blockBuf)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return WrapErrorf(err, "failed to read revision entry #%d of revision %s", entryCount, revisionId)
			}
			entryCount++
			if lastEntry != nil && RevisionEntryPathCompare(lastEntry, entry) >= 0 {
				return Errorf("paths of revision %s are not strictly sorted at position %d: %s >= %s",
					revisionId, entryCount, lastEntry.Path, entry.Path)
			}
			if entry.Metadata.FileMode.IsSymlink() && entry.Metadata.SymLinkTarget == nil {
				return Errorf("entry %s in revision %s is a symlink but has no SymLinkTarget",
					entry.Path, revisionId)
			}
			if !entry.Metadata.FileMode.IsSymlink() && entry.Metadata.SymLinkTarget != nil {
				return Errorf("entry %s in revision %s has SymLinkTarget but is not a symlink",
					entry.Path, revisionId)
			}
			monitor.OnRevisionEntry(entry)
			if seen != nil {
				for _, blockId := range entry.Metadata.BlockIds {
					if err := seen.Add(blockId); err != nil {
						return WrapErrorf(err,
							"failed to record block id %s of path %s of revision %s", blockId, entry.Path, revisionId)
					}
				}
			}
			lastEntry = entry
		}
		revisionId = revision.ParentRevisionId
	}
	return nil
}

func checkOrphanedBlocks(repository *Repository, tempFS FS, monitor HealthCheckMonitor, seen *Temp[BlockId]) error {
	// Read all block ids.
	storedFS, err := tempFS.MkSub("stored")
	if err != nil {
		return WrapErrorf(err, "failed to create temp directory for stored block ids")
	}
	storedWriter := NewTempWriterWithIgnoreDuplicates[BlockId](
		BlockIdCompare,
		blockIdChunkMarshaller{},
		storedFS,
		DefaultTempChunkSize,
	)
	err = repository.storage.ReadBlockIds(func(id BlockId) error {
		return storedWriter.Add(id)
	})
	if err != nil {
		return WrapErrorf(err, "failed to read storage block ids")
	}
	stored, err := storedWriter.Finalize()
	if err != nil {
		return WrapErrorf(err, "failed to sort stored block ids")
	}
	defer stored.Remove() //nolint:errcheck

	// Keep seen block ids in a cache for lookup.
	seenCache, err := NewTempCache(seen, func(id BlockId) string { return string(id[:]) }, 1)
	if err != nil {
		return WrapErrorf(err, "failed to open seen cache")
	}

	// Go through all blocks and report those not in `seen`.
	reader := stored.Reader(nil)
	buf := NewBlockBuf()
	for {
		id, err := reader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return WrapErrorf(err, "failed to read stored block id")
		}
		_, ok, err := seenCache.Get(string(id[:]))
		if err != nil {
			return WrapErrorf(err, "failed to look up block id %s in seen cache", id)
		}
		if !ok {
			monitor.OnOrphanedBlock(id)
		}
	}
	return nil
}

func checkBlocks(repository *Repository, monitor HealthCheckMonitor, seen *Temp[BlockId]) error {
	reader := seen.Reader(nil)
	buf := NewBlockBuf()
	for {
		id, err := reader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return WrapErrorf(err, "failed to read seen block id")
		}
		data, err := repository.ReadBlock(id, buf)
		if err != nil {
			return WrapErrorf(err, "failed to verify block %s", id)
		}
		monitor.OnBlockVerified(id, len(data))
	}
	return nil
}
