package lib

import (
	"crypto/sha256"
	"errors"
	"hash"
	"io"
)

type HealthCheckMonitor interface {
	OnRevisionStart(revisionId RevisionId)
	OnBlockOk(blockId BlockId, duplicate bool, length int)
	OnRevisionEntry(entry *RevisionEntry)
}

type HealthCheckOptions struct {
	Monitor    HealthCheckMonitor
	DataBlocks bool
}

// Check that the revision chain is intact, i.e. all revisions are reachable from the
// given revision.
func CheckHealth(repository *Repository, opts HealthCheckOptions) error { //nolint:funlen
	blocksSeen := make(map[BlockId]bool)
	revisionId, err := repository.Head()
	if err != nil {
		return WrapErrorf(err, "failed to get head revision")
	}
	for !revisionId.IsRoot() {
		opts.Monitor.OnRevisionStart(revisionId)
		revision, err := repository.ReadRevision(revisionId)
		if err != nil {
			return WrapErrorf(err, "failed to read revision %s", revisionId)
		}
		for _, blockId := range revision.Blocks {
			length, duplicate, err := VerifyBlock(repository, blocksSeen, nil, blockId)
			if err != nil {
				return WrapErrorf(err, "failed to check block %s of revision %s", blockId, revisionId)
			}
			opts.Monitor.OnBlockOk(blockId, duplicate, length)
		}
		reader := NewRevisionReader(repository, &revision)
		var lastEntry *RevisionEntry
		entryCount := 0
		for {
			entry, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return WrapErrorf(err, "failed to read revision entry #%d or revision %s", entryCount, revisionId)
			}
			opts.Monitor.OnRevisionEntry(entry)
			entryCount++
			if lastEntry != nil && RevisionEntryPathCompare(lastEntry, entry) > 0 {
				return Errorf("paths of revision %s are not sorted at position %d: %s > %s",
					revisionId, entryCount, lastEntry.Path, entry.Path)
			}
			if opts.DataBlocks {
				var fileSize int64 = 0
				fileHash := sha256.New()
				for _, blockId := range entry.Metadata.BlockIds {
					length, duplicate, err := VerifyBlock(repository, blocksSeen, fileHash, blockId)
					if err != nil {
						return WrapErrorf(
							err,
							"failed to check block %s of path %s of revision %s",
							blockId,
							entry.Path,
							revisionId,
						)
					}
					opts.Monitor.OnBlockOk(blockId, duplicate, length)
					fileSize += int64(length)
				}
				if entry.Metadata.Size != fileSize {
					return Errorf("file size mismatch for path %s of revision %s: want %d, got %d",
						entry.Path, revisionId, entry.Metadata.Size, fileSize)
				}
				expectedHash := Sha256(fileHash.Sum(nil))
				if entry.Metadata.ModeAndPerm.IsDir() {
					// Directories have no hash.
					expectedHash = Sha256{}
				}
				if expectedHash != entry.Metadata.FileHash {
					return Errorf("file hash mismatch for path %s of revision %s: want %s, got %s",
						entry.Path, revisionId, expectedHash, entry.Metadata.FileHash)
				}
			}
			lastEntry = entry
		}
		revisionId = revision.Parent
	}
	return nil
}

// Check that the block can be read and decrypted/uncompressed.
// Return the size of the unencrypted/uncompressed data.
func VerifyBlock(
	repository *Repository,
	seen map[BlockId]bool,
	fileHash hash.Hash,
	blockId BlockId,
) (int, bool, error) {
	duplicate := seen[blockId]
	data, header, err := repository.ReadBlock(blockId)
	if err != nil {
		return 0, false, WrapErrorf(err, "failed to read block %s", blockId)
	}
	if header.BlockId != blockId {
		return 0, false, Errorf("block id mismatch: want %s, got %s", blockId, header.BlockId)
	}
	if fileHash != nil {
		if _, err := fileHash.Write(data); err != nil {
			return 0, false, WrapErrorf(err, "failed to hash block %s", blockId)
		}
	}
	// todo: We should warn if the number of blocks gets too high and `seen` consumes too much memory.
	seen[blockId] = true
	return len(data), duplicate, nil
}
