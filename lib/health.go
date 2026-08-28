package lib

import (
	"bytes"
	"context"
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
	// Read and decrypt every block referenced by any revision, check that each
	// block's id is the HMAC of its content, and check that no two block
	// headers were encrypted with the same nonce.
	CheckBlocks bool
	// Report every block in storage that is not referenced by any revision.
	CheckOrphanedBlocks bool
}

// CheckHealth verifies the integrity of `repository`.
//
// It always traverses the entire revision chain (head to root), checking that
// every revision can be read and that every revision's path entries are
// strictly sorted. Additional checks can be enabled via `opts`.
//
// The repository must be backed by a direct storage (e.g. a `FileStorage`),
// never by a caching one: the check would verify the cache instead of the
// repository.
func CheckHealth(ctx context.Context, repository *Repository, tempFS FS, opts HealthCheckOptions) error {
	return checkHealth(ctx, repository, tempFS, opts, DefaultTempChunkSize)
}

// `nonceChunkSize` bounds the chunks of the block header nonce writer. It is a
// parameter only so tests can force the writer to spill into several chunks.
func checkHealth(
	ctx context.Context,
	repository *Repository,
	tempFS FS,
	opts HealthCheckOptions,
	nonceChunkSize int,
) error {
	var seenWriter *TempWriter[BlockId]
	if opts.CheckBlocks || opts.CheckOrphanedBlocks {
		seenFS, err := tempFS.MkSub("seen")
		if err != nil {
			return WrapErrorf(err, "failed to create temp directory for seen block ids")
		}
		seenWriter = NewBlockIdTempWriter(seenFS)
	}
	if err := walkRevisions(ctx, repository, opts.Monitor, seenWriter); err != nil {
		return err
	}
	if seenWriter == nil {
		return nil
	}
	seen, err := seenWriter.CloseAndSort()
	if err != nil {
		return WrapErrorf(err, "failed to sort seen block ids")
	}
	defer seen.Remove() //nolint:errcheck
	if opts.CheckOrphanedBlocks {
		if err := checkOrphanedBlocks(ctx, repository, tempFS, opts.Monitor, seen); err != nil {
			return err
		}
	}
	if opts.CheckBlocks {
		if err := checkBlocks(ctx, repository, tempFS, opts.Monitor, seen, nonceChunkSize); err != nil {
			return err
		}
	}
	return nil
}

//nolint:funlen
func walkRevisions(
	ctx context.Context,
	repository *Repository,
	monitor HealthCheckMonitor,
	seen *TempWriter[BlockId],
) error {
	revisionId, err := repository.Head(ctx)
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
		revision, err := repository.ReadRevision(ctx, revisionId, blockBuf)
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
			entry, err := reader.Read(ctx, blockBuf)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return WrapErrorf(err, "failed to read revision entry #%d of revision %s", entryCount, revisionId)
			}
			entryCount++
			if lastEntry != nil && lastEntry.PathCompare(entry) >= 0 {
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

func checkOrphanedBlocks(
	ctx context.Context,
	repository *Repository,
	tempFS FS,
	monitor HealthCheckMonitor,
	seen *Temp[BlockId],
) error {
	storedFS, err := tempFS.MkSub("stored")
	if err != nil {
		return WrapErrorf(err, "failed to create temp directory for stored block ids")
	}
	stored, err := ReadSortedBlockIds(ctx, repository.storage, storedFS, nil)
	if err != nil {
		return WrapErrorf(err, "failed to snapshot storage block ids")
	}
	defer stored.Remove() //nolint:errcheck

	// Keep seen block ids in a cache for lookup.
	seenCache, err := NewTempCache(seen, func(id BlockId) BlockId { return id }, BlockIdCompare, 1)
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
		_, ok, err := seenCache.Get(id)
		if err != nil {
			return WrapErrorf(err, "failed to look up block id %s in seen cache", id)
		}
		if !ok {
			monitor.OnOrphanedBlock(id)
		}
	}
	return nil
}

// Decrypt every block in `seen`, verify that its id is the HMAC of its
// content, and make sure that no two block headers were encrypted with the
// same nonce. Every header is encrypted with the repository KEK, so a reused
// nonce would break the confidentiality of both blocks.
func checkBlocks(
	ctx context.Context,
	repository *Repository,
	tempFS FS,
	monitor HealthCheckMonitor,
	seen *Temp[BlockId],
	nonceChunkSize int,
) error {
	nonceFS, err := tempFS.MkSub("nonces")
	if err != nil {
		return WrapErrorf(err, "failed to create temp directory for block header nonces")
	}
	// The writer rejects duplicates on its own, so writing every nonce to it is
	// the whole check.
	compare := func(a, b Sha256) int { return bytes.Compare(a[:], b[:]) }
	nonces := NewTempWriter(compare, bytes32ChunkMarshaller[Sha256]{}, nonceFS, nonceChunkSize)
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
		// An integrity check must verify what the repository actually holds,
		// never a cached copy.
		block, err := repository.readBlockEnvelope(ctx, id, buf, ReadBlockOpts{AuthoritativeHint: true})
		if err != nil {
			return WrapErrorf(err, "failed to verify block %s", id)
		}
		data, err := repository.decryptBlock(block, id)
		if err != nil {
			return WrapErrorf(err, "failed to verify block %s", id)
		}
		if actualId := BlockId(CalculateHmac(data, repository.blockIdHmacKey)); actualId != id {
			return Errorf("id of block %s does not match the HMAC of its content (%s)", id, actualId)
		}
		// Nonces are not secret, but hashing them keeps raw crypto material out
		// of the temporary files this check spills to disk.
		digest := CalculateSha256(block.EncryptedHeader[:nonceSize])
		if err := nonces.Add(digest); err != nil {
			return wrapNonceCheckError(err)
		}
		monitor.OnBlockVerified(id, len(data))
	}
	sorted, err := nonces.CloseAndSort()
	if err != nil {
		return wrapNonceCheckError(err)
	}
	_ = sorted.Remove()
	return nil
}

func wrapNonceCheckError(err error) error {
	if errors.Is(err, ErrDuplicateTempEntry) {
		return WrapErrorf(err, "the same nonce is used by more than one block header")
	}
	return WrapErrorf(err, "failed to check block headers for nonce reuse")
}
