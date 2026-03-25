package lib

import (
	"context"
	"errors"
	"io"
)

type RepositorySyncMonitor interface {
	OnRevisionStart(revisionId RevisionId)
	OnCopyBlock(blockId BlockId, existed bool, length int)
	OnRevisionEntry(entry *RevisionEntry)
	OnBeforeUpdateDstHead(newHead RevisionId)
}

type RepositorySyncOptions struct {
	Monitor RepositorySyncMonitor
}

// Copy blocks from src to dst for revisions that are newer in src.
// src and dst must have *exactly* the same repository config.
func SyncRepository( //nolint:funlen
	ctx context.Context, src *Repository, dst Storage, opts RepositorySyncOptions,
) error {
	srcToml, err := src.storage.Open()
	if err != nil {
		return WrapErrorf(err, "failed to read src repository config")
	}
	dstToml, err := dst.Open()
	if err != nil {
		return WrapErrorf(err, "failed to read dst repository config")
	}
	if !srcToml.Eq(dstToml) {
		return Errorf("src and dst repository must share the exact same configuration")
	}
	srcRevisionId, err := src.Head()
	if err != nil {
		return WrapErrorf(err, "failed to get source head revision")
	}
	dstRevisionId, err := ReadRef(dst, "head")
	if err != nil {
		return WrapErrorf(err, "failed to get destination head revision")
	}
	if srcRevisionId == dstRevisionId {
		return nil
	}
	// todo: src and dst must share the same repository config.
	// Make sure the current HEAD of the destination repository is in the list
	// of revisions of the source repository and find the base.
	revisionsToSync := map[RevisionId]*Revision{}
	baseRefId := srcRevisionId
	for {
		revision, err := src.ReadRevision(baseRefId)
		if err != nil {
			return WrapErrorf(err, "failed to read revision %s", baseRefId)
		}
		revisionsToSync[baseRefId] = &revision
		baseRefId = revision.Parent
		if revision.Parent == dstRevisionId {
			break
		}
		if revision.Parent.IsRoot() {
			return WrapErrorf(err, "destination and source don't have a common revision")
		}
	}
	blocksSeen := make(map[BlockId]bool)
	copyBlock := func(blockId BlockId) error {
		if _, ok := blocksSeen[blockId]; ok {
			return nil
		}
		data, header, err := src.storage.ReadBlock(blockId)
		if err != nil {
			return WrapErrorf(err, "failed to read block %s", blockId)
		}
		existed, err := dst.WriteBlock(Block{header, data})
		if err != nil {
			return WrapErrorf(err, "failed to write block %s", blockId)
		}
		opts.Monitor.OnCopyBlock(blockId, existed, BlockHeaderSize+len(data))
		blocksSeen[blockId] = true
		return nil
	}
	for revisionId, revision := range revisionsToSync {
		opts.Monitor.OnRevisionStart(revisionId)
		reader := NewRevisionReader(src, revision)
		entryCount := 0
		for {
			entry, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return WrapErrorf(err, "failed to read revision entry #%d of revision %s", entryCount, revisionId)
			}
			entryCount += 1
			opts.Monitor.OnRevisionEntry(entry)
			for _, blockId := range entry.Metadata.BlockIds {
				if err := copyBlock(blockId); err != nil {
					return WrapErrorf(
						err,
						"failed to copy block %s of path %s of revision %s",
						blockId,
						entry.Path,
						revisionId,
					)
				}
			}
		}
		if err := copyBlock(BlockId(revisionId)); err != nil {
			return err
		}
		for _, blockId := range revision.Blocks {
			if err := copyBlock(blockId); err != nil {
				return err
			}
		}
	}
	// Advance head revision on dst but only if dst did not change.
	unlockDst, err := dst.Lock(ctx, UpdateHeadRevisionLockName)
	if err != nil {
		return WrapErrorf(err, "failed to create lock in dst")
	}
	defer unlockDst() //nolint:errcheck
	latestDstRevisionId, err := ReadRef(dst, "head")
	if err != nil {
		return WrapErrorf(err, "failed to get destination head revision")
	}
	if latestDstRevisionId != dstRevisionId {
		return Errorf("dst head revision changed during sync")
	}
	opts.Monitor.OnBeforeUpdateDstHead(srcRevisionId)
	if err := WriteRef(dst, "head", srcRevisionId); err != nil {
		return WrapErrorf(err, "failed to write dst head reference")
	}
	return nil
}
