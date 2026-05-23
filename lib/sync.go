package lib

import (
	"context"
	"errors"
	"io"
)

type RepositorySyncMonitor interface {
	OnBeforeCopy(srcBlocks, dstBlocks int)
	OnCopyBlock(blockId BlockId, existed bool, length int)
	OnBeforeUpdateDstHead(newHead RevisionId)
}

type RepositorySyncOptions struct {
	Monitor RepositorySyncMonitor
}

// Sync new blocks from src to dst, then advance dst's head to src's.
// Both storages must share the exact same repository config.
func SyncRepository( //nolint:funlen
	ctx context.Context, src, dst Storage, tempFS FS, opts RepositorySyncOptions,
) error {
	srcToml, err := src.Open()
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
	// Read srcHead before listing src block ids: the listing is then
	// guaranteed to be a superset of everything reachable from srcHead.
	// The other order risks pointing dst at a head whose blocks were not
	// yet committed when we listed.
	srcHead, err := ReadRef(src, "head")
	if err != nil {
		return WrapErrorf(err, "failed to read src head")
	}
	dstHead, err := ReadRef(dst, "head")
	if err != nil {
		return WrapErrorf(err, "failed to read dst head")
	}
	if srcHead == dstHead {
		return nil
	}
	if srcHead.IsRoot() {
		return Errorf("src has no committed revisions")
	}
	srcFS, err := tempFS.MkSub("src")
	if err != nil {
		return WrapErrorf(err, "failed to create temp dir for src block ids")
	}
	srcCount := 0
	srcSeenHead := false
	srcTemp, err := ReadSortedBlockIds(src, srcFS, func(id BlockId) {
		srcCount++
		if id == BlockId(srcHead) {
			srcSeenHead = true
		}
	})
	if err != nil {
		return WrapErrorf(err, "failed to snapshot src block ids")
	}
	defer srcTemp.Remove() //nolint:errcheck
	if !srcSeenHead {
		return Errorf("src head %s is not present in src storage", srcHead)
	}
	dstFS, err := tempFS.MkSub("dst")
	if err != nil {
		return WrapErrorf(err, "failed to create temp dir for dst block ids")
	}
	dstCount := 0
	dstTemp, err := ReadSortedBlockIds(dst, dstFS, func(BlockId) { dstCount++ })
	if err != nil {
		return WrapErrorf(err, "failed to snapshot dst block ids")
	}
	defer dstTemp.Remove() //nolint:errcheck
	dstCache, err := NewTempCache(dstTemp, func(id BlockId) string { return string(id[:]) }, 4)
	if err != nil {
		return WrapErrorf(err, "failed to open dst block id cache")
	}
	opts.Monitor.OnBeforeCopy(srcCount, dstCount)
	reader := srcTemp.Reader(nil)
	buf := NewBlockBuf()
	blockBuf := NewBlockBuf()
	for {
		id, err := reader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return WrapErrorf(err, "failed to read src block id")
		}
		_, present, err := dstCache.Get(string(id[:]))
		if err != nil {
			return WrapErrorf(err, "failed to look up block %s in dst", id)
		}
		if present {
			continue
		}
		data, err := src.ReadBlock(id, blockBuf)
		if err != nil {
			return WrapErrorf(err, "failed to read block %s from src", id)
		}
		existed, err := dst.WriteBlock(id, data)
		if err != nil {
			return WrapErrorf(err, "failed to write block %s to dst", id)
		}
		opts.Monitor.OnCopyBlock(id, existed, len(data))
	}
	unlock, err := dst.Lock(ctx, UpdateHeadRevisionLockName)
	if err != nil {
		return WrapErrorf(err, "failed to lock dst head")
	}
	defer unlock() //nolint:errcheck
	latestDstHead, err := ReadRef(dst, "head")
	if err != nil {
		return WrapErrorf(err, "failed to re-read dst head")
	}
	if latestDstHead != dstHead {
		return Errorf("dst head revision changed during sync")
	}
	opts.Monitor.OnBeforeUpdateDstHead(srcHead)
	if err := WriteRef(dst, "head", srcHead); err != nil {
		return WrapErrorf(err, "failed to write dst head reference")
	}
	return nil
}
