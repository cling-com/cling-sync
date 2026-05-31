package lib

import (
	"context"
	"errors"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"
)

type RepositorySyncMonitor interface {
	OnSrcBlockIdsRead(blocksTotal int)
	OnDstBlockIdsRead(blocksTotal int)
	OnBeforeCopy(srcBlocks, dstBlocks int)
	OnCopyBlock(blockId BlockId, existed bool, length int)
	OnBeforeUpdateDstHead(newHead RevisionId)
}

type RepositorySyncOptions struct {
	Monitor RepositorySyncMonitor
	Workers int
}

const blockIdReadProgressEvery = 1000

// Sync new blocks from src to dst, then advance dst's head to src's.
// Both storages must share the exact same repository config.
func SyncRepository( //nolint:funlen
	ctx context.Context, src, dst Storage, tempFS FS, opts RepositorySyncOptions,
) error {
	if opts.Workers < 1 {
		return Errorf("number of workers must be at least 1")
	}
	srcToml, err := src.Open(ctx)
	if err != nil {
		return WrapErrorf(err, "failed to read src repository config")
	}
	dstToml, err := dst.Open(ctx)
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
	srcHead, err := ReadRef(ctx, src, "head")
	if err != nil {
		return WrapErrorf(err, "failed to read src head")
	}
	dstHead, err := ReadRef(ctx, dst, "head")
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
	srcTemp, err := ReadSortedBlockIds(ctx, src, srcFS, func(id BlockId) {
		srcCount++
		if srcCount%blockIdReadProgressEvery == 0 {
			opts.Monitor.OnSrcBlockIdsRead(srcCount)
		}
		if id == BlockId(srcHead) {
			srcSeenHead = true
		}
	})
	if err != nil {
		return WrapErrorf(err, "failed to snapshot src block ids")
	}
	defer srcTemp.Remove() //nolint:errcheck
	if srcCount%blockIdReadProgressEvery != 0 {
		opts.Monitor.OnSrcBlockIdsRead(srcCount)
	}
	if !srcSeenHead {
		return Errorf("src head %s is not present in src storage", srcHead)
	}
	dstFS, err := tempFS.MkSub("dst")
	if err != nil {
		return WrapErrorf(err, "failed to create temp dir for dst block ids")
	}
	dstCount := 0
	dstTemp, err := ReadSortedBlockIds(ctx, dst, dstFS, func(BlockId) {
		dstCount++
		if dstCount%blockIdReadProgressEvery == 0 {
			opts.Monitor.OnDstBlockIdsRead(dstCount)
		}
	})
	if err != nil {
		return WrapErrorf(err, "failed to snapshot dst block ids")
	}
	defer dstTemp.Remove() //nolint:errcheck
	if dstCount%blockIdReadProgressEvery != 0 {
		opts.Monitor.OnDstBlockIdsRead(dstCount)
	}
	dstCache, err := NewTempCache(dstTemp, func(id BlockId) string { return string(id[:]) }, 4)
	if err != nil {
		return WrapErrorf(err, "failed to open dst block id cache")
	}
	opts.Monitor.OnBeforeCopy(srcCount, dstCount)
	// A pool of workers copies each block missing from dst: read it from src,
	// write it to dst.
	g, gctx := errgroup.WithContext(ctx)
	ids := make(chan BlockId, opts.Workers)
	// Workers call OnCopyBlock concurrently, so serialize it. Every other monitor
	// call runs on this goroutine alone.
	var copyMu sync.Mutex
	for range opts.Workers {
		g.Go(func() error {
			// Each worker owns its BlockBuf because ReadBlock returns a slice that aliases it.
			blockBuf := NewBlockBuf()
			for id := range ids {
				data, err := src.ReadBlock(gctx, id, blockBuf)
				if err != nil {
					return WrapErrorf(err, "failed to read block %s from src", id)
				}
				existed, err := dst.WriteBlock(gctx, id, data)
				if err != nil {
					return WrapErrorf(err, "failed to write block %s to dst", id)
				}
				copyMu.Lock()
				opts.Monitor.OnCopyBlock(id, existed, len(data))
				copyMu.Unlock()
			}
			return nil
		})
	}
	// The dispatcher streams the ids of blocks missing from dst into `ids`. It
	// runs in the group alongside the workers so a failure on either side cancels
	// gctx and unblocks the other.
	g.Go(func() error {
		defer close(ids)
		reader := srcTemp.Reader(nil)
		buf := NewBlockBuf()
		for {
			id, err := reader.Read(buf)
			if errors.Is(err, io.EOF) {
				return nil
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
			select {
			case ids <- id:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})
	if err := g.Wait(); err != nil {
		return err //nolint:wrapcheck
	}
	unlock, err := dst.Lock(ctx, UpdateHeadRevisionLockName)
	if err != nil {
		return WrapErrorf(err, "failed to lock dst head")
	}
	defer unlock() //nolint:errcheck
	latestDstHead, err := ReadRef(ctx, dst, "head")
	if err != nil {
		return WrapErrorf(err, "failed to re-read dst head")
	}
	if latestDstHead != dstHead {
		return Errorf("dst head revision changed during sync")
	}
	opts.Monitor.OnBeforeUpdateDstHead(srcHead)
	if err := WriteRef(ctx, dst, "head", srcHead); err != nil {
		return WrapErrorf(err, "failed to write dst head reference")
	}
	return nil
}
