package lib

import (
	"context"
)

// Rewrite every revision in the current sort order, keeping the chain intact.
//
// This is temporary, for repositories written before the sort order changed.
// `RevisionReader` rejects those outright, so the entries are read here from
// their blocks instead. Everything it needs is copied rather than opened up in
// the rest of the package, because it goes away with the next release.
//
// Every revision id changes, since an id is the hash of the revision. The old
// blocks are left behind and `check --orphaned-blocks` will report them.
// Workspaces have to be pointed at the new head afterwards.
func RewriteRevisions( //nolint:funlen
	ctx context.Context,
	repository *Repository,
	tempFS FS,
	monitor RevisionSnapshotMonitor,
	tempChunkSize int,
) error {
	head, err := repository.Head(ctx)
	if err != nil {
		return WrapErrorf(err, "failed to get head revision")
	}
	buf := NewBlockBuf()
	type staleRevision struct {
		id       RevisionId
		revision Revision
	}
	chain := []staleRevision{}
	for id := head; !id.IsRoot(); {
		revision, err := repository.ReadRevision(ctx, id, buf)
		if err != nil {
			return WrapErrorf(err, "failed to read revision %s", id)
		}
		chain = append(chain, staleRevision{id, revision})
		id = revision.ParentRevisionId
	}
	// Oldest first, so each revision can name the parent just written for it.
	writeBuf := NewBlockBuf()
	parent := RevisionId{}
	for i := len(chain) - 1; i >= 0; i-- {
		stale := chain[i]
		monitor.OnRevisionStart(stale.id)
		sortFS, err := tempFS.MkSub(stale.id.String())
		if err != nil {
			return WrapErrorf(err, "failed to create temp directory for revision %s", stale.id)
		}
		sorter := NewRevisionEntryTempWriter(sortFS, tempChunkSize)
		for _, blockId := range stale.revision.BlockIds {
			data, err := repository.ReadBlock(ctx, blockId, buf)
			if err != nil {
				return WrapErrorf(err, "failed to read block %s of revision %s", blockId, stale.id)
			}
			chunk, err := UnmarshallRevisionEntryChunk(NewProtobufReader(data))
			if err != nil {
				return WrapErrorf(err, "failed to unmarshall block %s of revision %s", blockId, stale.id)
			}
			for _, entry := range chunk.Entries {
				monitor.OnRevisionEntry(entry)
				if err := sorter.Add(entry); err != nil {
					return WrapErrorf(err, "failed to sort %s of revision %s", entry.Path, stale.id)
				}
			}
		}
		sorted, err := sorter.CloseAndSort()
		if err != nil {
			return WrapErrorf(err, "failed to sort revision %s", stale.id)
		}
		blockIds := make([]BlockId, 0, sorted.Chunks())
		sortedReader := sorted.Reader(nil)
		for c := range sorted.Chunks() {
			entries, err := sortedReader.ReadChunk(c, buf)
			if err != nil {
				return WrapErrorf(err, "failed to read sorted chunk %d of revision %s", c, stale.id)
			}
			chunk := &RevisionEntryChunk{Entries: entries}
			chunkBuf := make([]byte, chunk.MarshallSize())
			pw := NewProtobufWriter(chunkBuf)
			if err := chunk.Marshall(pw); err != nil {
				return WrapErrorf(err, "failed to marshall chunk %d of revision %s", c, stale.id)
			}
			blockId, _, err := repository.WriteBlock(ctx, pw.Bytes(), writeBuf)
			if err != nil {
				return WrapErrorf(err, "failed to write chunk %d of revision %s", c, stale.id)
			}
			blockIds = append(blockIds, blockId)
		}
		_ = sorted.Remove()
		revision := stale.revision
		revision.Magic = RevisionMagic
		revision.ParentRevisionId = parent
		revision.BlockIds = blockIds
		revBuf := make([]byte, revision.MarshallSize())
		pw := NewProtobufWriter(revBuf)
		if err := revision.Marshall(pw); err != nil {
			return WrapErrorf(err, "failed to marshall revision %s", stale.id)
		}
		blockId, _, err := repository.WriteBlock(ctx, pw.Bytes(), writeBuf)
		if err != nil {
			return WrapErrorf(err, "failed to write revision %s", stale.id)
		}
		parent = RevisionId(blockId)
	}
	if parent.IsRoot() {
		return nil
	}
	// Everything above only added blocks, so the repository is still untouched
	// until the head moves. Guard that move the way `WriteRevision` does: a
	// revision committed while this was running is not in `chain`, so moving
	// the head past it would unlink it for good.
	unlock, err := repository.storage.Lock(ctx, UpdateHeadRevisionLockName)
	if err != nil {
		return WrapErrorf(err, "failed to lock the head revision")
	}
	defer unlock() //nolint:errcheck
	current, err := repository.Head(ctx)
	if err != nil {
		return WrapErrorf(err, "failed to re-read head revision")
	}
	if current != head {
		return WrapErrorf(ErrHeadChanged,
			"head moved from %s to %s while rewriting, so nothing was changed", head, current)
	}
	return WriteRef(ctx, repository.storage, "head", parent)
}
