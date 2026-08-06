// A revision snapshot represents a sorted list of all effective RevisionEntries
// for a given revision.
// It is created by reading all revisions from the given revision to the root
// revision, and then merging the revisions together.
package lib

import (
	"context"
	"errors"
	"io"
)

// Building a snapshot reads every revision down to the root, so it is the
// longest silent phase of most commands.
type RevisionSnapshotMonitor interface {
	OnRevisionStart(revisionId RevisionId)
	OnRevisionEntry(entry *RevisionEntry)
}

func NewRevisionSnapshot(
	ctx context.Context,
	repository *Repository,
	revisionId RevisionId,
	tmpFS FS,
	mon RevisionSnapshotMonitor,
) (*Temp[*RevisionEntry], error) {
	// Build a list of all revisions.
	revisions := make([]*Revision, 0)
	r := revisionId
	buf := NewBlockBuf()
	for !r.IsRoot() {
		mon.OnRevisionStart(r)
		revision, err := repository.ReadRevision(ctx, r, buf)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read revision: %s", r)
		}
		revisions = append(revisions, &revision)
		r = revision.ParentRevisionId
	}
	tempWriter := NewRevisionEntryTempWriter(tmpFS, DefaultTempChunkSize)
	if err := revisionNWayMerge(ctx, repository, revisions, tempWriter, buf, mon); err != nil {
		return nil, WrapErrorf(err, "failed to revision n-way merge revisions")
	}
	// todo: we don't need to call `tempWriter.Finalize()` because the entries
	// are already sorted.
	temp, err := tempWriter.Finalize()
	if err != nil {
		return nil, WrapErrorf(err, "failed to finalize temporary file")
	}
	return temp, nil
}

// Merge the entries of `revisions`, newest first, into `tempWriter`.
//
// Each revision lists its entries in `RevisionEntryPathCompare` order, so one
// k-way merge visits every path once. Where several revisions hold a path the
// newest one wins, and if that entry is a delete the path is left out.
//
// Memory is one cursor and one decoded block per revision. Nothing grows with
// the number of paths.
func revisionNWayMerge(
	ctx context.Context,
	repository *Repository,
	revisions []*Revision,
	tempWriter *TempWriter[*RevisionEntry],
	buf BlockBuf,
	mon RevisionSnapshotMonitor,
) error {
	// One revision's position in the merge.
	type mergeCursor struct {
		entry  *RevisionEntry
		reader *RevisionReader
		// Position in the revision list. Lowest is newest and wins a tie.
		index int
	}

	// Ordered by path, then by revision index so the newest revision holding a
	// path comes out first.
	queue := NewHeap(func(a, b mergeCursor) int {
		if c := RevisionEntryPathCompare(a.entry, b.entry); c != 0 {
			return c
		}
		return a.index - b.index
	}, len(revisions))
	// Put the cursor's next entry back into the queue. A revision that has no
	// entries left drops out of the merge.
	advance := func(c mergeCursor) error {
		entry, err := c.reader.Read(ctx, buf)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return WrapErrorf(err, "failed to read revision entry")
		}
		mon.OnRevisionEntry(entry)
		c.entry = entry
		queue.Push(c)
		return nil
	}
	for i, revision := range revisions {
		cursor := mergeCursor{entry: nil, reader: NewRevisionReader(repository, revision), index: i}
		if err := advance(cursor); err != nil {
			return err
		}
	}
	for queue.Len() > 0 {
		winner := queue.Pop()
		if winner.entry.Kind != RevisionEntryKindDelete {
			if err := tempWriter.Add(winner.entry); err != nil {
				return WrapErrorf(err, "failed to write entry %s", winner.entry.Path)
			}
		}
		// Any other revision holding the same entry is outdated. A revision
		// lists each entry once, so what replaces them sorts strictly after the
		// winner and cannot be mistaken for it. Comparing paths alone would be
		// wrong, because a file and a directory of one path are different
		// entries.
		for queue.Len() > 0 && RevisionEntryPathCompare(queue.Peek().entry, winner.entry) == 0 {
			if err := advance(queue.Pop()); err != nil {
				return err
			}
		}
		if err := advance(winner); err != nil {
			return err
		}
	}
	return nil
}
