// A revision snapshot represents a sorted list of all effective RevisionEntries
// for a given revision.
// It is created by reading all revisions from the given revision to the root
// revision, and then merging the revisions together.
package lib

import (
	"errors"
	"io"
	"slices"
)

func NewRevisionSnapshot(repository *Repository, revisionId RevisionId, tmpFS FS) (*Temp[RevisionEntry], error) {
	// Build a list of all revisions.
	revisions := make([]*Revision, 0)
	r := revisionId
	for !r.IsRoot() {
		revision, err := repository.ReadRevision(r)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read revision: %s", r)
		}
		revisions = append(revisions, &revision)
		r = revision.Parent
	}
	tempWriter := NewRevisionEntryTempWriter(tmpFS, DefaultTempChunkSize)
	if err := revisionNWayMerge(repository, revisions, tempWriter); err != nil {
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

func revisionNWayMerge(
	repository *Repository,
	revisions []*Revision,
	tempWriter *TempWriter[RevisionEntry],
) error {
	readers := make([]*RevisionReader, len(revisions))
	heap := []*RevisionEntry{}
	for i, revision := range revisions {
		readers[i] = NewRevisionReader(repository, revision)
		re, err := readers[i].Read()
		if errors.Is(err, io.EOF) {
			continue
		}
		if err != nil {
			return WrapErrorf(err, "failed to read revision")
		}
		heap = append(heap, re)
	}
	// We are done if the heap only contains `nil` values.
	for slices.IndexFunc(heap, func(e *RevisionEntry) bool { return e != nil }) != -1 {
		// Find the smallest revision entry (by path).
		// Making sure to use RevisionEntryPathCompare to guarantee our established sorting order.
		smallest := heap[0]
		for _, re := range heap {
			if re == nil {
				continue
			}
			if smallest == nil || RevisionEntryPathCompare(re, smallest) == -1 {
				smallest = re
			}
		}
		fullPath := smallest.Path
		// Find the newest entry and read the next entries for all revisions
		// that match the fullPath
		var newest *RevisionEntry
		for i, re := range heap {
			if re != nil && re.Path == fullPath {
				if newest == nil {
					newest = re
				}
				re, err := readers[i].Read()
				if errors.Is(err, io.EOF) {
					heap[i] = nil
					continue
				}
				if err != nil {
					return WrapErrorf(err, "failed to read revision")
				}
				heap[i] = re
			}
		}
		if newest.Type != RevisionEntryDelete {
			if err := tempWriter.Add(newest); err != nil {
				return WrapErrorf(err, "failed to write entry")
			}
		}
	}
	return nil
}
