// A revision snapshot contains the list of all paths present in a given revision.
// It does this by building a list of all revisions from the given revision to the root
// revision, and then merging the revisions together.
package lib

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
)

type RevisionSnapshot struct {
	RevisionId RevisionId
	targetFile string
	tmpDir     string
	reader     *RevisionSnapshotReader
}

func NewRevisionSnapshot(repository *Repository, revision RevisionId, tmpDir string) (*RevisionSnapshot, error) {
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read temporary directory %s", tmpDir)
	}
	if len(files) > 0 {
		return nil, Errorf("temporary directory %s is not empty", tmpDir)
	}
	targetFile := filepath.Join(tmpDir, "snapshot")
	// Build a list of all revisions.
	revisions := make([]*Revision, 0)
	r := revision
	blockBuf := BlockBuf{}
	for !r.IsRoot() {
		revision, err := repository.ReadRevision(r, blockBuf)
		if err != nil {
			return nil, WrapErrorf(err, "failed to read revision: %s", r)
		}
		revisions = append(revisions, &revision)
		r = revision.Parent
	}
	// todo: encrypt the temporary file
	if err := revisionNWayMerge(repository, revisions, targetFile); err != nil {
		return nil, WrapErrorf(err, "failed to revision n-way merge revisions")
	}
	return &RevisionSnapshot{revision, targetFile, tmpDir, nil}, nil
}

func (rs *RevisionSnapshot) Close() error {
	defer func() {
		os.Remove(rs.targetFile) //nolint:errcheck,gosec
		os.RemoveAll(rs.tmpDir)  //nolint:errcheck,gosec
	}()
	if rs.reader != nil {
		if err := rs.reader.file.Close(); err != nil {
			return WrapErrorf(err, "failed to close revision snapshot reader")
		}
		rs.reader = nil
	}
	return nil
}

func (rs *RevisionSnapshot) Reader(ignore []PathPattern) (*RevisionSnapshotReader, error) {
	if rs.reader != nil {
		return nil, Errorf("reader already created")
	}
	file, err := os.Open(rs.targetFile)
	if err != nil {
		return nil, WrapErrorf(err, "failed to open revision snapshot")
	}
	bufReader := bufio.NewReader(file)
	rs.reader = &RevisionSnapshotReader{file: file, bufReader: *bufReader, ignore: ignore}
	return rs.reader, nil
}

type RevisionSnapshotReader struct {
	file      *os.File
	bufReader bufio.Reader
	ignore    []PathPattern
}

// Return `io.EOF` if we are done.
func (rs *RevisionSnapshotReader) Read() (*RevisionEntry, error) {
	for {
		re, err := UnmarshalRevisionEntry(&rs.bufReader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, WrapErrorf(err, "failed to read revision snapshot")
		}
		if slices.IndexFunc(rs.ignore, func(p PathPattern) bool { return p.Match(re.Path.FSString()) }) != -1 {
			continue
		}
		return re, nil
	}
}

func revisionNWayMerge(repository *Repository, revisions []*Revision, targetFile string) error {
	file, err := os.OpenFile(targetFile, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return WrapErrorf(err, "failed to open target file for writing")
	}
	defer file.Close() //nolint:errcheck
	w := bufio.NewWriter(file)
	cr := make([]*RevisionReader, len(revisions))
	heap := []*RevisionEntry{}
	for i, revision := range revisions {
		cr[i] = NewRevisionReader(repository, revision, BlockBuf{})
		re, err := cr[i].Read()
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
		// Find the smallest fullPath.
		fullPath := Path("")
		for _, re := range heap {
			if re != nil && (fullPath == "" || re.Path < fullPath) {
				fullPath = re.Path
			}
		}
		// Find the newest entry and read the next entries for all revisions
		// that match the fullPath
		var newest *RevisionEntry
		for i, re := range heap {
			if re != nil && re.Path == fullPath {
				if newest == nil {
					newest = re
				}
				re, err := cr[i].Read()
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
			if err := MarshalRevisionEntry(newest, w); err != nil {
				return WrapErrorf(err, "failed to write to target file")
			}
		}
	}
	if err := w.Flush(); err != nil {
		return WrapErrorf(err, "failed to flush target file")
	}
	if err := file.Close(); err != nil {
		if err2 := os.Remove(targetFile); err2 != nil {
			return WrapErrorf(err, "failed to close file (and it could not be deleted, it is garbage now)")
		}
		return WrapErrorf(err, "failed to close target file")
	}
	return nil
}
