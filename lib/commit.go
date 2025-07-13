package lib

import (
	"strings"
	"time"
)

var ErrEmptyCommit = Errorf("empty commit")

type Commit struct {
	BaseRevision RevisionId
	repository   *Repository
	tempWriter   *RevisionTempWriter
	tmpFS        FS
}

func NewCommit(repository *Repository, tmpFS FS) (*Commit, error) {
	head, err := repository.Head()
	if err != nil {
		return nil, WrapErrorf(err, "failed to read head revision")
	}
	tempWriter := NewRevisionTempWriter(tmpFS, DefaultRevisionTempChunkSize)
	return &Commit{head, repository, tempWriter, tmpFS}, nil
}

func (c *Commit) Add(entry *RevisionEntry) error {
	if c.tempWriter == nil {
		return Errorf("commit is closed")
	}
	if strings.HasPrefix(entry.Path.FSString(), "/") {
		return Errorf("commit cannot add absolute paths to commit: %s", entry.Path)
	}
	return c.tempWriter.Add(entry)
}

type CommitInfo struct {
	Author  string
	Message string
}

// Return `ErrHeadChanged` if the head has changed during the commit.
// Return `ErrEmptyCommit` if the commit is empty.
func (c *Commit) Commit(info *CommitInfo) (RevisionId, error) {
	sorted, err := c.tempWriter.Finalize()
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to finalize temp writer")
	}
	defer sorted.Remove() //nolint:errcheck
	if sorted.Chunks() == 0 {
		return RevisionId{}, ErrEmptyCommit
	}
	blockIds := []BlockId{}
	sortedReader := sorted.Reader(nil)
	for chunk := range sorted.Chunks() {
		buf, err := sortedReader.ReadChunkRaw(chunk)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to read sorted chunk file")
		}
		_, blockHeader, err := c.repository.WriteBlock(buf)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to write block")
		}
		blockIds = append(blockIds, blockHeader.BlockId)
	}
	now := time.Now()
	revision := &Revision{
		TimestampSec:  now.Unix(),
		TimestampNSec: int32(now.Nanosecond()), //nolint:gosec
		Message:       info.Message,
		Author:        info.Author,
		Parent:        c.BaseRevision,
		Blocks:        blockIds,
	}
	revisionId, err := c.repository.WriteRevision(revision)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write revision")
	}
	return revisionId, nil
}
