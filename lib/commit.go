package lib

import (
	"os"
	"time"
)

const defaultChunkSize = 4 * 1024 * 1024

type Commit struct {
	BaseRevision RevisionId
	repository   *Repository
	tempWriter   *RevisionTempWriter
	tmpDir       string
}

func NewCommit(repository *Repository, tmpDir string) (*Commit, error) {
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read temporary directory %s", tmpDir)
	}
	head, err := repository.Head()
	if err != nil {
		return nil, WrapErrorf(err, "failed to read head revision")
	}
	if len(files) > 0 {
		return nil, Errorf("temporary directory %s is not empty", tmpDir)
	}
	tempWriter := NewRevisionTempWriter(tmpDir, defaultChunkSize)
	return &Commit{head, repository, tempWriter, tmpDir}, nil
}

func (c *Commit) Add(entry *RevisionEntry) error {
	if c.tempWriter == nil {
		return Errorf("commit is closed")
	}
	return c.tempWriter.Add(entry)
}

type CommitInfo struct {
	Author  string
	Message string
}

func (c *Commit) Commit(info *CommitInfo) (RevisionId, error) {
	sorted, err := c.tempWriter.Finalize()
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to finalize temp writer")
	}
	defer sorted.Remove() //nolint:errcheck
	if sorted.Chunks() == 0 {
		return RevisionId{}, Errorf("empty commit")
	}
	blockBuf := BlockBuf{}
	blockIds := []BlockId{}
	sortedReader := sorted.Reader()
	for chunk := range sorted.Chunks() {
		buf, err := sortedReader.ReadChunkRaw(chunk)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to read sorted chunk file")
		}
		_, blockHeader, err := c.repository.WriteBlock(buf, blockBuf)
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
	revisionId, err := c.repository.WriteRevision(revision, blockBuf)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write revision")
	}
	return revisionId, nil
}
