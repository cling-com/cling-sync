package lib

import (
	"io"
	"os"
	"time"
)

const defaultChunkSize = 4 * 1024 * 1024

type Commit struct {
	BaseRevision RevisionId
	repository   *Repository
	chunkWriter  *RevisionEntryChunks
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
	chunkWriter := NewRevisionEntryChunks(tmpDir, "add", defaultChunkSize)
	return &Commit{head, repository, chunkWriter, tmpDir}, nil
}

func (c *Commit) Add(entry *RevisionEntry) error {
	if c.chunkWriter == nil {
		return Errorf("staging is closed")
	}
	return c.chunkWriter.Add(entry)
}

type CommitInfo struct {
	Author  string
	Message string
}

func (c *Commit) Commit(info *CommitInfo) (RevisionId, error) {
	sortedChunkWriter := NewRevisionEntryChunks(c.tmpDir, "sorted", MaxBlockDataSize)
	if err := c.chunkWriter.MergeChunks(sortedChunkWriter.Add); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to merge chunks")
	}
	if err := sortedChunkWriter.Close(); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to close sorted chunk writer")
	}
	if sortedChunkWriter.Chunks() == 0 {
		return RevisionId{}, Errorf("empty commit")
	}
	blockBuf := BlockBuf{}
	blockIds := []BlockId{}
	for chunk := range sortedChunkWriter.Chunks() {
		f, err := sortedChunkWriter.ChunkReader(chunk)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to open sorted chunk file")
		}
		defer f.Close() //nolint:errcheck
		buf, err := io.ReadAll(f)
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
