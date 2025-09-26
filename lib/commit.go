package lib

import (
	"errors"
	"io"
	"time"
)

var ErrEmptyCommit = Errorf("empty commit")

type Commit struct {
	BaseRevision RevisionId
	repository   *Repository
	tempWriter   *TempWriter[RevisionEntry]
	tmpFS        FS
	ensureDirs   []RevisionEntry
}

func NewCommit(repository *Repository, tmpFS FS) (*Commit, error) {
	head, err := repository.Head()
	if err != nil {
		return nil, WrapErrorf(err, "failed to read head revision")
	}
	tempWriter := NewRevisionEntryTempWriter(tmpFS, DefaultTempChunkSize)
	return &Commit{head, repository, tempWriter, tmpFS, nil}, nil
}

func (c *Commit) Add(entry *RevisionEntry) error {
	if c.tempWriter == nil {
		return Errorf("commit is closed")
	}
	return c.tempWriter.Add(entry)
}

// Make sure that the directory `path` exists in the current head of the repository.
// If some parent directory does not exist, it will be created with the given
// `NewEmptyDirFileMetadata` metadata.
func (c *Commit) EnsureDirExists(
	path Path,
	snapshotCache *TempCache[RevisionEntry],
	snapshotRevisionId RevisionId,
) error {
	if path.IsEmpty() {
		return nil
	}
	if c.BaseRevision != snapshotRevisionId {
		return Errorf(
			"the commit's base revision %s does not match the snapshot revision %s",
			c.BaseRevision,
			snapshotRevisionId,
		)
	}
	md := NewEmptyDirFileMetadata(time.Now())
	p := path
	for !p.IsEmpty() {
		// Check whether we already have the directory.
		for _, entry := range c.ensureDirs {
			if entry.Path == p {
				return nil
			}
		}
		// Check whether it is a file.
		existing, found, err := snapshotCache.Get(PathCompareString(p, false))
		if err != nil {
			return WrapErrorf(err, "failed to get path %s from remote revision", p)
		}
		if found && !existing.Metadata.ModeAndPerm.IsDir() {
			return Errorf(
				"cannot ensure directory %s exists, because %s already exists and is not a directory",
				path,
				p,
			)
		}
		// Check whether the directory already exists.
		_, found, err = snapshotCache.Get(PathCompareString(p, true))
		if err != nil {
			return WrapErrorf(err, "failed to get path %s from remote revision", p)
		}
		if found {
			break
		}
		entry, err := NewRevisionEntry(p, RevisionEntryAdd, &md)
		if err != nil {
			return WrapErrorf(err, "failed to create revision entry for path %s", p)
		}
		c.ensureDirs = append(c.ensureDirs, entry)
		p = p.Dir()
	}
	return nil
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
	if c.ensureDirs != nil {
		sorted, err = c.appendEnsureDirs(sorted)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to append ensured dirs")
		}
	}
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

func (c *Commit) appendEnsureDirs(sorted *Temp[RevisionEntry]) (*Temp[RevisionEntry], error) {
	// We have to rewrite the whole commit because we have to check whether
	// the directories we want to add already exist.
	tmpFS, err := c.tmpFS.MkSub("ensuredirs")
	if err != nil {
		return nil, WrapErrorf(err, "failed to create temporary directory")
	}
	tempWriter := NewRevisionEntryTempWriter(tmpFS, DefaultTempChunkSize)
	cache, err := NewRevisionEntryTempCache(sorted, 10)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create revision temp cache")
	}
	for _, entry := range c.ensureDirs {
		_, found, err := cache.Get(PathCompareString(entry.Path, true))
		if err != nil {
			return nil, WrapErrorf(err, "failed to get path %s from remote revision", entry.Path)
		}
		if found {
			continue
		}
		if err := tempWriter.Add(&entry); err != nil {
			return nil, WrapErrorf(err, "failed to add path %s to commit", entry.Path)
		}
	}
	r := sorted.Reader(nil)
	for {
		entry, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, WrapErrorf(err, "failed to read revision snapshot")
		}
		if err := tempWriter.Add(entry); err != nil {
			return nil, WrapErrorf(err, "failed to add path %s to commit", entry.Path)
		}
	}
	sorted, err = tempWriter.Finalize()
	if err != nil {
		return nil, WrapErrorf(err, "failed to finalize temp writer")
	}
	return sorted, nil
}
