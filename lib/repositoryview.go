package lib

import (
	"context"
)

// A repository seen from one of its directories.
//
// Every path that crosses the view, entry paths and symlink targets alike, is
// relative to that directory. Entries outside of it, and symlinks whose target
// points outside of it, do not exist as far as callers are concerned ("hidden link").
// What is hidden is kept privately so that a commit through the view leaves the
// repository consistent.
// An empty prefix is a view of the whole repository.
type RepositoryView struct {
	Repository *Repository
	prefix     Path
}

func NewRepositoryView(repository *Repository, prefix Path) *RepositoryView {
	return &RepositoryView{Repository: repository, prefix: prefix}
}

// The view of the directory `path` inside this view.
func (v *RepositoryView) Sub(path Path) *RepositoryView {
	return &RepositoryView{Repository: v.Repository, prefix: v.prefix.Join(path)}
}

// Translate a repository entry into the view.
//
// Return nil if the entry is not visible, and whether that is because it is a
// symlink with a target outside the view.
func (v *RepositoryView) localize(e *RevisionEntry) (local *RevisionEntry, hiddenLink bool) {
	if v.prefix.IsEmpty() {
		return e, false
	}
	path, ok := e.Path.TrimBase(v.prefix)
	if !ok {
		return nil, false
	}
	md := e.Metadata
	if target, isSymlink := md.SymLink(); isSymlink {
		target, ok := target.TrimBase(v.prefix)
		if !ok {
			return nil, true
		}
		md.SymLinkTarget = &target
	}
	return &RevisionEntry{Kind: e.Kind, Path: path, Metadata: md}, false
}

// Translate a view entry into a repository entry.
func (v *RepositoryView) globalize(e *RevisionEntry) *RevisionEntry {
	md := e.Metadata
	if target, isSymlink := md.SymLink(); isSymlink {
		target = v.prefix.Join(target)
		md.SymLinkTarget = &target
	}
	return &RevisionEntry{Kind: e.Kind, Path: v.prefix.Join(e.Path), Metadata: md}
}

// A revision snapshot as seen from a view.
type ViewSnapshot struct {
	*Temp[*RevisionEntry]
	RevisionId RevisionId
	// The prefix and its parent directories, as they are in the repository.
	//
	// They are not visible through the view, but a commit needs them to tell
	// whether the prefix still has to be created.
	prefixChain []*RevisionEntry
	// Symlinks inside the view whose target points outside of it, as they are
	// in the repository ("hidden links").
	//
	// They are not visible through the view, but a commit needs them so that
	// whatever replaces one of them does not end up next to it in the
	// repository.
	hiddenLinks *RevisionEntryCache
	cache       *RevisionEntryCache
}

// Snapshot cache by path, built on first use.
func (s *ViewSnapshot) Cache() (*RevisionEntryCache, error) {
	if s.cache == nil {
		cache, err := NewRevisionEntryTempCache(s.Temp, 10)
		if err != nil {
			return nil, WrapErrorf(err, "failed to create revision temp cache")
		}
		s.cache = cache
	}
	return s.cache, nil
}

func (s *ViewSnapshot) Remove() error {
	if err := s.hiddenLinks.Source.Remove(); err != nil {
		return err
	}
	return s.Temp.Remove()
}

func (v *RepositoryView) NewSnapshot(
	ctx context.Context,
	revisionId RevisionId,
	tmpFS FS,
	mon RevisionSnapshotMonitor,
) (*ViewSnapshot, error) {
	entriesFS, err := tmpFS.MkSub("entries")
	if err != nil {
		return nil, WrapErrorf(err, "failed to create temporary directory")
	}
	hiddenLinksFS, err := tmpFS.MkSub("hidden-links")
	if err != nil {
		return nil, WrapErrorf(err, "failed to create temporary directory")
	}
	entries := NewRevisionEntryTempWriter(entriesFS, DefaultTempChunkSize)
	hiddenLinks := NewRevisionEntryTempWriter(hiddenLinksFS, DefaultTempChunkSize)
	var prefixChain []*RevisionEntry
	add := func(e *RevisionEntry) error {
		local, hiddenLink := v.localize(e)
		switch {
		case local != nil:
			return entries.Add(local)
		case hiddenLink:
			return hiddenLinks.Add(e)
		default:
			if v.prefix.IsRelativeTo(e.Path) || e.Path == v.prefix {
				prefixChain = append(prefixChain, e)
			}
		}
		return nil
	}
	if err := mergeRevisions(ctx, v.Repository, revisionId, add, mon); err != nil {
		return nil, err
	}
	// The merge emits every path once and in order, so we don't need to sort again.
	entriesTemp, err := entries.CloseWithoutSort()
	if err != nil {
		return nil, WrapErrorf(err, "failed to finalize temporary file")
	}
	hiddenLinksTemp, err := hiddenLinks.CloseWithoutSort()
	if err != nil {
		return nil, WrapErrorf(err, "failed to finalize temporary file")
	}
	hiddenLinksCache, err := NewRevisionEntryTempCache(hiddenLinksTemp, 1)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create revision temp cache")
	}
	return &ViewSnapshot{entriesTemp, revisionId, prefixChain, hiddenLinksCache, nil}, nil
}

// Reads the entries of one revision as seen from a view.
type ViewRevisionReader struct {
	view   *RepositoryView
	reader *RevisionReader
	hidden int
}

func (v *RepositoryView) NewRevisionReader(revision *Revision) *ViewRevisionReader {
	return &ViewRevisionReader{view: v, reader: NewRevisionReader(v.Repository, revision), hidden: 0}
}

// Return the next visible entry of the revision, or `io.EOF`.
func (r *ViewRevisionReader) Read(ctx context.Context, buf BlockBuf) (*RevisionEntry, error) {
	for {
		entry, err := r.reader.Read(ctx, buf)
		if err != nil {
			return nil, err
		}
		if local, _ := r.view.localize(entry); local != nil {
			return local, nil
		}
		r.hidden++
	}
}

// The number of entries read so far that the view does not show.
func (r *ViewRevisionReader) Hidden() int {
	return r.hidden
}

// A commit whose entries are given as seen from a view.
type ViewCommit struct {
	view   *RepositoryView
	commit *Commit
	base   *ViewSnapshot
}

// Start a commit on top of `base`, which must be a snapshot of the repository
// head.
//
// The commit makes sure the view's prefix exists in the repository once it is
// committed.
func (v *RepositoryView) NewCommit(ctx context.Context, tmpFS FS, base *ViewSnapshot) (*ViewCommit, error) {
	commit, err := NewCommit(ctx, v.Repository, tmpFS)
	if err != nil {
		return nil, err
	}
	if commit.BaseRevision != base.RevisionId {
		return nil, WrapErrorf(
			ErrHeadChanged,
			"the repository head %s does not match the snapshot revision %s",
			commit.BaseRevision,
			base.RevisionId,
		)
	}
	return &ViewCommit{view: v, commit: commit, base: base}, nil
}

// Add an entry to the commit.
//
// A directory added or updated where the view hides a symlink is recorded next
// to the symlink's deletion, because a file and a directory of one path are
// different entries to the repository. A file-like entry simply supersedes the
// symlink and is recorded as given. An update can meet a hidden symlink because
// local changes are computed against the workspace head, while the commit
// lands on the repository head.
func (c *ViewCommit) Add(entry *RevisionEntry) error {
	repoEntry := c.view.globalize(entry)
	if repoEntry.Kind == RevisionEntryKindDelete || !repoEntry.Metadata.FileMode.IsDir() {
		return c.commit.Add(repoEntry)
	}
	hidden, found, err := c.base.hiddenLinks.Get(PathKey{repoEntry.Path, false})
	if err != nil {
		return WrapErrorf(err, "failed to look up %s in hidden links", repoEntry.Path)
	}
	if found {
		deleted := *hidden
		deleted.Kind = RevisionEntryKindDelete
		if err := c.commit.Add(&deleted); err != nil {
			return err
		}
	}
	return c.commit.Add(repoEntry)
}

func (c *ViewCommit) Commit(ctx context.Context, info *CommitInfo) (RevisionId, error) {
	if err := c.commit.EnsureDirExists(c.view.prefix, c.base.prefixExists); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to ensure %s exists in the repository", c.view.prefix)
	}
	return c.commit.Commit(ctx, info)
}

// Whether the prefix chain holds the entry at the repository path `key`.
//
// Only the prefix and its parents are ever asked for, which is exactly what
// the chain holds.
func (s *ViewSnapshot) prefixExists(key PathKey) (bool, error) {
	for _, e := range s.prefixChain {
		if e.PathKey() == key {
			return true, nil
		}
	}
	return false, nil
}
