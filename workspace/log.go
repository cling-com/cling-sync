//nolint:godot // It is strange but godot complains about the comment to `RevisionLog.Short`.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cling-com/cling-sync/lib"
)

type RevisionLog struct {
	RevisionId lib.RevisionId
	Revision   lib.Revision
	Files      []StatusFile
	// TotalFiles is how many paths the revision holds, including the ones
	// skipped by PathPrefix, Include, and Exclude.
	TotalFiles int
}

// Return the log in long format (a bit like `git log`).
//
// Revision: 54601297f7a5003df8a4be36f4298c03dd2f90d1
// Author:   pero
// Date:     Tue, 13 May 2025 12:16:16 CEST
//
//	Commit message
func (l *RevisionLog) Long() string {
	r := l.Revision
	date := r.Timestamp.Time().Format(time.RFC1123)
	return fmt.Sprintf(
		"Revision: %s\nAuthor:   %s\nDate:     %s\n\n    %s",
		l.RevisionId,
		strings.ReplaceAll(derefString(r.Author), "\n", " "),
		date,
		strings.ReplaceAll(derefString(r.Message), "\n", "\n    "),
	)
}

// Return the log in short format.
//
// <RevisionId> <Date> <Message>
func (l *RevisionLog) Short() string {
	r := l.Revision
	date := r.Timestamp.Time().Format(time.RFC3339)
	return fmt.Sprintf("%s %s %s", l.RevisionId, date, strings.ReplaceAll(derefString(r.Message), "\n", " "))
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

type LogOptions struct {
	// Include and Exclude match paths relative to PathPrefix. Either one drops
	// a revision that has no path left after filtering.
	Include *lib.PathInclusionFilter
	Exclude *lib.PathExclusionFilter
	Status  bool
	// Range is not validated against the repository:
	// a Range.Until not in the repository fails when its revision is read,
	// and a Range.Since not in the repository is never reached, so the log
	// runs to the root.
	Range lib.RevisionRange
	// PathPrefix scopes the reported paths to a subtree. It deliberately does
	// not hide revisions that touched nothing inside it, because history is
	// global: revision ids, `~<n>`, and ranges all address the whole chain.
	PathPrefix lib.Path
}

func Log(ctx context.Context, repository *lib.Repository, opts *LogOptions) ([]RevisionLog, error) {
	var revisionId lib.RevisionId
	if opts.Range.Until != nil {
		revisionId = *opts.Range.Until
	} else {
		head, err := repository.Head(ctx)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to get head revision")
		}
		revisionId = head
	}
	logs := []RevisionLog{}
	buf := lib.NewBlockBuf()
	// A path filter lists a revision only if one of its entries survives it, so
	// the entries have to be read even when no status was requested. This asks
	// whether a filter exists, not whether it matches, so it tests for nil.
	filtered := opts.Include != nil || opts.Exclude != nil
	for !revisionId.IsRoot() {
		if opts.Range.Since != nil && revisionId == *opts.Range.Since {
			break
		}
		revision, err := repository.ReadRevision(ctx, revisionId, buf)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read revision %s", revisionId)
		}
		files := []StatusFile{}
		matchedAtLeastOnePath := false
		totalFiles := 0
		if opts.Status || filtered {
			revisionReader := lib.NewRevisionReader(repository, &revision)
			for {
				entry, err := revisionReader.Read(ctx, buf)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return nil, lib.WrapErrorf(err, "failed to read revision %s", revisionId)
				}
				totalFiles += 1
				// Trim the prefix first so the filter matches against the
				// prefix-relative path the user sees, not the full repository path.
				path, ok := entry.Path.TrimBase(opts.PathPrefix)
				if !ok {
					continue
				}
				isDir := entry.Metadata.FileMode.IsDir()
				if !opts.Include.Include(path, isDir) || !opts.Exclude.Include(path, isDir) {
					continue
				}
				matchedAtLeastOnePath = true
				if opts.Status {
					files = append(files, StatusFile{path, entry.Kind, entry.Metadata})
				}
			}
		}
		if !opts.Status {
			files = nil
		}
		if !filtered || matchedAtLeastOnePath {
			logs = append(logs, RevisionLog{revisionId, revision, files, totalFiles})
		}
		revisionId = revision.ParentRevisionId
	}
	return logs, nil
}
