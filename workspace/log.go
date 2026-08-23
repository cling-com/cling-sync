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
	// How many paths the revision holds, including the ones the view hides
	// and the ones Include and Exclude drop.
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
	// Either filter drops a revision that has no path left after filtering.
	Include *lib.PathInclusionFilter
	Exclude *lib.PathExclusionFilter
	Status  bool
	// Range is not validated against the repository:
	// a Range.Until not in the repository fails when its revision is read,
	// and a Range.Since not in the repository is never reached, so the log
	// runs to the root.
	Range lib.RevisionRange
}

// A view deliberately does not hide revisions that touched nothing inside it,
// because history is global: revision ids, `~<n>`, and ranges all address the
// whole chain.
func Log( //nolint:funlen
	ctx context.Context,
	view *lib.RepositoryView,
	opts *LogOptions,
) ([]RevisionLog, error) {
	var revisionId lib.RevisionId
	if opts.Range.Until != nil {
		revisionId = *opts.Range.Until
	} else {
		head, err := view.Repository.Head(ctx)
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
		revision, err := view.Repository.ReadRevision(ctx, revisionId, buf)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read revision %s", revisionId)
		}
		files := []StatusFile{}
		matchedAtLeastOnePath := false
		hidden := 0
		totalFiles := 0
		if opts.Status || filtered {
			revisionReader := view.NewRevisionReader(&revision)
			displayReader := lib.NewDisplayOrderReader(func(buf lib.BlockBuf) (*lib.RevisionEntry, error) {
				return revisionReader.Read(ctx, buf)
			})
			for {
				entry, err := displayReader.Read(buf)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return nil, lib.WrapErrorf(err, "failed to read revision %s", revisionId)
				}
				totalFiles += 1
				isDir := entry.Metadata.FileMode.IsDir()
				if !opts.Include.Include(entry.Path, isDir) || !opts.Exclude.Include(entry.Path, isDir) {
					continue
				}
				matchedAtLeastOnePath = true
				if opts.Status {
					files = append(files, StatusFile{entry.Path, entry.Kind, entry.Metadata})
				}
			}
			hidden = revisionReader.Hidden()
		}
		if !opts.Status {
			files = nil
		}
		if !filtered || matchedAtLeastOnePath {
			logs = append(logs, RevisionLog{revisionId, revision, files, totalFiles + hidden})
		}
		revisionId = revision.ParentRevisionId
	}
	return logs, nil
}
