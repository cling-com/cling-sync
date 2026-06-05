//nolint:godot // It is strange but godot complains about the comment to `RevisionLog.Short`.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

type RevisionLog struct {
	RevisionId lib.RevisionId
	Revision   lib.Revision
	Files      []StatusFile
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
	PathFilter lib.PathFilter
	Status     bool
	// Range is not validated against the repository:
	// a Range.Until not in the repository fails when its revision is read,
	// and a Range.Since not in the repository is never reached, so the log
	// runs to the root.
	Range lib.RevisionRange
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
		if opts.Status || opts.PathFilter != nil {
			revisionReader := lib.NewRevisionReader(repository, &revision)
			for {
				entry, err := revisionReader.Read(ctx, buf)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return nil, lib.WrapErrorf(err, "failed to read revision %s", revisionId)
				}
				if opts.PathFilter != nil && !opts.PathFilter.Include(entry.Path, entry.Metadata.FileMode.IsDir()) {
					continue
				}
				matchedAtLeastOnePath = true
				if opts.Status {
					files = append(files, StatusFile{entry.Path, entry.Kind, entry.Metadata})
				}
			}
		}
		if !opts.Status {
			files = nil
		}
		if opts.PathFilter == nil || matchedAtLeastOnePath {
			logs = append(logs, RevisionLog{revisionId, revision, files})
		}
		revisionId = revision.ParentRevisionId
	}
	return logs, nil
}
