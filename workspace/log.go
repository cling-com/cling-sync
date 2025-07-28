//nolint:godot // It is strange but godot complains about the comment to `RevisionLog.Short`.
package workspace

import (
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
	date := time.Unix(r.TimestampSec, int64(r.TimestampNSec)).Format(time.RFC1123)
	return fmt.Sprintf(
		"Revision: %s\nAuthor:   %s\nDate:     %s\n\n    %s",
		l.RevisionId,
		strings.ReplaceAll(r.Author, "\n", " "),
		date,
		strings.ReplaceAll(r.Message, "\n", "\n    "),
	)
}

// Return the log in short format.
//
// <RevisionId> <Date> <Message>
func (l *RevisionLog) Short() string {
	r := l.Revision
	date := time.Unix(r.TimestampSec, int64(r.TimestampNSec)).Format(time.RFC3339)
	return fmt.Sprintf("%s %s %s", l.RevisionId, date, strings.ReplaceAll(r.Message, "\n", " "))
}

type LogOptions struct {
	PathFilter lib.PathFilter
	Status     bool
}

func Log(repository *lib.Repository, opts *LogOptions) ([]RevisionLog, error) {
	head, err := repository.Head()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get head revision")
	}
	logs := []RevisionLog{}
	revisionId := head
	for !revisionId.IsRoot() {
		revision, err := repository.ReadRevision(revisionId)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read revision %s", revisionId)
		}
		files := []StatusFile{}
		matchedAtLeastOnePath := false
		if opts.Status || opts.PathFilter != nil {
			revisionReader := lib.NewRevisionReader(repository, &revision)
			for {
				entry, err := revisionReader.Read()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return nil, lib.WrapErrorf(err, "failed to read revision %s", revisionId)
				}
				if opts.PathFilter != nil && !opts.PathFilter.Include(entry.Path) {
					continue
				}
				matchedAtLeastOnePath = true
				if opts.Status {
					files = append(files, StatusFile{entry.Path, entry.Type, entry.Metadata})
				}
			}
		}
		if !opts.Status {
			files = nil
		}
		if opts.PathFilter == nil || matchedAtLeastOnePath {
			logs = append(logs, RevisionLog{revisionId, revision, files})
		}
		revisionId = revision.Parent
	}
	return logs, nil
}
