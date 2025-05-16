//nolint:godot // It is strange but godot complains about the comment to `RevisionLog.Short`.
package workspace

import (
	"fmt"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

type RevisionLog struct {
	RevisionId lib.RevisionId
	Revision   lib.Revision
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
		l.RevisionId.Long(),
		strings.ReplaceAll(r.Author, "\n", " "),
		date,
		strings.ReplaceAll(r.Message, "\n", "\n    "),
	)
}

// Return the log in short format.
//
// <8-char-RevisionId> <Date> <Message>
func (l *RevisionLog) Short() string {
	r := l.Revision
	date := time.Unix(r.TimestampSec, int64(r.TimestampNSec)).Format(time.RFC3339)
	return fmt.Sprintf("%s %s %s", l.RevisionId.Short(), date, strings.ReplaceAll(r.Message, "\n", " "))
}

func Log(repository *lib.Repository) ([]RevisionLog, error) {
	head, err := repository.Head()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get head revision")
	}
	logs := []RevisionLog{}
	revisionId := head
	blockBuf := lib.BlockBuf{}
	for !revisionId.IsRoot() {
		revision, err := repository.ReadRevision(revisionId, blockBuf)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read revision %s", revisionId)
		}
		logs = append(logs, RevisionLog{revisionId, revision})
		revisionId = revision.Parent
	}
	return logs, nil
}
