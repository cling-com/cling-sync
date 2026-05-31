package workspace

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/flunderpero/cling-sync/lib"
)

type StatusFile struct {
	Path     lib.Path
	Kind     lib.RevisionEntryKind
	Metadata lib.PathMetadata
}

func (f StatusFile) Format() string {
	var typeStr string
	switch f.Kind {
	case lib.RevisionEntryKindAdd:
		typeStr = "A"
	case lib.RevisionEntryKindUpdate:
		typeStr = "M"
	case lib.RevisionEntryKindDelete:
		typeStr = "D"
	default:
		panic(fmt.Sprintf("invalid revision entry type %d", f.Kind))
	}
	path := f.Path.String()
	if f.Metadata.FileMode.IsDir() {
		path += "/"
	}
	return fmt.Sprintf("%s %s", typeStr, path)
}

type StatusFiles []StatusFile

func (s StatusFiles) Summary() string {
	if len(s) == 0 {
		return "No changes"
	}
	added := 0
	updated := 0
	deleted := 0
	for _, file := range s {
		switch file.Kind {
		case lib.RevisionEntryKindAdd:
			added++
		case lib.RevisionEntryKindUpdate:
			updated++
		case lib.RevisionEntryKindDelete:
			deleted++
		default:
			panic(fmt.Sprintf("invalid revision entry type %d", file.Kind))
		}
	}
	return fmt.Sprintf("%d added, %d updated, %d deleted", added, updated, deleted)
}

type StatusOptions struct {
	PathFilter             lib.PathFilter
	Monitor                StagingEntryMonitor
	RestorableMetadataFlag lib.RestorableMetadataFlag
	UseStagingCache        bool
}

func Status(
	ctx context.Context,
	ws *Workspace,
	repository *lib.Repository,
	opts *StatusOptions,
	tmpFS lib.FS,
) (StatusFiles, error) {
	head, err := ws.Head(ctx)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get head")
	}
	// A root workspace head means the workspace was attached but never
	// merged. Compare against the repository head so `status` predicts
	// what `merge` would commit. `merge` fetches remote-only files rather
	// than deleting them, so suppress `Delete` entries in that mode.
	suppressDeletes := false
	if head.IsRoot() {
		head, err = repository.Head(ctx)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read repository head")
		}
		suppressDeletes = true
	}
	snapshotFS, err := tmpFS.MkSub("snapshot")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary snapshot directory")
	}
	stagingTmpFS, err := tmpFS.MkSub("staging")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary staging directory")
	}
	snapshot, err := lib.NewRevisionSnapshot(ctx, repository, head, snapshotFS)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	staging, err := NewStaging(ws.FS, ws.PathPrefix, opts.PathFilter, opts.UseStagingCache, stagingTmpFS, opts.Monitor)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to scan changes")
	}
	revisionTemp, err := staging.MergeWithSnapshot(snapshot, opts.RestorableMetadataFlag, suppressDeletes)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to merge staging and revision snapshot")
	}
	if revisionTemp.Chunks() == 0 {
		return []StatusFile{}, nil
	}
	revisionTempReader := revisionTemp.Reader(nil)
	result := []StatusFile{}
	buf := lib.NewBlockBuf()
	for {
		entry, err := revisionTempReader.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read revision chunk file")
		}
		path, ok := entry.Path.TrimBase(ws.PathPrefix)
		if !ok {
			continue
		}
		result = append(result, StatusFile{path, entry.Kind, entry.Metadata})
	}
	return result, nil
}
