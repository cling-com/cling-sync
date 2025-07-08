package workspace

import (
	"errors"
	"fmt"
	"io"

	"github.com/flunderpero/cling-sync/lib"
)

type StatusFile struct {
	Path     string
	Type     lib.RevisionEntryType
	Metadata *lib.FileMetadata
}

func (f StatusFile) Format() string {
	var typeStr string
	switch f.Type {
	case lib.RevisionEntryAdd:
		typeStr = "A"
	case lib.RevisionEntryUpdate:
		typeStr = "M"
	case lib.RevisionEntryDelete:
		typeStr = "D"
	default:
		panic(fmt.Sprintf("invalid revision entry type %d", f.Type))
	}
	path := f.Path
	if f.Metadata.ModeAndPerm.IsDir() {
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
		switch file.Type {
		case lib.RevisionEntryAdd:
			added++
		case lib.RevisionEntryUpdate:
			updated++
		case lib.RevisionEntryDelete:
			deleted++
		default:
			panic(fmt.Sprintf("invalid revision entry type %d", file.Type))
		}
	}
	return fmt.Sprintf("%d added, %d updated, %d deleted", added, updated, deleted)
}

type StatusOptions struct {
	PathFilter lib.PathFilter
	Monitor    StagingEntryMonitor
}

func Status(ws *Workspace, repository *lib.Repository, opts *StatusOptions, tmpFS lib.FS) (StatusFiles, error) {
	head, err := repository.Head()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get head")
	}
	snapshotFS, err := tmpFS.MkSub("snapshot")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary snapshot directory")
	}
	stagingTmpFS, err := tmpFS.MkSub("staging")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary staging directory")
	}
	snapshot, err := lib.NewRevisionSnapshot(repository, head, snapshotFS)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	staging, err := NewStaging(ws.FS, opts.PathFilter, stagingTmpFS, opts.Monitor)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to scan changes")
	}
	revisionTemp, err := staging.MergeWithSnapshot(snapshot)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to merge staging and revision snapshot")
	}
	if revisionTemp.Chunks() == 0 {
		return []StatusFile{}, nil
	}
	revisionTempReader := revisionTemp.Reader(nil)
	result := []StatusFile{}
	for {
		entry, err := revisionTempReader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read revision chunk file")
		}
		result = append(result, StatusFile{entry.Path.FSString(), entry.Type, entry.Metadata})
	}
	return result, nil
}
