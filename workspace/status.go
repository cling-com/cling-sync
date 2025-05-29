package workspace

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
	return fmt.Sprintf("%s %s", typeStr, f.Path)
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

func Status(ws *Workspace, repository *lib.Repository, opts *StatusOptions, tmpDir string) (StatusFiles, error) {
	head, err := repository.Head()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get head")
	}
	snapshotDir := filepath.Join(tmpDir, "snapshot")
	stagingDir := filepath.Join(tmpDir, "staging")
	for _, dir := range []string{snapshotDir, stagingDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, lib.WrapErrorf(err, "failed to create temporary directory %s", dir)
		}
	}
	snapshot, err := lib.NewRevisionSnapshot(repository, head, snapshotDir)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	staging, err := NewStaging(ws.WorkspacePath, repository, snapshot, opts.PathFilter, false, stagingDir, opts.Monitor)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to scan changes")
	}
	revisionTemp, err := staging.MergeWithSnapshot(repository, snapshot)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to merge staging chunks")
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
