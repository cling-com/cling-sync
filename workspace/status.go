package workspace

import (
	"bufio"
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

func Status(src string, repository *lib.Repository, opts *StatusOptions, tmpDir string) (StatusFiles, error) {
	staging, err := NewStaging(src, repository, opts.PathFilter, false, tmpDir, opts.Monitor)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to scan changes")
	}
	cw, err := staging.MergeWithSnapshot(repository)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to merge staging chunks")
	}
	if cw.Chunks() == 0 {
		return []StatusFile{}, nil
	}
	result := []StatusFile{}
	for i := range cw.Chunks() {
		f, err := cw.ChunkReader(i)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to open revision chunk file")
		}
		defer f.Close() //nolint:errcheck
		reader := bufio.NewReader(f)
		for {
			re, err := lib.UnmarshalRevisionEntry(reader)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, lib.WrapErrorf(err, "failed to read revision chunk file")
			}
			result = append(result, StatusFile{re.Path.FSString(), re.Type, re.Metadata})
		}
	}
	return result, nil
}
