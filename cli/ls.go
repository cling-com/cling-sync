package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

type LsFile struct {
	Path     string
	Metadata *lib.FileMetadata
}

func (f *LsFile) String() string {
	mtime := time.Unix(f.Metadata.MTimeSec, int64(f.Metadata.MTimeNSec)).Format(time.RFC3339)
	return fmt.Sprintf(
		"%s %12d %s %s",
		f.Metadata.ModeAndPerm.String(),
		f.Metadata.Size,
		mtime,
		f.Path,
	)
}

func Ls(repository *lib.Repository, revisionId lib.RevisionId, pattern *lib.PathPattern) ([]LsFile, error) {
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("ls-%d", os.Getpid()))
	if err := os.MkdirAll(tmpDir, 0o700); err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary directory %s", tmpDir)
	}
	snapshot, err := lib.NewRevisionSnapshot(repository, revisionId, tmpDir)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	defer snapshot.Close() //nolint:errcheck
	reader, err := snapshot.Reader()
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open revision snapshot reader")
	}
	files := []LsFile{}
	for {
		re, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		path := re.Path.FSString()
		if pattern != nil && !pattern.Match(path) {
			continue
		}
		files = append(files, LsFile{path, re.Metadata})
	}
	return files, nil
}
