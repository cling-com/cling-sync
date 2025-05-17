package workspace

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

type LsFormat struct {
	FullPath bool
	FullMode bool
	// A `time.Format` string or the special value "relative".
	TimestampFormat   string
	HumanReadableSize bool
}

func (f *LsFile) Format(format *LsFormat) string {
	mtime := time.Unix(f.Metadata.MTimeSec, int64(f.Metadata.MTimeNSec))
	var mtimeStr string
	if format.TimestampFormat == "relative" {
		if time.Since(mtime) < time.Hour*24*365 {
			mtimeStr = mtime.Format("Jan _2 15:04")
		} else {
			mtimeStr = mtime.Format("Jan _2  2006")
		}
	} else {
		mtimeStr = mtime.Format(format.TimestampFormat)
	}
	var size string
	if format.HumanReadableSize {
		suffixes := []string{"", "K", "M", "G", "T", "P"}
		var i int
		s := float32(f.Metadata.Size)
		for ; i < len(suffixes)-1; i++ {
			if s < 1000 {
				break
			}
			s /= 1000
		}
		if f.Metadata.Size < 1000 { //nolint:gocritic
			size = fmt.Sprintf("%d%s", f.Metadata.Size, suffixes[i])
		} else if f.Metadata.Size < 1000000 {
			size = fmt.Sprintf("%.0f%s", s, suffixes[i])
		} else {
			size = fmt.Sprintf("%.1f%s", s, suffixes[i])
		}
	} else {
		size = fmt.Sprintf("%d", f.Metadata.Size)
	}
	var path string
	if format.FullPath {
		path = f.Path
	} else {
		path = filepath.Base(f.Path)
	}
	if f.Metadata.ModeAndPerm.IsDir() {
		path += "/"
	}
	var mode string
	if format.FullMode {
		mode = f.Metadata.ModeAndPerm.String()
	} else {
		mode = f.Metadata.ModeAndPerm.ShortString()
	}
	if format.HumanReadableSize {
		return fmt.Sprintf("%s %6s %s %s", mode, size, mtimeStr, path)
	} else {
		return fmt.Sprintf("%s %12s %s %s", mode, size, mtimeStr, path)
	}
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
	reader, err := snapshot.Reader(nil)
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
