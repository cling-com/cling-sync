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
		size = FormatBytes(f.Metadata.Size)
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

type LsOptions struct {
	RevisionId lib.RevisionId
	PathFilter lib.PathFilter
}

func Ls(repository *lib.Repository, opts *LsOptions) ([]LsFile, error) {
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("ls-%d", os.Getpid()))
	if err := os.MkdirAll(tmpDir, 0o700); err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary directory %s", tmpDir)
	}
	snapshot, err := lib.NewRevisionSnapshot(repository, opts.RevisionId, tmpDir)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(opts.PathFilter)
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
		files = append(files, LsFile{path, re.Metadata})
	}
	return files, nil
}

func FormatBytes(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%c", float64(b)/float64(div), "KMGTPE"[exp])
}
