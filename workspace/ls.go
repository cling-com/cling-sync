package workspace

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

type LsFile struct {
	Path     lib.Path
	Metadata *lib.PathMetadata
}

func (f *LsFile) String() string {
	mtime := f.Metadata.MTime().Format(time.RFC3339)
	return fmt.Sprintf(
		"%s %12d %s %s",
		f.Metadata.FileMode.String(),
		f.Metadata.Size,
		mtime,
		f.Path,
	)
}

type LsFormat struct {
	FullPath bool
	FullMode bool
	FileHash bool
	// A `time.Format` string or one of the special values "relative", "unix", or "unix-fraction".
	TimestampFormat   string
	HumanReadableSize bool
}

func (f *LsFile) Format(format *LsFormat) string {
	mtime := f.Metadata.MTime()
	var mtimeStr string
	switch format.TimestampFormat {
	case "relative":
		if time.Since(mtime) < time.Hour*24*365 {
			mtimeStr = mtime.Format("Jan _2 15:04")
		} else {
			mtimeStr = mtime.Format("Jan _2  2006")
		}
	case "unix-fraction":
		mtimeStr = fmt.Sprintf("%d.%09d0", mtime.Unix(), mtime.Nanosecond())
	case "unix":
		mtimeStr = fmt.Sprintf("%d", mtime.Unix())
	default:
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
		path = f.Path.String()
	} else {
		path = f.Path.Base().String()
	}
	if f.Metadata.FileMode.IsDir() {
		path += "/"
	}
	var mode string
	if format.FullMode {
		mode = f.Metadata.FileMode.String()
	} else {
		mode = f.Metadata.FileMode.ShortString()
	}
	s := " " + path
	if format.FileHash {
		s = fmt.Sprintf("%s %s", s, hex.EncodeToString(f.Metadata.FileHash[:]))
	}
	if format.HumanReadableSize {
		return fmt.Sprintf("%s %6s %s %s", mode, size, mtimeStr, s)
	} else {
		return fmt.Sprintf("%s %12s %s %s", mode, size, mtimeStr, s)
	}
}

type LsOptions struct {
	RevisionId lib.RevisionId
	PathFilter lib.PathFilter
	PathPrefix lib.Path
}

func Ls(repository *lib.Repository, tmpFS lib.FS, opts *LsOptions) ([]LsFile, error) {
	snapshot, err := lib.NewRevisionSnapshot(repository, opts.RevisionId, tmpFS)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create revision snapshot")
	}
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(lib.RevisionEntryPathFilter(opts.PathFilter))
	files := []LsFile{}
	buf := lib.BlockBuf{}
	for {
		re, err := reader.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, lib.WrapErrorf(err, "failed to read revision snapshot")
		}
		path, ok := re.Path.TrimBase(opts.PathPrefix)
		if !ok {
			continue
		}
		re.Path = path
		files = append(files, LsFile{re.Path, re.Metadata})
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
