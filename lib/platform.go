//go:build !darwin && !linux

// Platform dependent code.

package lib

import (
	"io/fs"
)

type EnhancedStat_t struct {
	CTimeSec  int64
	CTimeNSec int32
	Inode     uint64
}

func EnhanceMetadata(md *FileMetadata, fileInfo fs.FileInfo) {
}

func EnhancedStat(fileInfo fs.FileInfo) (*EnhancedStat_t, error) {
	return nil, Errorf("not implemented")
}
