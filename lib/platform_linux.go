//go:build linux

package lib

import (
	"io/fs"
	"syscall"
)

type EnhancedStat_t struct {
	CTimeSec  int64
	CTimeNSec int32
	Inode     uint64
}

func EnhanceMetadata(md *FileMetadata, fileInfo fs.FileInfo) {
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		md.GID = stat.Gid
		md.UID = stat.Uid
	}
}

func EnhancedStat(fileInfo fs.FileInfo) (*EnhancedStat_t, error) {
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, Errorf("not a linux file")
	}
	return &EnhancedStat_t{
		CTimeSec:  stat.Ctim.Sec,
		CTimeNSec: int32(stat.Ctim.Nsec),
		Inode:     stat.Ino,
	}, nil
}
