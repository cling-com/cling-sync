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

func EnhanceMetadata(md *PathMetadata, fileInfo fs.FileInfo) {
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		uid, gid := stat.Uid, stat.Gid
		md.Uid = &uid
		md.Gid = &gid
	}
}

func EnhancedStat(fileInfo fs.FileInfo) (*EnhancedStat_t, error) {
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, Errorf("not a linux file")
	}
	return &EnhancedStat_t{
		CTimeSec:  stat.Ctim.Sec,
		CTimeNSec: int32(stat.Ctim.Nsec), //nolint:gosec
		Inode:     stat.Ino,
	}, nil
}
