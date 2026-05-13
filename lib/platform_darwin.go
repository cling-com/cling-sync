//go:build darwin

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
		md.Birthtime = &Timestamp{Sec: stat.Birthtimespec.Sec, Nsec: uint32(stat.Birthtimespec.Nsec)} //nolint:gosec
	}
}

func EnhancedStat(fileInfo fs.FileInfo) (*EnhancedStat_t, error) {
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, Errorf("not a darwin file")
	}
	return &EnhancedStat_t{
		CTimeSec:  stat.Ctimespec.Sec,
		CTimeNSec: int32(stat.Ctimespec.Nsec), //nolint:gosec
		Inode:     stat.Ino,
	}, nil
}
