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

func EnhanceMetadata(md *FileMetadata, fileInfo fs.FileInfo) {
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		md.GID = stat.Gid
		md.UID = stat.Uid
		md.BirthtimeSec = stat.Birthtimespec.Sec
		md.BirthtimeNSec = int32(stat.Birthtimespec.Nsec) //nolint:gosec
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
