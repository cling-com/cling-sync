//go:build darwin

package lib

import (
	"io/fs"
	"syscall"
)

func EnhanceMetadata(md *FileMetadata, fileInfo fs.FileInfo) {
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		md.GID = stat.Gid
		md.UID = stat.Uid
		md.BirthtimeSec = stat.Birthtimespec.Sec
		md.BirthtimeNSec = int32(stat.Birthtimespec.Nsec) //nolint:gosec
	}
}
