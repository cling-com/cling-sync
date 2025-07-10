//go:build linux

package lib

import (
	"io/fs"
	"syscall"
)

func EnhanceMetadata(md *FileMetadata, fileInfo fs.FileInfo) {
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		md.GID = stat.Gid
		md.UID = stat.Uid
	}
}
