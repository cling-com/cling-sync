//go:build darwin

package workspace

import (
	"io/fs"
	"syscall"

	"github.com/flunderpero/cling-sync/lib"
)

func EnhanceMetadata(md *lib.FileMetadata, fileInfo fs.FileInfo) {
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		md.GID = stat.Gid
		md.UID = stat.Uid
		md.BirthtimeSec = stat.Birthtimespec.Sec
		md.BirthtimeNSec = int32(stat.Birthtimespec.Nsec) //nolint:gosec
	}
}
