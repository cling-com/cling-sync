//go:build darwin

package workspace

import (
	"os"
	"syscall"

	"github.com/flunderpero/cling-sync/lib"
)

func EnhanceMetadata(md *lib.FileMetadata, fileInfo os.FileInfo) {
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); ok {
		md.GID = stat.Gid
		md.UID = stat.Uid
		md.BirthtimeSec = stat.Birthtimespec.Sec
		md.BirthtimeNSec = int32(stat.Birthtimespec.Nsec) //nolint:gosec
	}
}
