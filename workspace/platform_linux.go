//go:build linux

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
	}
}
