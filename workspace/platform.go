//go:build !darwin && !linux

// Platform dependent code.

package workspace

import (
	"io/fs"

	"github.com/flunderpero/cling-sync/lib"
)

func EnhanceMetadata(md *lib.FileMetadata, fileInfo fs.FileInfo) {
}
