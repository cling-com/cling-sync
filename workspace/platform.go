//go:build !darwin && !linux

// Platform dependent code.

package workspace

import (
	"os"

	"github.com/flunderpero/cling-sync/lib"
)

func EnhanceMetadata(md *lib.FileMetadata, fileInfo os.FileInfo) {
}
