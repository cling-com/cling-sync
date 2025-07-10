//go:build !darwin && !linux

// Platform dependent code.

package lib

import (
	"io/fs"
)

func EnhanceMetadata(md *FileMetadata, fileInfo fs.FileInfo) {
}
