package lib

import (
	"io/fs"
	"time"
)

// NewPathMetadataFromFileInfo returns a PathMetadata populated from the
// given FileInfo. Platform-specific fields (UID/GID/Birthtime) are filled
// in via EnhanceMetadata.
//
// Symlinks carry only the symlink bit, their own `Mtime`, and a
// `SymLinkTarget` set by the caller. Perm bits, owner, group, size, and
// birthtime are blanked: the FS layer refuses to chmod/chown a symlink, and
// the link's reported size (target string length) is just noise once the
// target is stored explicitly.
//
// Panics if `fileHash` is non-zero or `blockIds` is non-nil for a directory
// or symlink: that's a sign the caller mis-typed the entry.
func NewPathMetadataFromFileInfo(fileInfo fs.FileInfo, fileHash Sha256, blockIds []BlockId) PathMetadata {
	mode := NewFileMode(fileInfo.Mode())
	if mode.IsSymlink() {
		if fileHash != (Sha256{}) || blockIds != nil {
			panic("NewPathMetadataFromFileInfo: symlinks cannot carry FileHash or BlockIds")
		}
		return PathMetadata{ //nolint:exhaustruct
			FileMode: FileModeSymlink,
			Mtime:    NewTimestampFromTime(fileInfo.ModTime()),
		}
	}
	if fileInfo.IsDir() {
		if fileHash != (Sha256{}) || blockIds != nil {
			panic("NewPathMetadataFromFileInfo: directories cannot carry FileHash or BlockIds")
		}
	}
	var size int64
	if !fileInfo.IsDir() {
		size = fileInfo.Size()
	}
	md := PathMetadata{ //nolint:exhaustruct
		FileMode: mode,
		Mtime:    NewTimestampFromTime(fileInfo.ModTime()),
		Size:     size,
		FileHash: fileHash,
		BlockIds: blockIds,
	}
	EnhanceMetadata(&md, fileInfo)
	return md
}

// NewEmptyDirPathMetadata returns a PathMetadata representing a directory
// created at the given time. UID/GID are left unset; Birthtime is set to mtime.
func NewEmptyDirPathMetadata(mtime time.Time) PathMetadata {
	ts := NewTimestampFromTime(mtime)
	birth := ts
	return PathMetadata{ //nolint:exhaustruct
		FileMode:  0o700 | FileModeDir,
		Mtime:     ts,
		Birthtime: &birth,
	}
}

func (p *PathMetadata) MTime() time.Time {
	return time.Unix(p.Mtime.Sec, int64(p.Mtime.Nsec))
}

func (p *PathMetadata) HasUID() bool {
	return p.Uid != nil
}

func (p *PathMetadata) HasGID() bool {
	return p.Gid != nil
}

func (p *PathMetadata) HasBirthtime() bool {
	return p.Birthtime != nil
}

func (p *PathMetadata) HasSymLinkTarget() bool {
	return p.SymLinkTarget != nil
}

type RestorableMetadataFlag uint8

const (
	// This includes `FileModePerm`, `FileModeSetUid`, `FileModeSetGid`, `FileModeSticky` but
	// not `FileModeDir` or `FileModeSymlink`, because the latter indicates a fundamental change.
	RestorableMetadataMode      RestorableMetadataFlag = 1
	RestorableMetadataMTime     RestorableMetadataFlag = 2
	RestorableMetadataOwnership RestorableMetadataFlag = 4
	RestorableMetadataAll       RestorableMetadataFlag = RestorableMetadataMode | RestorableMetadataMTime | RestorableMetadataOwnership
	restorableMetadataModeMask  FileMode               = FileModePerm | FileModeSticky | FileModeSetUid | FileModeSetGid
)

// Compare all attributes that can be restored like `FileMode`, `Size`, `FileHash` etc.
// `Birthtime` is not compared because it cannot be restored.
// `BlockIds` are not compared because they should be the same if the `FileHash` is the same.
func (p *PathMetadata) IsEqualRestorableAttributes(other PathMetadata, flags RestorableMetadataFlag) bool {
	if p.FileMode&^restorableMetadataModeMask != other.FileMode&^restorableMetadataModeMask {
		return false
	}
	if p.Size != other.Size {
		return false
	}
	if p.FileHash != other.FileHash {
		return false
	}
	if p.HasSymLinkTarget() != other.HasSymLinkTarget() {
		return false
	}
	if p.HasSymLinkTarget() && *p.SymLinkTarget != *other.SymLinkTarget {
		return false
	}
	if flags&RestorableMetadataOwnership != 0 {
		if p.HasUID() != other.HasUID() || p.HasGID() != other.HasGID() {
			return false
		}
		if p.HasUID() && *p.Uid != *other.Uid {
			return false
		}
		if p.HasGID() && *p.Gid != *other.Gid {
			return false
		}
	}
	if flags&RestorableMetadataMTime != 0 && p.Mtime != other.Mtime {
		return false
	}
	if flags&RestorableMetadataMode != 0 &&
		p.FileMode&restorableMetadataModeMask != other.FileMode&restorableMetadataModeMask {
		return false
	}
	return true
}
