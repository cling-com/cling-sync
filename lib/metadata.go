package lib

import (
	"io"
	"io/fs"
	"time"
)

const MetadataVersion uint16 = 1

const (
	UIDUnset       = 0xffffffff
	BirthtimeUnset = -1
)

type FileMetadata struct {
	FileMode  FileMode
	MTimeSec  int64
	MTimeNSec int32
	Size      int64
	FileHash  Sha256
	BlockIds  []BlockId

	// SymlinkTarget can be the target of a symlink (`FileModeSymlink` is set)
	SymlinkTarget string

	// *nix specific fields.
	UID           uint32 // 2^31 (UIDUnset) if not present.
	GID           uint32 // 2^31 (UIDUnset) if not present.
	BirthtimeSec  int64  // -1 (BirthtimeUnset) if not present.
	BirthtimeNSec int32  // -1 (BirthtimeUnset) if not present.
}

func NewFileMetadataFromFileInfo(fileInfo fs.FileInfo, fileHash Sha256, blockIds []BlockId) FileMetadata {
	mtime := fileInfo.ModTime().UTC()
	var size int64
	if !fileInfo.IsDir() {
		size = fileInfo.Size()
	}
	md := FileMetadata{
		FileMode:  NewFileMode(fileInfo.Mode()),
		MTimeSec:  mtime.Unix(),
		MTimeNSec: int32(mtime.Nanosecond()), //nolint:gosec
		Size:      size,
		FileHash:  fileHash,
		BlockIds:  blockIds,

		SymlinkTarget: "", // todo: handle symlinks

		UID:           UIDUnset,
		GID:           UIDUnset,
		BirthtimeSec:  BirthtimeUnset,
		BirthtimeNSec: BirthtimeUnset,
	}
	EnhanceMetadata(&md, fileInfo)
	return md
}

// Create an empty `FileMetadata` that represents a directory created at the given time.
func NewEmptyDirFileMetadata(mtime time.Time) FileMetadata {
	return FileMetadata{
		FileMode:  0o700 | FileModeDir,
		MTimeSec:  mtime.Unix(),
		MTimeNSec: int32(mtime.Nanosecond()), //nolint:gosec
		Size:      0,
		FileHash:  Sha256{},
		BlockIds:  nil,

		SymlinkTarget: "",

		UID:           UIDUnset,
		GID:           UIDUnset,
		BirthtimeSec:  mtime.Unix(),
		BirthtimeNSec: int32(mtime.Nanosecond()), //nolint:gosec
	}
}

func (fm *FileMetadata) MTime() time.Time {
	return time.Unix(fm.MTimeSec, int64(fm.MTimeNSec))
}

func (fm *FileMetadata) HasGID() bool {
	return fm.GID != UIDUnset
}

func (fm *FileMetadata) HasUID() bool {
	return fm.UID != UIDUnset
}

func (fm *FileMetadata) HasBirthtime() bool {
	return fm.BirthtimeSec != BirthtimeUnset
}

func (fm *FileMetadata) MarshalledSize() int {
	return 2 + // MetadataVersion
		4 + // FileMode
		8 + // MTimeSec
		4 + // MTimeNSec
		8 + // Size
		32 + // FileHash
		len(fm.BlockIds)*32 + 2 + // BlockIds + len(BlockIds)
		len(fm.SymlinkTarget) + 2 + // SymlinkTarget + len(SymlinkTarget)
		4 + // UID
		4 + // GID
		8 + // BirthtimeSec
		4 // BirthtimeNSec
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
// Fields like `BirthtimeSec` and `BirthtimeNSec` are not compared because they cannot be restored.
// The `BlockIds` are not compared because they should be the same if the `FileHash` is the same.
func (fm *FileMetadata) IsEqualRestorableAttributes(other *FileMetadata, flags RestorableMetadataFlag) bool {
	return fm.FileMode&^restorableMetadataModeMask == other.FileMode&^restorableMetadataModeMask &&
		fm.Size == other.Size &&
		fm.FileHash == other.FileHash &&
		fm.SymlinkTarget == other.SymlinkTarget &&
		(flags&RestorableMetadataOwnership == 0 || fm.UID == other.UID && fm.GID == other.GID) &&
		(flags&RestorableMetadataMTime == 0 || fm.MTimeSec == other.MTimeSec && fm.MTimeNSec == other.MTimeNSec) &&
		(flags&RestorableMetadataMode == 0 || fm.FileMode&restorableMetadataModeMask == other.FileMode&restorableMetadataModeMask)
}

func MarshalFileMetadata(f *FileMetadata, w io.Writer) error {
	bw := NewBinaryWriter(w)
	bw.Write(MetadataVersion)
	bw.Write(f.FileMode)
	bw.Write(f.MTimeSec)
	bw.Write(f.MTimeNSec)
	bw.Write(f.Size)
	bw.Write(f.FileHash)
	bw.WriteLen(len(f.BlockIds))
	for _, blockId := range f.BlockIds {
		bw.Write(blockId)
	}
	bw.WriteString(f.SymlinkTarget)
	bw.Write(f.UID)
	bw.Write(f.GID)
	bw.Write(f.BirthtimeSec)
	bw.Write(f.BirthtimeNSec)
	return bw.Err
}

func UnmarshalFileMetadata(r io.Reader) (*FileMetadata, error) {
	var f FileMetadata
	br := NewBinaryReader(r)
	var version uint16
	br.Read(&version)
	if br.Err == nil && version != MetadataVersion {
		return nil, Errorf("unsupported metadata version: %d", version)
	}
	br.Read(&f.FileMode)
	br.Read(&f.MTimeSec)
	br.Read(&f.MTimeNSec)
	br.Read(&f.Size)
	br.Read(&f.FileHash)
	blockIdCount := br.ReadLen()
	f.BlockIds = make([]BlockId, blockIdCount)
	for i := range blockIdCount {
		br.Read(&f.BlockIds[i])
	}
	f.SymlinkTarget = br.ReadString()
	br.Read(&f.UID)
	br.Read(&f.GID)
	br.Read(&f.BirthtimeSec)
	br.Read(&f.BirthtimeNSec)
	return &f, br.Err
}
