package lib

import (
	"io"
	"io/fs"
	"time"
)

// The lower bits represent attributes (see `Mode` constants).
// Bits 0 to 8 represent the file permissions.
type ModeAndPerm uint32

func NewModeAndPerm(fm fs.FileMode) ModeAndPerm {
	mode := fm.Perm()
	if fm&fs.ModeDir != 0 {
		mode |= ModeDir
	}
	if fm&fs.ModeSymlink != 0 {
		mode |= ModeSymlink
	}
	if fm&fs.ModeSetuid != 0 {
		mode |= ModeSetUID
	}
	if fm&fs.ModeSetgid != 0 {
		mode |= ModeSetGID
	}
	if fm&fs.ModeSticky != 0 {
		mode |= ModeSticky
	}
	return ModeAndPerm(mode)
}

func (m ModeAndPerm) AsFileMode() fs.FileMode {
	mode := fs.FileMode(m.Perm())
	if m&ModeDir != 0 {
		mode |= fs.ModeDir
	}
	if m&ModeSymlink != 0 {
		mode |= fs.ModeSymlink
	}
	if m&ModeSetUID != 0 {
		mode |= fs.ModeSetuid
	}
	if m&ModeSetGID != 0 {
		mode |= fs.ModeSetgid
	}
	if m&ModeSticky != 0 {
		mode |= fs.ModeSticky
	}
	return mode
}

func (m ModeAndPerm) String() string {
	const str = "dLugtr"
	bits := []uint32{ModeDir, ModeSymlink, ModeSetUID, ModeSetGID, ModeSticky}
	var buf [14]byte
	for i, b := range bits {
		if m&ModeAndPerm(b) != 0 {
			buf[i] = str[i]
		} else {
			buf[i] = '-'
		}
	}
	const rwx = "rwxrwxrwx"
	for i, c := range rwx {
		if m&ModeAndPerm(1<<(8-i)) != 0 {
			buf[i+5] = byte(c)
		} else {
			buf[i+5] = '-'
		}
	}
	return string(buf[:])
}

// Return a string in the style of `ls -l`.
func (m ModeAndPerm) ShortString() string {
	var buf [10]byte
	buf[0] = '-'
	const rwx = "rwxrwxrwx"
	for i, c := range rwx {
		if m&ModeAndPerm(1<<(8-i)) != 0 {
			buf[i+1] = byte(c)
		} else {
			buf[i+1] = '-'
		}
	}
	if m&ModeSymlink != 0 {
		buf[0] = 'l'
	} else if m&ModeDir != 0 {
		buf[0] = 'd'
	}
	const ownerExecMask ModeAndPerm = 1 << 6
	const groupExecMask ModeAndPerm = 1 << 3
	const othersExecMask ModeAndPerm = 1 << 0
	// SetUID (modifies owner execute at index 3).
	if m&ModeSetUID != 0 {
		if m&ownerExecMask != 0 {
			buf[3] = 's'
		} else {
			buf[3] = 'S'
		}
	}
	// SetGID (modifies group execute at index 6).
	if m&ModeSetGID != 0 {
		if m&groupExecMask != 0 {
			buf[6] = 's'
		} else {
			buf[6] = 'S'
		}
	}
	// Sticky (modifies others execute at index 9).
	if m&ModeSticky != 0 {
		if m&othersExecMask != 0 {
			buf[9] = 't'
		} else {
			buf[9] = 'T'
		}
	}
	return string(buf[:])
}

func (m ModeAndPerm) IsDir() bool {
	return m&ModeDir != 0
}

func (m ModeAndPerm) IsSymlink() bool {
	return m&ModeSymlink != 0
}

func (m ModeAndPerm) IsRegular() bool {
	return m&ModeType == 0
}

func (m ModeAndPerm) IsSticky() bool {
	return m&ModeSticky != 0
}

func (m ModeAndPerm) IsSetUID() bool {
	return m&ModeSetUID != 0
}

func (m ModeAndPerm) IsSetGUID() bool {
	return m&ModeSetGID != 0
}

func (m ModeAndPerm) Perm() uint32 {
	return uint32(m & ModePerm)
}

const MetadataVersion uint16 = 1

const (
	ModePerm    = 0o777
	ModeDir     = 1 << 10
	ModeSymlink = 1 << 11
	ModeSetUID  = 1 << 12
	ModeSetGID  = 1 << 13
	ModeSticky  = 1 << 14
	ModeType    = ModeDir | ModeSymlink

	UIDUnset       = 0xffffffff
	BirthtimeUnset = -1
)

type FileMetadata struct {
	ModeAndPerm ModeAndPerm
	MTimeSec    int64
	MTimeNSec   int32
	Size        int64
	FileHash    Sha256
	BlockIds    []BlockId

	// SymlinkTarget can be the target of a symlink (`ModeSymlink` is set)
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
		ModeAndPerm: NewModeAndPerm(fileInfo.Mode()),
		MTimeSec:    mtime.Unix(),
		MTimeNSec:   int32(mtime.Nanosecond()), //nolint:gosec
		Size:        size,
		FileHash:    fileHash,
		BlockIds:    blockIds,

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
		ModeAndPerm: 0o700 | ModeDir,
		MTimeSec:    mtime.Unix(),
		MTimeNSec:   int32(mtime.Nanosecond()), //nolint:gosec
		Size:        0,
		FileHash:    Sha256{},
		BlockIds:    nil,

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
		4 + // ModeAndPerm
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
	// This includes `ModePerm`, `ModeSetUID`, `ModeSetGID`, `ModeSticky` but
	// not `ModeDir` or `ModeSymlink`, because the latter indicates a fundamental change.
	RestorableMetadataMode      RestorableMetadataFlag = 1
	RestorableMetadataMTime     RestorableMetadataFlag = 2
	RestorableMetadataOwnership RestorableMetadataFlag = 4
	RestorableMetadataAll       RestorableMetadataFlag = RestorableMetadataMode | RestorableMetadataMTime | RestorableMetadataOwnership
	restorableMetadataModeMask                         = ModePerm | ModeSticky | ModeSetUID | ModeSetGID
)

// Compare all attributes that can be restored like `ModeAndPerm`, `Size`, `FileHash` etc.
// Fields like `BirthtimeSec` and `BirthtimeNSec` are not compared because they cannot be restored.
// The `BlockIds` are not compared because they should be the same if the `FileHash` is the same.
func (fm *FileMetadata) IsEqualRestorableAttributes(other *FileMetadata, flags RestorableMetadataFlag) bool {
	return fm.ModeAndPerm&^restorableMetadataModeMask == other.ModeAndPerm&^restorableMetadataModeMask &&
		fm.Size == other.Size &&
		fm.FileHash == other.FileHash &&
		fm.SymlinkTarget == other.SymlinkTarget &&
		(flags&RestorableMetadataOwnership == 0 || fm.UID == other.UID && fm.GID == other.GID) &&
		(flags&RestorableMetadataMTime == 0 || fm.MTimeSec == other.MTimeSec && fm.MTimeNSec == other.MTimeNSec) &&
		(flags&RestorableMetadataMode == 0 || fm.ModeAndPerm&restorableMetadataModeMask == other.ModeAndPerm&restorableMetadataModeMask)
}

func MarshalFileMetadata(f *FileMetadata, w io.Writer) error {
	bw := NewBinaryWriter(w)
	bw.Write(MetadataVersion)
	bw.Write(f.ModeAndPerm)
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
	br.Read(&f.ModeAndPerm)
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
