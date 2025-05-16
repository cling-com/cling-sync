package lib

import (
	"bytes"
	"io"
	"io/fs"
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
	UID           uint32 // 2^31 if not present.
	GID           uint32 // 2^31 if not present.
	BirthtimeSec  int64  // -1 if not present.
	BirthtimeNSec int32  // -1 if not present.
}

func (fm *FileMetadata) EstimatedSize() int {
	return 4 + 8 + 4 + 8 + len(fm.FileHash) + 4 + len(fm.BlockIds)*32 + len(fm.SymlinkTarget) + 4 + 4 + 8 + 4
}

func (fm *FileMetadata) IsEqual(other *FileMetadata) bool {
	var thisBuf bytes.Buffer
	var otherBuf bytes.Buffer
	if err := MarshalFileMetadata(fm, &thisBuf); err != nil {
		return false
	}
	if err := MarshalFileMetadata(other, &otherBuf); err != nil {
		return false
	}
	return bytes.Equal(thisBuf.Bytes(), otherBuf.Bytes())
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
