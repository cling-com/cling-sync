package lib

import (
	"encoding/binary"
	"io"
	"io/fs"
	"time"
)

// NewPathMetadataFromFileInfo returns a PathMetadata populated from the
// given FileInfo. Platform-specific fields (UID/GID/Birthtime) are filled
// in via EnhanceMetadata.
func NewPathMetadataFromFileInfo(fileInfo fs.FileInfo, fileHash Sha256, blockIds []BlockId) PathMetadata {
	mtime := fileInfo.ModTime().UTC()
	var size int64
	if !fileInfo.IsDir() {
		size = fileInfo.Size()
	}
	md := PathMetadata{ //nolint:exhaustruct
		FileMode: NewFileMode(fileInfo.Mode()),
		Mtime:    Timestamp{Sec: mtime.Unix(), Nsec: uint32(mtime.Nanosecond())}, //nolint:gosec
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
	ts := Timestamp{Sec: mtime.Unix(), Nsec: uint32(mtime.Nanosecond())} //nolint:gosec
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

// MarshalledSize returns a rough upper-bound estimate of the protobuf-encoded
// size of this PathMetadata. It is only used for chunk-size budgeting in
// TempWriter and is intentionally over-estimated. This will be removed in
// a follow-up commit.
func (p *PathMetadata) MarshalledSize() int {
	n := 128 // fixed-field overhead + slack
	n += len(p.BlockIds) * 35
	if p.HasSymLinkTarget() {
		n += len(*p.SymLinkTarget) + 4
	}
	return n
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

// MarshalPathMetadata writes a length-prefixed, protobuf-encoded PathMetadata
// to w. This io.Writer wrapper bridges the existing RevisionEntry marshalling;
// it will be removed when RevisionEntry is replaced by RevisionEntry1.
func MarshalPathMetadata(p *PathMetadata, w io.Writer) error {
	buf := make([]byte, p.MarshalledSize()+1024)
	pw := NewProtobufWriter(buf)
	if err := p.Marshall(pw); err != nil {
		return WrapErrorf(err, "failed to marshal path metadata")
	}
	payload := pw.Bytes()
	if err := binary.Write(w, binary.LittleEndian, uint32(len(payload))); err != nil { //nolint:gosec
		return WrapErrorf(err, "failed to write path metadata length")
	}
	if _, err := w.Write(payload); err != nil {
		return WrapErrorf(err, "failed to write path metadata payload")
	}
	return nil
}

func UnmarshalPathMetadata(r io.Reader) (*PathMetadata, error) {
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return nil, WrapErrorf(err, "failed to read path metadata length")
	}
	buf := make([]byte, l)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, WrapErrorf(err, "failed to read path metadata payload")
	}
	p, err := UnmarshallPathMetadata(NewProtobufReader(buf))
	if err != nil {
		return nil, err
	}
	return &p, nil
}
