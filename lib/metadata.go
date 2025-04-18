package lib

import (
	"io"
)

// The lower bits represent attributes (see `Mode` constants).
// Bits 22 to 32 represent the file permissions.
type ModeAndPerm uint32

const MetadataVersion uint16 = 1

const (
	ModeDir     = 1
	ModeSymlink = 2
	ModeSetUID  = 4
	ModeSetGUID = 8
	ModeSticky  = 16
	ModeDeleted = 32
	ModeMoved   = 64
)

type FileRevision struct {
	// todo: current tag
	SyncTimeSec  int64
	SyncTimeNSec int32
	ModeAndPerm  ModeAndPerm
	MTimeSec     int64
	MTimeNSec    int32
	Size         int64
	FileHash     Sha256
	BlockIds     []BlockId

	// Target can be the target of a symlink (`ModeSymlink` is set)
	// or the move target (`ModeMove` is set).
	Target string

	// *nix specific fields.
	UID           uint32 // 2^31 if not present.
	GID           uint32 // 2^31 if not present.
	BirthtimeSec  int64  // -1 if not present.
	BirthtimeNSec int32  // -1 if not present.
}

func MarshalFileRevision(f FileRevision, w io.Writer) error {
	bw := NewBinaryWriter(w)
	bw.Write(MetadataVersion)
	bw.Write(f.SyncTimeSec)
	bw.Write(f.SyncTimeNSec)
	bw.Write(f.ModeAndPerm)
	bw.Write(f.MTimeSec)
	bw.Write(f.MTimeNSec)
	bw.Write(f.Size)
	bw.Write(f.FileHash)
	bw.WriteLen(len(f.BlockIds))
	for _, blockId := range f.BlockIds {
		bw.Write(blockId)
	}
	bw.WriteString(f.Target)
	bw.Write(f.UID)
	bw.Write(f.GID)
	bw.Write(f.BirthtimeSec)
	bw.Write(f.BirthtimeNSec)
	return bw.Err
}

func UnmarshalFileRevision(r io.Reader) (FileRevision, error) {
	var f FileRevision
	br := NewBinaryReader(r)
	var version uint16
	br.Read(&version)
	if br.Err == nil && version != MetadataVersion {
		return f, Errorf("unsupported metadata version: %d", version)
	}
	br.Read(&f.SyncTimeSec)
	br.Read(&f.SyncTimeNSec)
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
	f.Target = br.ReadString()
	br.Read(&f.UID)
	br.Read(&f.GID)
	br.Read(&f.BirthtimeSec)
	br.Read(&f.BirthtimeNSec)
	return f, br.Err
}
