package lib

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

type TestData struct{}

var td = TestData{} //nolint:gochecknoglobals

func (td TestData) RawKey(suffix string) RawKey {
	return RawKey([]byte(strings.Repeat("k", RawKeySize-len(suffix)) + suffix))
}

func (td TestData) SHA256(suffix string) Sha256 {
	return Sha256([]byte(strings.Repeat("s", 32-len(suffix)) + suffix))
}

func (td TestData) EncryptedKey(suffix string) EncryptedKey {
	return EncryptedKey([]byte(strings.Repeat("e", EncryptedKeySize-len(suffix)) + suffix))
}

func (td TestData) BlockId(suffix string) BlockId {
	return BlockId([]byte(strings.Repeat("b", 32-len(suffix)) + suffix))
}

func (td TestData) RevisionId(suffix string) RevisionId {
	return RevisionId(td.SHA256(suffix))
}

func (td TestData) FileMetadata(mode ModeAndPerm) *FileMetadata {
	return &FileMetadata{
		ModeAndPerm:   mode,
		MTimeSec:      4567890,
		MTimeNSec:     567890,
		Size:          67890,
		FileHash:      td.SHA256("1"),
		BlockIds:      []BlockId{td.BlockId("1"), td.BlockId("2")},
		SymlinkTarget: "some/target",
		UID:           7890,
		GID:           890,
		BirthtimeSec:  90,
		BirthtimeNSec: 12345,
	}
}

func (td TestData) RevisionEntry(path string, entryType RevisionEntryType) *RevisionEntry {
	return td.RevisionEntryExt(path, entryType, 0o600, "test")
}

func (td TestData) RevisionEntryExt(
	path string,
	entryType RevisionEntryType,
	mode ModeAndPerm,
	content string,
) *RevisionEntry {
	sha := sha256.New()
	sha.Write([]byte(content))
	md := td.FileMetadata(mode)
	md.FileHash = Sha256(sha.Sum(nil))
	md.Size = int64(len(content))
	return &RevisionEntry{
		Path:     NewPath(strings.Split(path, "/")...),
		Type:     entryType,
		Metadata: md,
	}
}

func (td TestData) Revision(parent RevisionId) *Revision {
	return &Revision{
		TimestampSec:  123456789,
		TimestampNSec: 12345,
		Message:       "test message",
		Author:        "test author",
		Parent:        parent,
		Blocks:        []BlockId{td.BlockId("1")},
	}
}

func (td TestData) CommitInfo() *CommitInfo {
	return &CommitInfo{Author: "test author", Message: "test message"}
}

func details(msg []any) string {
	if len(msg) == 0 {
		return ""
	}
	if len(msg) == 1 {
		return fmt.Sprintf("%v: ", msg[0])
	}
	return fmt.Sprintf(msg[0].(string), msg[1:]...) + ": " //nolint:forcetypeassert
}
