//go:build !wasm

package lib

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

type TestData struct{}

var td = TestData{} //nolint:gochecknoglobals

// Return a new FS that is cleaned up after the test.
// todo: Make the FS implementation configurable.
func (td TestData) NewFS(tb testing.TB) FS {
	tb.Helper()
	return td.NewRealFS(tb)
	// return NewMemoryFS(10000000)
}

// Return a new RealFS that is cleaned up after the test.
func (td TestData) NewRealFS(tb testing.TB) *RealFS {
	tb.Helper()
	dir := tb.TempDir()
	tb.Cleanup(func() {
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			_ = os.Chmod(path, 0o700) //nolint:gosec,forbidigo
			return nil
		})
		_ = os.RemoveAll(dir) //nolint:forbidigo
	})
	return NewRealFS(dir)
}

func (td TestData) RawKey(suffix string) RawKey {
	return RawKey([]byte(strings.Repeat("k", RawKeySize-len(suffix)) + suffix))
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
	p, err := NewPath(path)
	if err != nil {
		panic(err)
	}
	return &RevisionEntry{p, entryType, md}
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

func (td TestData) SHA256(content string) Sha256 {
	if len(content) == 0 {
		return Sha256{}
	}
	return CalculateSha256([]byte(content))
}

func (td TestData) NewTestFS(tb testing.TB, fs FS) *TestFS {
	tb.Helper()
	return &TestFS{fs, tb, NewAssert(tb)}
}

func (td TestData) NewTestRepository(tb testing.TB, fs FS) *TestRepository {
	tb.Helper()
	assert := NewAssert(tb)
	passphrase := "testpassphrase"
	storage, err := NewFileStorage(fs, StoragePurposeRepository)
	assert.NoError(err)
	repository, err := InitNewRepository(storage, []byte(passphrase))
	assert.NoError(err)
	return &TestRepository{repository, td.NewTestFS(tb, fs), passphrase, storage, tb, assert}
}

// Return the column at `column` for every line in `s`.
// A bit like the command `cut -f`.
func (td TestData) Column(s string, column int) string {
	column -= 1 // Columns are 1-based.
	if column < 0 {
		panic("column < 0")
	}
	lines := strings.Split(s, "\n")
	result := []string{}
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		parts := strings.Split(line, " ")
		i := 0
		for _, part := range parts {
			if len(part) == 0 {
				continue
			}
			if i == column {
				result = append(result, part)
				break
			}
			i += 1
		}
	}
	return strings.Join(result, "\n")
}

// Sort like the `sort` command.
func (td TestData) Sort(s string, column int) string {
	column -= 1 // Columns are 1-based.
	if column < 0 {
		panic("column < 0")
	}
	lines := strings.Split(s, "\n")
	slices.SortFunc(lines, func(a, b string) int {
		// Lazy way to remove all subsequent spaces.
		for strings.Contains(a, "  ") {
			a = strings.ReplaceAll(a, "  ", " ")
		}
		for strings.Contains(b, "  ") {
			b = strings.ReplaceAll(b, "  ", " ")
		}
		icols := strings.Split(a, " ")
		jcols := strings.Split(b, " ")
		if len(icols) < column {
			return 1
		}
		if len(jcols) < column {
			return -1
		}
		return strings.Compare(icols[column], jcols[column])
	})
	return strings.Join(lines, "\n")
}

func (td TestData) Dedent(s string) string {
	if s == "" {
		return ""
	}
	s = strings.TrimRight(s, " \t\n")
	if s[0] == '\n' {
		s = s[1:]
	}
	lines := strings.Split(s, "\n")
	minIndent := -1
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		indent := len(line) - len(strings.TrimLeft(line, " \t"))
		if minIndent == -1 || indent < minIndent {
			minIndent = indent
		}
	}
	for i, line := range lines {
		if len(line) == 0 {
			continue
		}
		lines[i] = line[minIndent:]
	}
	return strings.Join(lines, "\n")
}

// `option` can only be `-l` at the moment.
func (td TestData) Wc(option string, content string) int {
	if option != "-l" {
		panic("wc only supports -l")
	}
	return strings.Count(content, "\n") + 1
}

type TestFileInfo struct {
	Path    string
	Mode    fs.FileMode
	Size    int
	Content string
}

type TestRevisionEntryInfo struct {
	Path string
	Type RevisionEntryType
	Mode fs.FileMode
	Hash Sha256
}

type TestFS struct {
	FS
	t      testing.TB
	assert Assert
}

func (f *TestFS) Cat(path string) string {
	f.t.Helper()
	data, err := ReadFile(f.FS, path)
	f.assert.NoError(err)
	return string(data)
}

func (f *TestFS) Write(path string, content string) {
	f.t.Helper()
	dir := filepath.Dir(path)
	if dir != "." {
		f.MkdirAll(dir)
	}
	err := WriteFile(f.FS, path, []byte(content))
	f.assert.NoError(err)
}

func (f *TestFS) Rm(path string) {
	f.t.Helper()
	err := f.FS.RemoveAll(path) //nolint:staticcheck
	f.assert.NoError(err)
}

func (f *TestFS) RmAll(path string) {
	f.t.Helper()
	err := f.FS.RemoveAll(path) //nolint:staticcheck
	f.assert.NoError(err)
}

func (f *TestFS) Mkdir(path string) {
	f.t.Helper()
	err := f.FS.Mkdir(path)
	f.assert.NoError(err)
}

func (f *TestFS) MkdirAll(path string) {
	f.t.Helper()
	err := f.FS.MkdirAll(path)
	f.assert.NoError(err)
}

func (f *TestFS) Chmod(path string, mode fs.FileMode) {
	f.t.Helper()
	err := f.FS.Chmod(path, mode)
	f.assert.NoError(err)
}

func (f *TestFS) Chown(path string, uid int, gid int) {
	f.t.Helper()
	err := f.FS.Chown(path, uid, gid)
	f.assert.NoError(err)
}

func (f *TestFS) Touch(path string, mtime time.Time) {
	f.t.Helper()
	err := f.FS.Chmtime(path, mtime) //nolint:staticcheck
	f.assert.NoError(err)
}

func (f *TestFS) Stat(path string) fs.FileInfo {
	f.t.Helper()
	stat, err := f.FS.Stat(path)
	f.assert.NoError(err)
	return stat
}

func (f *TestFS) Sha256(path string) Sha256 {
	f.t.Helper()
	data, err := ReadFile(f.FS, path)
	f.assert.NoError(err)
	return CalculateSha256(data)
}

func (f *TestFS) FileMetadata(path string) *FileMetadata {
	f.t.Helper()
	stat := f.Stat(path)
	md := &FileMetadata{
		ModeAndPerm:   NewModeAndPerm(stat.Mode()),
		MTimeSec:      stat.ModTime().Unix(),
		MTimeNSec:     int32(stat.ModTime().Nanosecond()), //nolint:gosec
		Size:          stat.Size(),
		FileHash:      Sha256{},
		BlockIds:      nil,
		SymlinkTarget: "",
		UID:           0xffffffff,
		GID:           0xffffffff,
		BirthtimeSec:  -1,
		BirthtimeNSec: -1,
	}
	if stat.IsDir() {
		md.Size = 0
	}
	if md.ModeAndPerm.IsRegular() {
		md.FileHash = f.Sha256(path)
	}
	EnhanceMetadata(md, stat)
	return md
}

func (f *TestFS) Ls(path string) []TestFileInfo {
	f.t.Helper()
	fileInfos := []TestFileInfo{}
	err := f.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Base(path) == ".cling" {
			return filepath.SkipDir
		}
		f.assert.NoError(err)
		if path == "." {
			return nil
		}
		info := f.Stat(path)
		content := ""
		var size int
		if !info.IsDir() {
			content = f.Cat(path)
			size = int(info.Size())
		}
		fileInfos = append(fileInfos, TestFileInfo{
			Path:    path,
			Mode:    info.Mode(),
			Size:    size,
			Content: content,
		})
		return nil
	})
	f.assert.NoError(err)
	slices.SortFunc(fileInfos, func(a, b TestFileInfo) int { return strings.Compare(a.Path, b.Path) })
	return fileInfos
}

type TestRepository struct {
	*Repository
	*TestFS
	Passphrase string
	Storage    *FileStorage
	t          testing.TB
	assert     Assert
}

func (r *TestRepository) Head() RevisionId {
	r.t.Helper()
	head, err := r.Repository.Head()
	r.assert.NoError(err)
	return head
}

func (r *TestRepository) RevisionSnapshot(revisionId RevisionId, pathFilter PathFilter) []*RevisionEntry {
	r.t.Helper()
	tmpFS := td.NewFS(r.t)
	defer tmpFS.RemoveAll(".") //nolint:errcheck
	snapshot, err := NewRevisionSnapshot(r.Repository, revisionId, tmpFS)
	r.assert.NoError(err)
	defer snapshot.Remove() //nolint:errcheck
	reader := snapshot.Reader(pathFilter)
	entries := []*RevisionEntry{}
	for {
		entry, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		r.assert.NoError(err)
		entries = append(entries, entry)
	}
	return entries
}

func (r *TestRepository) RevisionSnapshotFileInfos(revisionId RevisionId, pathFilter PathFilter) []TestFileInfo {
	r.t.Helper()
	entries := r.RevisionSnapshot(revisionId, pathFilter)
	actual := []TestFileInfo{}
	for _, entry := range entries {
		content := ""
		if entry.Type != RevisionEntryDelete && entry.Metadata.ModeAndPerm.IsRegular() {
			// Rebuild the content from the repository.
			buf := bytes.NewBuffer([]byte{})
			for _, blockId := range entry.Metadata.BlockIds {
				data, _, err := r.ReadBlock(blockId)
				r.assert.NoError(err)
				buf.Write(data)
			}
			content = buf.String()
		}
		actual = append(actual, TestFileInfo{
			Path:    entry.Path.String(),
			Mode:    entry.Metadata.ModeAndPerm.AsFileMode(),
			Size:    int(entry.Metadata.Size),
			Content: content,
		})
	}
	return actual
}

func (r *TestRepository) RevisionEntryReaderInfos(reader RevisionEntryReader) []TestRevisionEntryInfo {
	infos := []TestRevisionEntryInfo{}
	for {
		entry, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		r.assert.NoError(err)
		infos = append(infos, TestRevisionEntryInfo{
			Path: entry.Path.String(),
			Type: entry.Type,
			Mode: entry.Metadata.ModeAndPerm.AsFileMode(),
			Hash: entry.Metadata.FileHash,
		})
	}
	return infos
}

func (r *TestRepository) RevisionInfos(revisionId RevisionId) []TestRevisionEntryInfo {
	r.t.Helper()
	revision, err := r.ReadRevision(revisionId)
	r.assert.NoError(err)
	return r.RevisionEntryReaderInfos(NewRevisionReader(r.Repository, &revision))
}

func (r *TestRepository) RevisionTempInfos(temp *RevisionTemp) []TestRevisionEntryInfo {
	r.t.Helper()
	return r.RevisionEntryReaderInfos(temp.Reader(nil))
}
