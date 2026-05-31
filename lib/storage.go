package lib

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"time"
)

type ControlFileSection string

const (
	ControlFileSectionRefs     ControlFileSection = "refs"
	ControlFileSectionSecurity ControlFileSection = "security"
	ControlFileSectionConf     ControlFileSection = "conf"
)

type StoragePurpose string

const (
	StoragePurposeRepository StoragePurpose = "repository"
	StoragePurposeWorkspace  StoragePurpose = "workspace"
)

const (
	MaxBlockSize       = 8 * 1024 * 1024
	MaxControlFileSize = 1 * 1024 * 1024
)

type (
	BlockId  Sha256Hmac
	BlockBuf struct {
		buf *[MaxBlockSize]byte
	}
)

const BlockIdSize = 32

func NewBlockBuf() BlockBuf {
	return BlockBuf{buf: new([MaxBlockSize]byte)}
}

// Bytes exposes the underlying fixed-size slice so callers (e.g. transport
// layers) can read directly into it without allocating.
func (b BlockBuf) Bytes() []byte {
	return b.buf[:]
}

// Counterpart of `BlockId.String()`. Parse a hex-encoded BlockId.
func NewBlockIdFromString(s string) (BlockId, error) {
	if len(s) != 2*BlockIdSize {
		return BlockId{}, Errorf(
			"invalid block id length %d hex chars, want %d",
			len(s),
			2*BlockIdSize,
		)
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return BlockId{}, WrapErrorf(err, "invalid block id")
	}
	return BlockId(raw), nil
}

func (id BlockId) String() string {
	return hex.EncodeToString(id[:])
}

func BlockIdCompare(a, b BlockId) int {
	return bytes.Compare(a[:], b[:])
}

type blockIdChunkMarshaller struct{}

func (blockIdChunkMarshaller) MarshallAll(ids []BlockId, w ProtobufWriter) error {
	for _, id := range ids {
		if err := w.WriteBytes(1, id[:]); err != nil {
			return WrapErrorf(err, "failed to write block id")
		}
	}
	return nil
}

func (blockIdChunkMarshaller) UnmarshallAll(r *ProtobufReader) ([]BlockId, error) {
	var ids []BlockId
	for !r.AtEnd() {
		tag, wireType, err := r.ReadTag()
		if err != nil {
			return nil, WrapErrorf(err, "failed to read block id tag")
		}
		if tag != 1 || wireType != 2 {
			return nil, Errorf("unexpected tag %d / wire type %d for block id", tag, wireType)
		}
		b, err := r.ReadBytes()
		if err != nil {
			return nil, WrapErrorf(err, "failed to read block id")
		}
		if len(b) != BlockIdSize {
			return nil, Errorf("block id must have length %d, got %d", BlockIdSize, len(b))
		}
		ids = append(ids, BlockId(b))
	}
	return ids, nil
}

func (blockIdChunkMarshaller) EntrySize(_ BlockId) int {
	return TagLen(1, 2) + VarintLen(BlockIdSize) + BlockIdSize
}

// NewBlockIdTempWriter returns a sorted, de-duplicating TempWriter for
// BlockIds backed by `fs`.
func NewBlockIdTempWriter(fs FS) *TempWriter[BlockId] {
	return NewTempWriterWithIgnoreDuplicates[BlockId](
		BlockIdCompare, blockIdChunkMarshaller{}, fs, DefaultTempChunkSize,
	)
}

// ReadSortedBlockIds drains `storage.ReadBlockIds` into a sorted Temp.
// `inspect`, if non-nil, sees every id before it is added to the writer.
func ReadSortedBlockIds(ctx context.Context, storage Storage, fs FS, inspect func(BlockId)) (*Temp[BlockId], error) {
	writer := NewBlockIdTempWriter(fs)
	var addErr error
	err := storage.ReadBlockIds(ctx, func(id BlockId) bool {
		if inspect != nil {
			inspect(id)
		}
		addErr = writer.Add(id)
		return addErr == nil
	})
	if err != nil {
		return nil, WrapErrorf(err, "failed to read block ids")
	}
	if addErr != nil {
		return nil, WrapErrorf(addErr, "failed to add block id")
	}
	temp, err := writer.Finalize()
	if err != nil {
		return nil, WrapErrorf(err, "failed to sort block ids")
	}
	return temp, nil
}

var (
	ErrStorageNotFound      = Errorf("storage not found")
	ErrStorageAlreadyExists = Errorf("storage already exists")
	ErrBlockNotFound        = Errorf("block not found")
	ErrControlFileNotFound  = Errorf("control file not found")
	ErrLockNotFound         = Errorf("lock not found")
)

// LockExistsError is returned by `Storage.Lock` when the lock is already
// held. Fields describe the current holder so a user can decide whether to
// wait or force-release.
type LockExistsError struct {
	Name      string
	Owner     string
	Host      string
	Pid       int
	CreatedAt time.Time
}

func (e *LockExistsError) Error() string {
	return fmt.Sprintf("lock %q held by %s pid %d (owner %s, created %s)",
		e.Name, e.Host, e.Pid, e.Owner, e.CreatedAt.Format(time.RFC3339))
}

type Storage interface {
	Init(ctx context.Context, config Toml, headerComment string) error
	Open(ctx context.Context) (Toml, error)
	HasBlock(ctx context.Context, blockId BlockId) (bool, error)

	// Stream all block ids present in storage. `yield` returns false to stop early.
	ReadBlockIds(ctx context.Context, yield func(BlockId) bool) error

	// Return `ErrBlockNotFound` if the block does not exist.
	ReadBlock(ctx context.Context, blockId BlockId, buf BlockBuf) ([]byte, error)

	// Write a block and return whether it was written.
	//
	// Returns `true` if the block was already present.
	WriteBlock(ctx context.Context, blockId BlockId, data []byte) (bool, error)

	// Return `ErrControlFileNotFound` if the control file does not exist.
	ReadControlFile(ctx context.Context, section ControlFileSection, name string) ([]byte, error)
	WriteControlFile(ctx context.Context, section ControlFileSection, name string, data []byte) error
	HasControlFile(ctx context.Context, section ControlFileSection, name string) (bool, error)

	// Return `ErrControlFileNotFound` if the control file does not exist.
	DeleteControlFile(ctx context.Context, section ControlFileSection, name string) error

	// Create a lock file in `.cling/<purpose>/locks/<name>`. Returns
	// `*LockExistsError` if the lock is already held by another acquirer.
	Lock(ctx context.Context, name string) (func() error, error)

	// Forcefully drop a lock regardless of ownership. The caller is responsible
	// for being sure the previous holder is dead. Returns `ErrLockNotFound` if
	// there is nothing to release.
	ForceUnlock(ctx context.Context, name string) error
}

type FileStorage struct {
	FS      FS
	Purpose StoragePurpose
}

func NewFileStorage(fs FS, purpose StoragePurpose) (*FileStorage, error) {
	return &FileStorage{FS: fs, Purpose: purpose}, nil
}

// FileStorage operates on a local FS, so most operations are fast and do not
// observe `ctx`. `ReadBlockIds` is the exception: it can walk a large tree, so
// it honors cancellation.
var _ Storage = (*FileStorage)(nil)

func (s *FileStorage) Init(_ context.Context, config Toml, headerComment string) error {
	stat, err := s.FS.Stat(".")
	if err != nil {
		return WrapErrorf(err, "failed to stat %s", s.FS)
	} else if !stat.IsDir() {
		return Errorf("%s is not a directory", s.FS)
	}
	purposeDir := filepath.Join(".cling", string(s.Purpose))
	_, err = s.FS.Stat(purposeDir)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return WrapErrorf(err, "failed to stat %s", purposeDir)
	}
	if err == nil {
		return ErrStorageAlreadyExists
	}
	err = s.FS.MkdirAll(purposeDir)
	if err != nil && !errors.Is(err, fs.ErrExist) {
		return WrapErrorf(err, "failed to create new directory %s", purposeDir)
	}
	// Create the directory layout.
	mkClingDir := func(names ...string) error {
		fullPath := filepath.Join(append([]string{".cling"}, names...)...)
		if err := s.FS.Mkdir(fullPath); err != nil {
			return WrapErrorf(err, "failed to create new directory %s", fullPath)
		}
		return nil
	}
	if err := mkClingDir(string(s.Purpose), "refs"); err != nil {
		return err
	}
	if err := mkClingDir(string(s.Purpose), "objects"); err != nil {
		return err
	}
	f, err := s.FS.OpenWrite(s.configFilePath())
	if err != nil {
		return WrapErrorf(err, "failed to open config file %s", s.configFilePath())
	}
	defer f.Close() //nolint:errcheck
	if err := WriteToml(f, headerComment, config); err != nil {
		return WrapErrorf(err, "failed to write config file %s", s.configFilePath())
	}
	if err := f.Close(); err != nil {
		return WrapErrorf(err, "failed to close config file %s", s.configFilePath())
	}
	if err := s.FS.Chmod(s.configFilePath(), 0o600); err != nil {
		return WrapErrorf(err, "failed to change permissions of %s", s.configFilePath())
	}
	return nil
}

func (s *FileStorage) Open(_ context.Context) (Toml, error) {
	f, err := s.FS.OpenRead(s.configFilePath())
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrStorageNotFound
	}
	if err != nil {
		return nil, WrapErrorf(err, "failed to open config file %s", s.configFilePath())
	}
	defer f.Close() //nolint:errcheck
	toml, err := ReadToml(f)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read config file %s", s.configFilePath())
	}
	return toml, nil
}

func (s *FileStorage) HasBlock(_ context.Context, blockId BlockId) (bool, error) {
	p := s.blockPath(blockId)
	_, err := s.FS.Stat(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, WrapErrorf(err, "failed to stat block file %s", p)
	}
	return true, nil
}

func (s *FileStorage) ReadBlockIds(ctx context.Context, yield func(BlockId) bool) error {
	objectsPath := filepath.Join(".cling", string(s.Purpose), "objects")
	stat, err := s.FS.Stat(objectsPath)
	if err != nil {
		return WrapErrorf(err, "failed to stat objects directory %s", objectsPath)
	}
	if !stat.IsDir() {
		return Errorf("objects path %s is not a directory", objectsPath)
	}
	err = s.FS.WalkDir(objectsPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return WrapErrorf(ctxErr, "block id listing canceled")
		}
		if d.IsDir() {
			return nil
		}
		if IsAtomicWriteTempFile(path) {
			return nil
		}
		relPath, err := filepath.Rel(objectsPath, path)
		if err != nil {
			return WrapErrorf(err, "failed to get relative block path for %s", path)
		}
		parts := strings.Split(filepath.ToSlash(relPath), "/")
		if len(parts) != 3 || len(parts[0]) != 2 || len(parts[1]) != 2 || len(parts[2]) != 60 {
			return Errorf("invalid block path %s", path)
		}
		blockId, err := NewBlockIdFromString(parts[0] + parts[1] + parts[2])
		if err != nil {
			return WrapErrorf(err, "invalid block path %s", path)
		}
		if !yield(blockId) {
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return WrapErrorf(err, "failed to read block ids")
	}
	return nil
}

func (s *FileStorage) WriteBlock(_ context.Context, blockId BlockId, data []byte) (bool, error) {
	if len(data) > MaxBlockSize {
		return false, Errorf("block %s is too large: %d", blockId, len(data))
	}
	targetPath := s.blockPath(blockId)
	_, err := s.FS.Stat(targetPath)
	if err == nil {
		return true, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, WrapErrorf(err, "failed to stat file for block %s", blockId)
	}
	if err := s.FS.MkdirAll(filepath.Dir(targetPath)); err != nil {
		return false, WrapErrorf(err, "failed to create directory for block %s", blockId)
	}
	if err := AtomicWriteFile(s.FS, targetPath, 0o400, data); err != nil {
		return false, WrapErrorf(err, "failed to write block %s", blockId)
	}
	return false, nil
}

// Return `ErrBlockNotFound` if the block does not exist.
func (s *FileStorage) ReadBlock(_ context.Context, blockId BlockId, buf BlockBuf) ([]byte, error) {
	path := s.blockPath(blockId)
	file, err := s.FS.OpenRead(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, WrapErrorf(ErrBlockNotFound, "block %s does not exist", blockId)
		}
		return nil, WrapErrorf(err, "failed to open block file %s", path)
	}
	defer file.Close() //nolint:errcheck
	data, err := buf.Read(file)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read block data %s", blockId)
	}
	return data, nil
}

func (s *FileStorage) WriteControlFile(_ context.Context, section ControlFileSection, name string, data []byte) error {
	if len(data) > MaxControlFileSize {
		return Errorf("control file %s/%s is too large: %d", section, name, len(data))
	}
	path, err := s.controlFilePath(section, name)
	if err != nil {
		return err
	}
	if err := s.FS.MkdirAll(filepath.Dir(path)); err != nil {
		return WrapErrorf(err, "failed to create directory for control file %s", path)
	}
	if err := AtomicWriteFile(s.FS, path, 0o600, data); err != nil {
		return WrapErrorf(err, "failed to write control file %s", path)
	}
	return nil
}

func (s *FileStorage) ReadControlFile(_ context.Context, section ControlFileSection, name string) ([]byte, error) {
	path, err := s.controlFilePath(section, name)
	if err != nil {
		return nil, err
	}
	f, err := s.FS.OpenRead(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, WrapErrorf(ErrControlFileNotFound, "control file %s/%s does not exist", section, name)
		}
		return nil, WrapErrorf(err, "failed to read control file %s", path)
	}
	defer f.Close() //nolint:errcheck
	data, err := io.ReadAll(io.LimitReader(f, MaxControlFileSize+1))
	if err != nil {
		return nil, WrapErrorf(err, "failed to read control file %s", path)
	}
	if len(data) > MaxControlFileSize {
		return nil, Errorf("control file %s exceeds maximum control file size %d", path, MaxControlFileSize)
	}
	return data, nil
}

func (s *FileStorage) HasControlFile(_ context.Context, section ControlFileSection, name string) (bool, error) {
	path, err := s.controlFilePath(section, name)
	if err != nil {
		return false, err
	}
	_, err = s.FS.Stat(path)
	return err == nil, nil
}

func (s *FileStorage) DeleteControlFile(_ context.Context, section ControlFileSection, name string) error {
	path, err := s.controlFilePath(section, name)
	if err != nil {
		return err
	}
	if err := s.FS.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return WrapErrorf(ErrControlFileNotFound, "control file %s/%s does not exist", section, name)
		}
		return WrapErrorf(err, "failed to delete control file %s", path)
	}
	return nil
}

func (s *FileStorage) Lock(ctx context.Context, name string) (func() error, error) {
	if err := ValidateStorageLockName(name); err != nil {
		return nil, err
	}
	path := filepath.Join(".cling", string(s.Purpose), "locks", name)
	if err := s.FS.MkdirAll(filepath.Dir(path)); err != nil {
		return nil, WrapErrorf(err, "failed to create directory for lock file %s", path)
	}
	unlock, err := s.FS.Lock(ctx, path)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create lock file %s", path)
	}
	return unlock, nil
}

// ForceUnlock removes the lock file. Any process still holding the orphaned
// flock keeps the kernel-level lock on its open fd until it exits, but a new
// acquirer opens a fresh file (different inode) and gets its own flock cleanly.
func (s *FileStorage) ForceUnlock(_ context.Context, name string) error {
	if err := ValidateStorageLockName(name); err != nil {
		return err
	}
	path := filepath.Join(".cling", string(s.Purpose), "locks", name)
	if err := s.FS.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return WrapErrorf(ErrLockNotFound, "lock %s does not exist", name)
		}
		return WrapErrorf(err, "failed to remove lock file %s", path)
	}
	return nil
}

// Read up to `MaxBlockSize` bytes from `src` into the buffer and return
// the populated sub-slice. If `src` has more than `MaxBlockSize` bytes
// available, return an error rather than silently truncating.
func (b BlockBuf) Read(src io.Reader) ([]byte, error) {
	data := b.buf[:]
	n, err := io.ReadFull(src, data)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return data[0:n], nil
		}
		return nil, WrapErrorf(err, "failed to read block data")
	}
	// The buffer filled exactly. If `src` has more bytes, the input exceeds
	// `MaxBlockSize` and must be rejected.
	var probe [1]byte
	if _, err := io.ReadFull(src, probe[:]); err == nil {
		return nil, Errorf("input exceeds maximum block size %d", MaxBlockSize)
	} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, WrapErrorf(err, "failed to read block data")
	}
	return data[0:n], nil
}

func (s *FileStorage) blockPath(blockId BlockId) string {
	hexPath := hex.EncodeToString(blockId[:])
	return filepath.Join(".cling", string(s.Purpose), "objects", hexPath[:2], hexPath[2:4], hexPath[4:])
}

func (s *FileStorage) controlFilePath(section ControlFileSection, name string) (string, error) {
	if err := ValidateControlFileName(name); err != nil {
		return "", err
	}
	return filepath.Join(".cling", string(s.Purpose), string(section), name), nil
}

func (s *FileStorage) configFilePath() string {
	return filepath.Join(".cling", fmt.Sprintf("%s.txt", s.Purpose))
}

func ValidateControlFileName(name string) error {
	if name == "" || len(name) > 64 {
		return Errorf("invalid control file name %q", name)
	}
	for i := range len(name) {
		c := name[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-'
		if !ok {
			return Errorf("invalid control file name %q", name)
		}
	}
	return nil
}

func ValidateStorageLockName(name string) error {
	if name == "" || len(name) > 64 {
		return Errorf("invalid lock name %q", name)
	}
	for i := range len(name) {
		c := name[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-'
		if !ok {
			return Errorf("invalid lock name %q", name)
		}
	}
	return nil
}
