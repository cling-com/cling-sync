package lib

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
)

type ControlFileSection string

const (
	ControlFileSectionRefs     ControlFileSection = "refs"
	ControlFileSectionSecurity ControlFileSection = "security"
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

func (id BlockId) String() string {
	return hex.EncodeToString(id[:])
}

var (
	ErrStorageNotFound      = Errorf("storage not found")
	ErrStorageAlreadyExists = Errorf("storage already exists")
	ErrBlockNotFound        = Errorf("block not found")
	ErrControlFileNotFound  = Errorf("control file not found")
)

type Storage interface {
	Init(config Toml, headerComment string) error
	Open() (Toml, error)
	HasBlock(blockId BlockId) (bool, error)

	// Return `ErrBlockNotFound` if the block does not exist.
	ReadBlock(blockId BlockId, buf BlockBuf) ([]byte, error)

	// Write a block and return whether it was written.
	//
	// Returns `true` if the block was already present.
	WriteBlock(blockId BlockId, data []byte) (bool, error)

	// Return `ErrControlFileNotFound` if the control file does not exist.
	ReadControlFile(section ControlFileSection, name string) ([]byte, error)
	WriteControlFile(section ControlFileSection, name string, data []byte) error
	HasControlFile(section ControlFileSection, name string) (bool, error)

	// Return `ErrControlFileNotFound` if the control file does not exist.
	DeleteControlFile(section ControlFileSection, name string) error

	// Create a lock file in `.cling/<purpose>/locks/<name>`.
	Lock(ctx context.Context, name string) (func() error, error)
}

type FileStorage struct {
	FS      FS
	Purpose StoragePurpose
}

func NewFileStorage(fs FS, purpose StoragePurpose) (*FileStorage, error) {
	return &FileStorage{FS: fs, Purpose: purpose}, nil
}

func (s *FileStorage) Init(config Toml, headerComment string) error {
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

func (s *FileStorage) Open() (Toml, error) {
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

func (s *FileStorage) HasBlock(blockId BlockId) (bool, error) {
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

func (s *FileStorage) WriteBlock(blockId BlockId, data []byte) (bool, error) {
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
func (s *FileStorage) ReadBlock(blockId BlockId, buf BlockBuf) ([]byte, error) {
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

func (s *FileStorage) WriteControlFile(section ControlFileSection, name string, data []byte) error {
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

func (s *FileStorage) ReadControlFile(section ControlFileSection, name string) ([]byte, error) {
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
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read control file %s", path)
	}
	return data, nil
}

func (s *FileStorage) HasControlFile(section ControlFileSection, name string) (bool, error) {
	path, err := s.controlFilePath(section, name)
	if err != nil {
		return false, err
	}
	_, err = s.FS.Stat(path)
	return err == nil, nil
}

func (s *FileStorage) DeleteControlFile(section ControlFileSection, name string) error {
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
	if filepath.Base(name) != name {
		return nil, Errorf("invalid file name %s", name)
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

// Read at most `MaxBlockSize` bytes from `src` into the buffer and return
// the populated sub-slice.
func (b BlockBuf) Read(src io.Reader) ([]byte, error) {
	data := b.buf[:]
	n, err := io.ReadFull(src, data)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return data[0:n], nil
		}
		return nil, WrapErrorf(err, "failed to read block data")
	}
	return data[0:n], nil
}

func (s *FileStorage) blockPath(blockId BlockId) string {
	hexPath := hex.EncodeToString(blockId[:])
	return filepath.Join(".cling", string(s.Purpose), "objects", hexPath[:2], hexPath[2:4], hexPath[4:])
}

func (s *FileStorage) controlFilePath(section ControlFileSection, name string) (string, error) {
	name = filepath.Clean(name)
	if strings.Contains(name, "/") || strings.Contains(name, "\\") || strings.Contains(name, "..") || len(name) == 0 {
		return "", Errorf("invalid file name %s", name)
	}
	return filepath.Join(".cling", string(s.Purpose), string(section), name), nil
}

func (s *FileStorage) configFilePath() string {
	return filepath.Join(".cling", fmt.Sprintf("%s.txt", s.Purpose))
}
