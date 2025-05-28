package lib

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type ControlFileSection string

const (
	ControlFileSectionRefs ControlFileSection = "refs"
)

type StoragePurpose string

const (
	StoragePurposeRepository StoragePurpose = "repository"
	StoragePurposeWorkspace  StoragePurpose = "workspace"
)

var (
	ErrStorageNotFound      = Errorf("storage not found")
	ErrStorageAlreadyExists = Errorf("storage already exists")
)

type Storage interface {
	Init(config Toml, headerComment string) error
	Open() (Toml, error)
	HasBlock(blockId BlockId) (bool, error)
	ReadBlock(blockId BlockId, buf BlockBuf) ([]byte, BlockHeader, error)
	ReadBlockHeader(blockId BlockId) (BlockHeader, error)
	// Write a block and return whether it was written.
	//
	// Returns `true` if the block was already present.
	WriteBlock(block Block) (bool, error)
	ReadControlFile(section ControlFileSection, name string) ([]byte, error)
	WriteControlFile(section ControlFileSection, name string, data []byte) error
}

type FileStorage struct {
	Dir      string
	Purpose  StoragePurpose
	clingDir string
}

func NewFileStorage(dir string, purpose StoragePurpose) (*FileStorage, error) {
	return &FileStorage{Dir: dir, Purpose: purpose, clingDir: filepath.Join(dir, ".cling")}, nil
}

func (fs *FileStorage) Init(config Toml, headerComment string) error {
	stat, err := os.Stat(fs.Dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(fs.Dir, 0o700); err != nil {
				return WrapErrorf(err, "failed to create directory %s", fs.Dir)
			}
		} else {
			return WrapErrorf(err, "failed to stat directory %s", fs.Dir)
		}
	} else if !stat.IsDir() {
		return Errorf("%s is not a directory", fs.Dir)
	}
	purposeDir := filepath.Join(fs.clingDir, string(fs.Purpose))
	_, err = os.Stat(purposeDir)
	if !errors.Is(err, os.ErrNotExist) {
		return ErrStorageAlreadyExists
	}
	err = os.MkdirAll(purposeDir, 0o700)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return WrapErrorf(err, "failed to create new directory %s", purposeDir)
	}
	// Create the directory layout.
	mkClingDir := func(names ...string) error {
		fullPath := filepath.Join(append([]string{fs.clingDir}, names...)...)
		if err := os.Mkdir(fullPath, 0o700); err != nil {
			return WrapErrorf(err, "failed to create new directory %s", fullPath)
		}
		return nil
	}
	if err := mkClingDir(string(fs.Purpose), "refs"); err != nil {
		return err
	}
	if err := mkClingDir(string(fs.Purpose), "objects"); err != nil {
		return err
	}
	f, err := os.OpenFile(fs.configFilePath(), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return WrapErrorf(err, "failed to open config file %s", fs.configFilePath())
	}
	defer f.Close() //nolint:errcheck
	if err := WriteToml(f, headerComment, config); err != nil {
		return WrapErrorf(err, "failed to write config file %s", fs.configFilePath())
	}
	if err := f.Close(); err != nil {
		return WrapErrorf(err, "failed to close config file %s", fs.configFilePath())
	}
	return nil
}

func (fs *FileStorage) Open() (Toml, error) {
	f, err := os.Open(fs.configFilePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrStorageNotFound
	}
	if err != nil {
		return nil, WrapErrorf(err, "failed to open config file %s", fs.configFilePath())
	}
	defer f.Close() //nolint:errcheck
	toml, err := ReadToml(f)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read config file %s", fs.configFilePath())
	}
	return toml, nil
}

func (fs *FileStorage) HasBlock(blockId BlockId) (bool, error) {
	p := fs.blockPath(blockId)
	_, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, WrapErrorf(err, "failed to stat block file %s", p)
	}
	return true, nil
}

func (fs *FileStorage) WriteBlock(block Block) (bool, error) { //nolint:funlen
	if len(block.EncryptedData) > MaxEncryptedBlockDataSize {
		return false, Errorf(
			"block data too large: %d bytes, max %d",
			len(block.EncryptedData),
			MaxEncryptedBlockDataSize,
		)
	}
	if int(block.Header.EncryptedDataSize) != len(block.EncryptedData) {
		return false, Errorf(
			"block header data size %d does not match block data size %d",
			block.Header.EncryptedDataSize,
			len(block.EncryptedData),
		)
	}
	targetPath := fs.blockPath(block.Header.BlockId)
	_, err := os.Stat(targetPath)
	if err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, WrapErrorf(err, "failed to stat block file %s", targetPath)
	}
	suffix, err := RandStr(32)
	if err != nil {
		return false, WrapErrorf(err, "failed to generate random suffix for %s", targetPath)
	}
	tmpPath := targetPath + suffix
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o700); err != nil {
		return false, WrapErrorf(err, "failed to create directory for %s", targetPath)
	}
	file, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE, 0o400)
	if err != nil {
		return false, WrapErrorf(err, "failed to open temporary file %s", tmpPath)
	}
	var header bytes.Buffer
	if err := MarshalBlockHeader(&block.Header, &header); err != nil {
		return false, WrapErrorf(err, "failed to marshal block header %s", block.Header.BlockId)
	}
	headerBytes := header.Bytes()
	if _, err := file.Write(headerBytes); err != nil {
		_ = file.Close()
		// Try to delete the file in case of an error.
		if err := os.Remove(tmpPath); err != nil {
			return false, WrapErrorf(
				err,
				"failed to write header and failed to remove temporary file %s (it is garbage now)",
				tmpPath,
			)
		}
		return false, WrapErrorf(
			err,
			"failed to write header of block %s to temporary file %s",
			block.Header.BlockId,
			tmpPath,
		)
	}
	if _, err := file.Write(block.EncryptedData); err != nil {
		_ = file.Close()
		// Try to delete the file in case of an error.
		if err := os.Remove(tmpPath); err != nil {
			return false, WrapErrorf(
				err,
				"failed to write data and failed to remove temporary file %s (it is garbage now)",
				tmpPath,
			)
		}
		return false, WrapErrorf(
			err,
			"failed to write data of block %s to temporary file %s",
			block.Header.BlockId,
			tmpPath,
		)
	}
	if err := file.Close(); err != nil {
		// Try to delete the file in case of an error.
		if err := os.Remove(tmpPath); err != nil {
			return false, WrapErrorf(
				err,
				"failed to close temporary file %s and failed to remove it (it is garbage now)",
				tmpPath,
			)
		}
		return false, WrapErrorf(err, "failed to close temporary file %s of block %s", tmpPath, block.Header.BlockId)
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		// Try to delete the file in case of an error.
		if err := os.Remove(tmpPath); err != nil {
			return false, WrapErrorf(
				err,
				"failed to rename temporary file %s to %s and failed to remove temporary file (it is garbage now)",
				tmpPath,
				targetPath,
			)
		}
		return false, WrapErrorf(err, "failed to rename temporary file %s to %s", tmpPath, targetPath)
	}
	return false, nil
}

func (fs *FileStorage) ReadBlock(blockId BlockId, buf BlockBuf) ([]byte, BlockHeader, error) {
	path := fs.blockPath(blockId)
	file, err := os.Open(path)
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to open block file %s", path)
	}
	defer file.Close() //nolint:errcheck
	bytesRead, err := file.Read(buf[:])
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to read block file %s", path)
	}
	if bytesRead < BlockHeaderSize {
		return nil, BlockHeader{}, Errorf(
			"not enough bytes read from block file %s, want at least %d, got %d",
			path,
			BlockHeaderSize,
			bytesRead,
		)
	}
	header, err := UnmarshalBlockHeader(blockId, bytes.NewBuffer(buf[:BlockHeaderSize]))
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to unmarshal block header of %s", path)
	}
	if int(header.EncryptedDataSize) != bytesRead-BlockHeaderSize {
		return nil, BlockHeader{}, Errorf(
			"read %d bytes, expected %d",
			bytesRead-BlockHeaderSize,
			header.EncryptedDataSize,
		)
	}
	return buf[BlockHeaderSize:bytesRead], header, nil
}

func (fs *FileStorage) ReadBlockHeader(blockId BlockId) (BlockHeader, error) {
	path := fs.blockPath(blockId)
	file, err := os.Open(path)
	if err != nil {
		return BlockHeader{}, WrapErrorf(err, "failed to open block file %s", path)
	}
	defer file.Close() //nolint:errcheck
	buf := [BlockHeaderSize]byte{}
	n, err := file.Read(buf[:])
	if err != nil {
		return BlockHeader{}, WrapErrorf(err, "failed to read block header of file %s", path)
	}
	if n != BlockHeaderSize {
		return BlockHeader{}, Errorf("read %d bytes, expected at least %d", n, BlockHeaderSize)
	}
	return UnmarshalBlockHeader(blockId, bytes.NewBuffer(buf[:]))
}

func (fs *FileStorage) WriteControlFile(section ControlFileSection, name string, data []byte) error {
	path, err := fs.controlFilePath(section, name)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return WrapErrorf(err, "failed to write control file %s", path)
	}
	return nil
}

func (fs *FileStorage) ReadControlFile(section ControlFileSection, name string) ([]byte, error) {
	path, err := fs.controlFilePath(section, name)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read control file %s", path)
	}
	return data, nil
}

func (fs *FileStorage) blockPath(blockId BlockId) string {
	hexPath := hex.EncodeToString(blockId[:])
	return filepath.Join(fs.clingDir, string(fs.Purpose), "objects", hexPath[:2], hexPath[2:4], hexPath[4:])
}

func (fs *FileStorage) controlFilePath(section ControlFileSection, name string) (string, error) {
	name = filepath.Clean(name)
	if strings.Contains(name, "/") || strings.Contains(name, "\\") || strings.Contains(name, "..") || len(name) == 0 {
		return "", Errorf("invalid file name %s", name)
	}
	return filepath.Join(fs.clingDir, string(fs.Purpose), string(section), name), nil
}

func (fs *FileStorage) configFilePath() string {
	return filepath.Join(fs.clingDir, fmt.Sprintf("%s.txt", fs.Purpose))
}
