package lib

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

const (
	repositoryConfigFile        = "repository.txt"
	StorageVersion       uint16 = 1
)

type Storage interface {
	Init(MasterKeyInfo) error
	Open() (RepositoryConfig, error)
	HasBlock(blockId BlockId) (bool, error)
	ReadBlock(blockId BlockId, buf BlockBuf) ([]byte, BlockHeader, error)
	ReadBlockHeader(blockId BlockId) (BlockHeader, error)
	WriteBlock(block Block) (bool, error)
}

type FileStorage struct {
	Dir      string
	clingDir string
}

func NewFileStorage(dir string) (*FileStorage, error) {
	return &FileStorage{Dir: dir, clingDir: filepath.Join(dir, ".cling")}, nil
}

func (fs *FileStorage) Init(masterKeyInfo MasterKeyInfo) error {
	stat, err := os.Stat(fs.Dir)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(fs.Dir, 0o700); err != nil {
			return WrapErrorf(err, "failed to create directory %s", fs.Dir)
		}
	} else if err != nil {
		return WrapErrorf(err, "failed to stat directory %s", fs.Dir)
	}
	if !stat.IsDir() {
		return Errorf("%s is not a directory", fs.Dir)
	}
	files, err := os.ReadDir(fs.Dir)
	if err != nil {
		return WrapErrorf(err, "failed to read directory %s", fs.Dir)
	}
	if len(files) > 0 {
		return Errorf("directory %s is not empty", fs.Dir)
	}
	// Create the directory layout.
	mkClingDir := func(names ...string) error {
		fullPath := filepath.Join(append([]string{fs.clingDir}, names...)...)
		if err := os.Mkdir(fullPath, 0o700); err != nil {
			return WrapErrorf(err, "failed to create directory %s", fullPath)
		}
		return nil
	}
	if err := mkClingDir(); err != nil {
		return err
	}
	if err := mkClingDir("revisions"); err != nil {
		return err
	}
	if err := mkClingDir("objects"); err != nil {
		return err
	}
	repositoryFile := filepath.Join(fs.clingDir, repositoryConfigFile)
	config := &RepositoryConfig{MasterKeyInfo: masterKeyInfo, StorageFormat: "file", StorageVersion: StorageVersion}
	if err := WriteRepositoryConfigFile(repositoryFile, config); err != nil {
		return WrapErrorf(err, "failed to write config file %s", repositoryFile)
	}
	return nil
}

func (fs *FileStorage) Open() (RepositoryConfig, error) {
	_, err := os.Stat(fs.clingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return RepositoryConfig{}, Errorf("repository does not exist")
		}
		return RepositoryConfig{}, WrapErrorf(err, "failed to stat directory %s", fs.clingDir)
	}
	repositoryFile := filepath.Join(fs.clingDir, repositoryConfigFile)
	config, _, err := ReadRepositoryConfigFile(repositoryFile)
	if err != nil {
		return RepositoryConfig{}, err
	}
	return config, nil
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

func (fs *FileStorage) WriteBlock(block Block) (bool, error) {
	if len(block.Data) > MaxBlockDataSize {
		return false, Errorf("block data too large: %d bytes, max %d", len(block.Data), MaxBlockDataSize)
	}
	if int(block.Header.DataSize) != len(block.Data) {
		return false, Errorf("block header data size %d does not match block data size %d", block.Header.DataSize, len(block.Data))
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
		return false, WrapErrorf(err, "failed to marshal block header %s", hex.EncodeToString(block.Header.BlockId[:]))
	}
	headerBytes := header.Bytes()
	if _, err := file.Write(headerBytes); err != nil {
		_ = file.Close()
		// Try to delete the file in case of an error.
		if err := os.Remove(tmpPath); err != nil {
			return false, WrapErrorf(err, "failed to write header and failed to remove temporary file %s (it is garbage now)", tmpPath)
		}
		return false, WrapErrorf(err, "failed to write header of block %s to temporary file %s", hex.EncodeToString(block.Header.BlockId[:]), tmpPath)
	}
	if _, err := file.Write(block.Data); err != nil {
		_ = file.Close()
		// Try to delete the file in case of an error.
		if err := os.Remove(tmpPath); err != nil {
			return false, WrapErrorf(err, "failed to write data and failed to remove temporary file %s (it is garbage now)", tmpPath)
		}
		return false, WrapErrorf(err, "failed to write data of block %s to temporary file %s", hex.EncodeToString(block.Header.BlockId[:]), tmpPath)
	}
	if err := file.Close(); err != nil {
		// Try to delete the file in case of an error.
		if err := os.Remove(tmpPath); err != nil {
			return false, WrapErrorf(err, "failed to close temporary file %s and failed to remove it (it is garbage now)", tmpPath)
		}
		return false, WrapErrorf(err, "failed to close temporary file %s of block %s", tmpPath, hex.EncodeToString(block.Header.BlockId[:]))
	}
	if err := os.Rename(tmpPath, targetPath); err != nil {
		// Try to delete the file in case of an error.
		if err := os.Remove(tmpPath); err != nil {
			return false, WrapErrorf(err, "failed to rename temporary file %s to %s and failed to remove temporary file (it is garbage now)", tmpPath, targetPath)
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
		return nil, BlockHeader{}, Errorf("not enough bytes read from block file %s, want at least %d, got %d", path, BlockHeaderSize, bytesRead)
	}
	header, err := UnmarshalBlockHeader(blockId, bytes.NewBuffer(buf[:BlockHeaderSize]))
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to unmarshal block header of %s", path)
	}
	if int(header.DataSize) != bytesRead-BlockHeaderSize {
		return nil, BlockHeader{}, Errorf("read %d bytes, expected %d", bytesRead-BlockHeaderSize, header.DataSize)
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

func (fs *FileStorage) blockPath(blockId BlockId) string {
	hexPath := hex.EncodeToString(blockId[:])
	return filepath.Join(fs.clingDir, "objects", hexPath[:2], hexPath[2:4], hexPath[4:])
}
