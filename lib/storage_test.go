package lib

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestFileStorageInit(t *testing.T) {
	t.Parallel()
	t.Run("Happy path (directory exists)", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewFileStorage(dir)
		assert.NoError(err)
		err = sut.Init(fakeMaserKeyInfo())
		assert.NoError(err)
		// Make sure the repository config file has been written.
		files, err := os.ReadDir(dir)
		assert.NoError(err)
		assert.Equal(1, len(files))
		clingDir := files[0]
		assert.Equal(".cling", clingDir.Name())
		assert.Equal(true, clingDir.IsDir())
		files, err = os.ReadDir(filepath.Join(dir, ".cling"))
		assert.NoError(err)
		assert.Equal(3, len(files))
		configFile := files[slices.IndexFunc(files, func(f os.DirEntry) bool { return !f.IsDir() })]
		assert.Equal("repository.txt", configFile.Name())
		config, _, err := ReadRepositoryConfigFile(filepath.Join(dir, ".cling", configFile.Name()))
		assert.NoError(err)
		assert.Equal(fakeMaserKeyInfo(), config.MasterKeyInfo)
		assert.Equal(StorageVersion, config.StorageVersion)
		assert.Equal("file", config.StorageFormat)
	})
}

func TestFileStorageOpen(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewFileStorage(dir)
		assert.NoError(err)
		err = sut.Init(fakeMaserKeyInfo())
		assert.NoError(err)

		sut, err = NewFileStorage(dir)
		assert.NoError(err)
		config, err := sut.Open()
		assert.NoError(err)
		assert.Equal(fakeMaserKeyInfo(), config.MasterKeyInfo)
		assert.Equal(StorageVersion, config.StorageVersion)
		assert.Equal("file", config.StorageFormat)
	})
	t.Run("Repository does not exist", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewFileStorage(dir)
		assert.NoError(err)
		_, err = sut.Open()
		assert.Error(err, "repository does not exist")
	})
	t.Run("Repository config file is missing", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewFileStorage(dir)
		assert.NoError(err)
		err = sut.Init(fakeMaserKeyInfo())
		assert.NoError(err)
		err = os.Remove(filepath.Join(dir, ".cling", repositoryConfigFile))
		assert.NoError(err)

		sut, err = NewFileStorage(dir)
		assert.NoError(err)
		_, err = sut.Open()
		assert.Error(err, "repository is corrupt")
	})
}

func TestFileStorageBlocks(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewFileStorage(dir)
		assert.NoError(err)
		err = sut.Init(fakeMaserKeyInfo())
		assert.NoError(err)

		block := Block{
			Header: BlockHeader{
				EncryptedDEK: fakeEncryptedKey("1"),
				BlockId:      fakeBlockId("1"),
				Flags:        0xffffffffffffffff,
				DataSize:     0, // is set below
			},
			Data: []byte("block 1 data"),
		}
		block.Header.DataSize = uint32(len(block.Data)) //nolint:gosec
		ok, err := sut.HasBlock(block.Header.BlockId)
		assert.NoError(err)
		assert.Equal(false, ok)

		// Write the block and verify its format on disk.
		existed, err := sut.WriteBlock(block)
		assert.NoError(err)
		assert.Equal(false, existed)
		f, err := os.Open(sut.blockPath(block.Header.BlockId))
		assert.NoError(err)
		defer f.Close() //nolint:errcheck
		// Read header.
		// We don't use the const `BlockHeaderSize` here so the test
		// also acts as a kind of regression test.
		headerBuf := make([]byte, 96)
		_, err = f.Read(headerBuf)
		assert.NoError(err)
		header := bytes.NewReader(headerBuf)
		var storageVersion uint16
		err = binary.Read(header, binary.LittleEndian, &storageVersion)
		assert.NoError(err)
		assert.Equal(StorageVersion, storageVersion)
		var flags uint64
		err = binary.Read(header, binary.LittleEndian, &flags)
		assert.NoError(err)
		assert.Equal(block.Header.Flags, flags)
		var encryptedDEK EncryptedKey
		err = binary.Read(header, binary.LittleEndian, &encryptedDEK)
		assert.NoError(err)
		assert.Equal(block.Header.EncryptedDEK, encryptedDEK)
		var dataSize uint32
		err = binary.Read(header, binary.LittleEndian, &dataSize)
		assert.NoError(err)
		assert.Equal(len(block.Data), int(dataSize))
		// Read data.
		data, err := io.ReadAll(f)
		assert.NoError(err)
		assert.Equal(block.Data, data)
		_ = f.Close()

		// Now `HasBlock` should return `true`.
		ok, err = sut.HasBlock(block.Header.BlockId)
		assert.NoError(err)
		assert.Equal(true, ok)

		// Read back the whole block with `ReadBlock`.
		readData, readHeader, err := sut.ReadBlock(block.Header.BlockId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(block.Header, readHeader)
		assert.Equal(block.Data, readData)

		// Write the block again - it should be seen as already existing.
		existed, err = sut.WriteBlock(block)
		assert.NoError(err)
		assert.Equal(true, existed)
	})
	t.Run("WriteBlock: Block.Data length must not exceed limits", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		dir := t.TempDir()
		sut, err := NewFileStorage(dir)
		assert.NoError(err)
		err = sut.Init(fakeMaserKeyInfo())
		assert.NoError(err)
		block := Block{
			Header: BlockHeader{
				EncryptedDEK: fakeEncryptedKey("1"),
				BlockId:      fakeBlockId("1"),
				Flags:        0,
				DataSize:     0, // is set below
			},
			Data: make([]byte, MaxBlockDataSize+1),
		}
		block.Header.DataSize = uint32(len(block.Data)) //nolint:gosec
		_, err = sut.WriteBlock(block)
		assert.Error(err, "block data too large")
		_, err = os.Stat(sut.blockPath(block.Header.BlockId))
		assert.Equal(true, os.IsNotExist(err))
	})
}

func TestBlockHeaderMarshalUnmarshal(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		header := BlockHeader{
			EncryptedDEK: fakeEncryptedKey("1"),
			BlockId:      fakeBlockId("1"),
			Flags:        0,
			DataSize:     1234,
		}
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, header)
		assert.NoError(err)
		var header2 BlockHeader
		err = binary.Read(buf, binary.LittleEndian, &header2)
		assert.NoError(err)
		assert.Equal(header, header2)
	})
}

func fakeMaserKeyInfo() MasterKeyInfo {
	return MasterKeyInfo{
		EncryptionVersion: EncryptionVersion,
		EncryptedKEK: EncryptedKey(
			[]byte("the encrypted kek 23456789abcdef0123456789abcdef0123456789abcdef01234567"),
		),
		EncryptedBlockIdHmacKey: EncryptedKey(
			[]byte("the encrypted block id hmac key 23456789abcdef0123456789abcdef0123456789abcdef01234567"),
		),
		UserKeySalt: Salt([]byte("the user salt ef0123456789abcdef")),
	}
}
