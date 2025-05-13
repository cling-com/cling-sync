package lib

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRepositoryInitAndOpen(t *testing.T) {
	userPassphrase := []byte("user passphrase")
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		storage, err := NewFileStorage(t.TempDir())
		assert.NoError(err)
		repo1, err := InitNewRepository(storage, userPassphrase)
		assert.NoError(err)
		head, err := repo1.Head()
		assert.NoError(err)
		assert.Equal(true, head.IsRoot())
		repo2, err := OpenRepository(storage, userPassphrase)
		assert.NoError(err)
		assert.Equal(repo1.kekCipher, repo2.kekCipher)
	})
	t.Run("MasterKeyInfo.EncryptedKEK is actually encrypted with user's passphrase", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		storage, err := NewFileStorage(t.TempDir())
		assert.NoError(err)
		repo1, err := InitNewRepository(storage, userPassphrase)
		assert.NoError(err)
		config, err := repo1.storage.Open()
		assert.NoError(err)
		// Decrypt KEK "by hand".
		userKey, err := DeriveUserKey(userPassphrase, config.MasterKeyInfo.UserKeySalt)
		assert.NoError(err)
		cipher, err := NewCipher(userKey)
		assert.NoError(err)
		kek := make([]byte, RawKeySize)
		_, err = Decrypt(config.MasterKeyInfo.EncryptedKEK[:], cipher, config.MasterKeyInfo.UserKeySalt[:], kek)
		assert.NoError(err)
		repo2, err := OpenRepository(storage, userPassphrase)
		assert.NoError(err)
		assert.Equal(repo1.kekCipher, repo2.kekCipher)
	})
	t.Run("Tampering with UserKeySalt is detected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		storage, err := NewFileStorage(t.TempDir())
		assert.NoError(err)
		_, err = InitNewRepository(storage, userPassphrase)
		assert.NoError(err)

		// Manipulate the UserKeySalt.
		configFilePath := filepath.Join(storage.clingDir, repositoryConfigFile)
		_ = os.Chmod(configFilePath, 0o600)
		config, err := storage.Open()
		assert.NoError(err)
		config.MasterKeyInfo.UserKeySalt[0] ^= 1
		err = WriteRepositoryConfigFile(configFilePath, &config)
		assert.NoError(err)

		repo2, err := OpenRepository(storage, userPassphrase)
		assert.Error(err, "message authentication failed")
		assert.Nil(repo2)
	})
	t.Run("Tampering with EncryptedKEK is detected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		storage, err := NewFileStorage(t.TempDir())
		assert.NoError(err)
		_, err = InitNewRepository(storage, userPassphrase)
		assert.NoError(err)

		// Manipulate the KEK.
		configFilePath := filepath.Join(storage.clingDir, repositoryConfigFile)
		_ = os.Chmod(configFilePath, 0o600)
		config, err := storage.Open()
		assert.NoError(err)
		config.MasterKeyInfo.UserKeySalt[0] ^= 1
		err = WriteRepositoryConfigFile(configFilePath, &config)
		assert.NoError(err)

		repo2, err := OpenRepository(storage, userPassphrase)
		assert.Error(err, "message authentication failed")
		assert.Nil(repo2)
	})
}

func TestRepositoryMarshalUnmarshalBlockHeader(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := BlockHeader{
			BlockId:      fakeBlockId("1"),
			Flags:        12345,
			EncryptedDEK: fakeEncryptedKey("1"),
			DataSize:     67890,
		}
		buf := bytes.NewBuffer([]byte{})
		err := MarshalBlockHeader(&sut, buf)
		assert.NoError(err)
		assert.Equal(BlockHeaderSize, len(buf.Bytes()))
		read, err := UnmarshalBlockHeader(sut.BlockId, buf)
		assert.NoError(err)
		assert.Equal(sut, read)
		assert.Equal(0, len(buf.Bytes()), "All of `BlockHeaderSize` bytes must have been read")
	})
}

func TestRepositoryReadWriteBlock(t *testing.T) {
	t.Parallel()
	userPassphrase := []byte("user passphrase")
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		storage, err := NewFileStorage(t.TempDir())
		assert.NoError(err)
		repo, err := InitNewRepository(storage, userPassphrase)
		assert.NoError(err)

		writeData := []byte("plaintext")
		existed, writeHeader, err := repo.WriteBlock(writeData, BlockBuf{})
		assert.NoError(err)
		assert.Equal(false, existed)

		readData, readHeader, err := repo.ReadBlock(writeHeader.BlockId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(writeData, readData)
		assert.Equal(writeHeader, readHeader)
	})
	// Because we write `Block` / `BlockHeader` to disk and might reuse those objects
	// we have to make sure that the struct definition does not change.
	t.Run("Structure of `BlockHeader` and `Block` must not change unexpectedly", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		header := reflect.TypeOf(BlockHeader{}) //nolint:exhaustruct
		assert.Equal(4, header.NumField())
		blockId := header.Field(0)
		assert.Equal("BlockId", blockId.Name)
		assert.Equal("BlockId", blockId.Type.Name())
		assert.Equal(32, int(blockId.Type.Size()))
		flags := header.Field(1)
		assert.Equal("Flags", flags.Name)
		assert.Equal("uint64", flags.Type.Name())
		encryptedDEK := header.Field(2)
		assert.Equal("EncryptedDEK", encryptedDEK.Name)
		assert.Equal("EncryptedKey", encryptedDEK.Type.Name())
		assert.Equal(72, int(encryptedDEK.Type.Size()))
		dataSize := header.Field(3)
		assert.Equal("DataSize", dataSize.Name)
		assert.Equal("uint32", dataSize.Type.Name())
		assert.Equal(4, int(dataSize.Type.Size()))
		assert.Equal(96, BlockHeaderSize)

		block := reflect.TypeOf(Block{}) //nolint:exhaustruct
		assert.Equal(2, block.NumField())
		headerField := block.Field(0)
		assert.Equal("Header", headerField.Name)
		assert.Equal("BlockHeader", headerField.Type.Name())
		dataField := block.Field(1)
		assert.Equal("Data", dataField.Name)
		assert.Equal("[]uint8", dataField.Type.String())
	})
}
