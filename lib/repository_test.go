package lib

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestRepositoryInitAndOpen(t *testing.T) {
	userPassphrase := []byte("user passphrase")
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		storage, err := NewFileStorage(t.TempDir(), StoragePurposeRepository)
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
		storage, err := NewFileStorage(t.TempDir(), StoragePurposeRepository)
		assert.NoError(err)
		repo1, err := InitNewRepository(storage, userPassphrase)
		assert.NoError(err)
		toml, err := repo1.storage.Open()
		assert.NoError(err)
		masterKeyInfo, err := parseRepositoryConfig(toml)
		assert.NoError(err)
		// Decrypt KEK "by hand".
		userKey, err := DeriveUserKey(userPassphrase, masterKeyInfo.UserKeySalt)
		assert.NoError(err)
		cipher, err := NewCipher(userKey)
		assert.NoError(err)
		kek := make([]byte, RawKeySize)
		_, err = Decrypt(masterKeyInfo.EncryptedKEK[:], cipher, masterKeyInfo.UserKeySalt[:], kek)
		assert.NoError(err)
		repo2, err := OpenRepository(storage, userPassphrase)
		assert.NoError(err)
		assert.Equal(repo1.kekCipher, repo2.kekCipher)
	})

	t.Run("OpenWithKeys", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		storage, err := NewFileStorage(t.TempDir(), StoragePurposeRepository)
		assert.NoError(err)
		repo1, err := InitNewRepository(storage, userPassphrase)
		assert.NoError(err)
		head, err := repo1.Head()
		assert.NoError(err)
		assert.Equal(true, head.IsRoot())
		keys, err := DecryptRepositoryKeys(storage, userPassphrase)
		assert.NoError(err)
		repo2, err := OpenRepositoryWithKeys(storage, keys)
		assert.NoError(err)
		assert.Equal(repo1.kekCipher, repo2.kekCipher)
	})

	for _, tamper := range []string{"UserKeySalt", "EncryptedKEK", "EncryptedBlockIdHmacKey"} {
		t.Run(fmt.Sprintf("Tampering with %s is detected", tamper), func(t *testing.T) {
			t.Parallel()
			assert := NewAssert(t)
			_, storage := testRepository(t)

			// Manipulate the UserKeySalt.
			configFilePath := filepath.Join(storage.clingDir, "repository.txt")
			_ = os.Chmod(configFilePath, 0o600)
			toml, err := storage.Open()
			assert.NoError(err)
			masterKeyInfo, err := parseRepositoryConfig(toml)
			assert.NoError(err)
			switch tamper {
			case "UserKeySalt":
				masterKeyInfo.UserKeySalt[0] ^= 1
				toml["encryption"]["user-key-salt"] = FormatRecoveryCode(masterKeyInfo.UserKeySalt[:])
			case "EncryptedKEK":
				masterKeyInfo.EncryptedKEK[0] ^= 1
				toml["encryption"]["encrypted-kek"] = FormatRecoveryCode(masterKeyInfo.EncryptedKEK[:])
			case "EncryptedBlockIdHmacKey":
				masterKeyInfo.EncryptedBlockIdHmacKey[0] ^= 1
				toml["encryption"]["encrypted-block-id-hmac"] = FormatRecoveryCode(
					masterKeyInfo.EncryptedBlockIdHmacKey[:],
				)
			default:
				panic("invalid tamper")
			}
			f, err := os.OpenFile(configFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
			assert.NoError(err)
			defer f.Close() //nolint:errcheck
			err = WriteToml(f, "", toml)
			assert.NoError(err)

			repo2, err := OpenRepository(storage, userPassphrase)
			assert.Error(err, "message authentication failed")
			assert.Nil(repo2)
		})
	}
}

func TestRepositoryMarshalUnmarshalBlockHeader(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut := BlockHeader{
			BlockId:           fakeBlockId("1"),
			Flags:             12345,
			EncryptedDEK:      fakeEncryptedKey("1"),
			EncryptedDataSize: 67890,
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
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

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
		assert.Equal("EncryptedDataSize", dataSize.Name)
		assert.Equal("uint32", dataSize.Type.Name())
		assert.Equal(4, int(dataSize.Type.Size()))
		assert.Equal(96, BlockHeaderSize)

		block := reflect.TypeOf(Block{}) //nolint:exhaustruct
		assert.Equal(2, block.NumField())
		headerField := block.Field(0)
		assert.Equal("Header", headerField.Name)
		assert.Equal("BlockHeader", headerField.Type.Name())
		dataField := block.Field(1)
		assert.Equal("EncryptedData", dataField.Name)
		assert.Equal("[]uint8", dataField.Type.String())
	})

	t.Run("Maximum block size", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, storage := testRepository(t)

		writeData := make([]byte, MaxBlockDataSize)
		_, _ = rand.Read(writeData)
		_, header, err := repo.WriteBlock(writeData, BlockBuf{})
		assert.NoError(err)
		assert.Equal(MaxEncryptedBlockDataSize, int(header.EncryptedDataSize))
		readData, _, err := repo.ReadBlock(header.BlockId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(writeData, readData)

		// Get the file size of the block file.
		file, err := os.Open(storage.blockPath(header.BlockId))
		assert.NoError(err)
		defer file.Close() //nolint:errcheck
		stat, err := file.Stat()
		assert.NoError(err)
		assert.Equal(int64(MaxBlockSize), stat.Size())
	})

	t.Run("Exceeding maximum block size", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		writeData := make([]byte, MaxBlockDataSize+1)
		_, header, err := repo.WriteBlock(writeData, BlockBuf{})
		assert.Error(err, "exceeds maximum block size")
		assert.Equal(BlockHeader{}, header) //nolint:exhaustruct
	})

	t.Run("Compression", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		// Create good compressible data.
		writeData := make([]byte, MaxBlockDataSize)
		for i := range writeData {
			writeData[i] = byte(i % 32)
		}
		assert.Equal(true, IsCompressible(writeData))
		existed, header, err := repo.WriteBlock(writeData, BlockBuf{})
		assert.NoError(err)
		assert.Equal(false, existed)
		assert.Equal(header.Flags&BlockFlagDeflate, BlockFlagDeflate)
		assert.Less(int(header.EncryptedDataSize), len(writeData)/5, "data should be very compressible")

		readData, _, err := repo.ReadBlock(header.BlockId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(writeData, readData)
	})

	t.Run("Compression is skipped if data is not compressible", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		// Create bad compressible data.
		writeData := make([]byte, MaxBlockDataSize)
		_, _ = rand.Read(writeData)
		assert.Equal(false, IsCompressible(writeData))
		existed, header, err := repo.WriteBlock(writeData, BlockBuf{})
		assert.NoError(err)
		assert.Equal(false, existed)
		assert.Equal(uint64(0), header.Flags&BlockFlagDeflate)
		assert.Equal(int(header.EncryptedDataSize), len(writeData)+TotalCipherOverhead)

		readData, _, err := repo.ReadBlock(header.BlockId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(writeData, readData)
	})

	t.Run("Compression is skipped if data is compressible but compression ratio is too low", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		// Create a block with random data.
		writeData := make([]byte, MaxBlockDataSize)
		_, _ = rand.Read(writeData)
		// But the first `compressionCheckSize` bytes are very compressible.
		for i := range compressionCheckSize {
			writeData[i] = byte(i % 32)
		}
		assert.Equal(true, IsCompressible(writeData))
		existed, header, err := repo.WriteBlock(writeData, BlockBuf{})
		assert.NoError(err)
		assert.Equal(false, existed)
		assert.Equal(uint64(0), header.Flags&BlockFlagDeflate)
		assert.Equal(int(header.EncryptedDataSize), len(writeData)+TotalCipherOverhead)

		readData, _, err := repo.ReadBlock(header.BlockId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(writeData, readData)
	})
}

func TestRepositoryReadWriteRevision(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		head, err := repo.Head()
		assert.NoError(err)

		_, blockHeader, err := repo.WriteBlock([]byte{1, 2, 3}, BlockBuf{})
		assert.NoError(err)

		revision := Revision{
			TimestampSec:  time.Now().Unix(),
			TimestampNSec: 1234,
			Message:       "test message",
			Author:        "test author",
			Blocks:        []BlockId{blockHeader.BlockId},
			Parent:        head,
		}
		revisionId, err := repo.WriteRevision(&revision, BlockBuf{})
		assert.NoError(err)

		readRevision, err := repo.ReadRevision(revisionId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(revision, readRevision)
	})

	t.Run("Write revision not based on current head", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		// Create a revision that is not based on the current head.
		revisionId := fakeRevisionId("1")
		_, blockHeader, err := repo.WriteBlock([]byte{1, 2, 3}, BlockBuf{})
		assert.NoError(err)

		revision := Revision{
			TimestampSec:  time.Now().Unix(),
			TimestampNSec: 1234,
			Message:       "test message",
			Author:        "test author",
			Blocks:        []BlockId{blockHeader.BlockId},
			Parent:        revisionId,
		}
		_, err = repo.WriteRevision(&revision, BlockBuf{})
		assert.ErrorIs(err, ErrHeadChanged)
	})

	t.Run("Read empty root revision", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		repo, _ := testRepository(t)

		head, err := repo.Head()
		assert.NoError(err)
		assert.Equal(true, head.IsRoot())

		_, err = repo.ReadRevision(head, BlockBuf{})
		assert.Error(err, "root revision cannot be read")
	})
}

func testRepository(t *testing.T) (*Repository, *FileStorage) {
	t.Helper()
	userPassphrase := []byte("user passphrase")
	assert := NewAssert(t)
	storage, err := NewFileStorage(t.TempDir(), StoragePurposeRepository)
	assert.NoError(err)
	repo, err := InitNewRepository(storage, userPassphrase)
	assert.NoError(err)
	return repo, storage
}
