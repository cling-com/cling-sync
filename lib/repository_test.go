package lib

import (
	"crypto/rand"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestRepositoryInitAndOpen(t *testing.T) {
	userPassphrase := []byte("user passphrase")
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		storage, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
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
		storage, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		repo, err := InitNewRepository(storage, userPassphrase)
		assert.NoError(err)
		toml, err := repo.storage.Open()
		assert.NoError(err)
		masterKeyInfo, err := parseRepositoryConfig(toml)
		assert.NoError(err)

		// Decrypt KEK "by hand".
		userKey, err := DeriveUserKey(userPassphrase, masterKeyInfo.Argon2id)
		assert.NoError(err)
		userKeyCipher, err := NewCipher(userKey)
		assert.NoError(err)
		rawKEK := make([]byte, RawKeySize)
		rawKEK, err = Decrypt(masterKeyInfo.EncryptedKEK[:], userKeyCipher, masterKeyInfo.Argon2id.Salt[:], rawKEK)
		assert.NoError(err)

		// Create the KEK cipher "by hand".
		kekCipher, err := NewCipher(RawKey(rawKEK))
		assert.NoError(err)
		assert.Equal(kekCipher, repo.kekCipher)

		// Make sure both ciphers are really the same by encrypting something with one and
		// decrypting with the other.
		data := make([]byte, 256)
		_, _ = rand.Read(data)
		encrypted, err := Encrypt(data, kekCipher, nil, make([]byte, 512))
		assert.NoError(err)
		decrypted, err := Decrypt(encrypted, repo.kekCipher, nil, make([]byte, 512))
		assert.NoError(err)
		assert.Equal(decrypted, data)
	})

	t.Run("OpenWithKeys", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		storage, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
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

	for _, tamper := range []string{"UserKeySalt", "EncryptedKEK", "EncryptedBlockIdHmacKey", "EncryptedGearCDCSeed"} {
		t.Run(fmt.Sprintf("Tampering with %s is detected", tamper), func(t *testing.T) {
			t.Parallel()
			assert := NewAssert(t)
			r := td.NewTestRepository(t, td.NewFS(t))

			configFilePath := filepath.Join(".cling", "repository.txt")
			r.Chmod(configFilePath, 0o600)
			toml, err := r.Storage.Open()
			assert.NoError(err)
			masterKeyInfo, err := parseRepositoryConfig(toml)
			assert.NoError(err)
			switch tamper {
			case "UserKeySalt":
				masterKeyInfo.Argon2id.Salt[0] ^= 1
				toml["encryption"]["user-key-salt"] = FormatRecoveryCode(masterKeyInfo.Argon2id.Salt[:])
			case "EncryptedKEK":
				masterKeyInfo.EncryptedKEK[0] ^= 1
				toml["encryption"]["encrypted-kek"] = FormatRecoveryCode(masterKeyInfo.EncryptedKEK[:])
			case "EncryptedBlockIdHmacKey":
				masterKeyInfo.EncryptedBlockIdHmacKey[0] ^= 1
				toml["encryption"]["encrypted-block-id-hmac"] = FormatRecoveryCode(
					masterKeyInfo.EncryptedBlockIdHmacKey[:],
				)
			case "EncryptedGearCDCSeed":
				masterKeyInfo.EncryptedGearCDCSeed[0] ^= 1
				toml["encryption"]["encrypted-gear-cdc-seed"] = FormatRecoveryCode(
					masterKeyInfo.EncryptedGearCDCSeed[:],
				)
			default:
				panic("invalid tamper")
			}
			f, err := r.OpenWrite(configFilePath)
			assert.NoError(err)
			defer f.Close() //nolint:errcheck
			err = WriteToml(f, "", toml)
			assert.NoError(err)

			repo2, err := OpenRepository(r.Storage, userPassphrase)
			assert.Error(err, "message authentication failed")
			assert.Nil(repo2)
		})
	}
}

func TestRepositoryReadWriteBlock(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		writeData := []byte("plaintext")
		blockId, bytesWritten, err := r.WriteBlock(writeData)
		assert.NoError(err)
		assert.NotNil(bytesWritten)

		buf := NewBlockBuf()
		readData, err := r.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal(writeData, readData)
	})

	t.Run("Writing the same block twice is deduplicated", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		writeData := []byte("some data")
		blockId1, bytesWritten1, err := r.WriteBlock(writeData)
		assert.NoError(err)
		assert.NotNil(bytesWritten1)

		blockId2, bytesWritten2, err := r.WriteBlock(writeData)
		assert.NoError(err)
		assert.Equal(blockId1, blockId2)
		assert.Nil(bytesWritten2)
	})

	t.Run("Tampering any byte of the encrypted block is detected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		writeData := make([]byte, 275)
		_, _ = rand.Read(writeData)

		blockId, bytesWritten, err := r.WriteBlock(writeData)
		assert.NoError(err)
		assert.Equal(len(writeData), *bytesWritten)

		path := r.Storage.blockPath(blockId)
		r.Chmod(path, 0o600)
		onDisk, err := ReadFile(r.Storage.FS, path)
		assert.NoError(err)

		// Make sure we picked `len(writeData)` so that padding is added.
		assert.Greater(Padme(uint64(len(writeData))), uint64(len(writeData)))

		buf := NewBlockBuf()
		prev := -1
		for i := range onDisk {
			// Restore the previous flip and confirm the block reads again.
			if prev >= 0 {
				onDisk[prev] ^= 1
				err = WriteFile(r.Storage.FS, path, onDisk)
				assert.NoError(err)
				readData, err := r.ReadBlock(blockId, buf)
				assert.NoError(err, "after restoring offset %d", prev)
				assert.Equal(writeData, readData)
			}
			// Flip byte `i` and confirm the block no longer reads.
			onDisk[i] ^= 1
			err = WriteFile(r.Storage.FS, path, onDisk)
			assert.NoError(err)
			_, err = r.ReadBlock(blockId, buf)
			assert.Error(err, "", "offset %d", i)
			prev = i
		}
	})

	t.Run("Renaming a block file to a different blockId must fail to read", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		blockIdA, _, err := r.WriteBlock([]byte("block A"))
		assert.NoError(err)
		blockIdB, _, err := r.WriteBlock([]byte("block B"))
		assert.NoError(err)

		// Move block A's file to block B's path so that `blockIdB`'s filename now holds
		// bytes that were encrypted with `blockIdA` as the KEK-AEAD associated data.
		pathA := r.Storage.blockPath(blockIdA)
		pathB := r.Storage.blockPath(blockIdB)
		err = r.Storage.FS.Remove(pathB)
		assert.NoError(err)
		err = r.Storage.FS.Rename(pathA, pathB)
		assert.NoError(err)

		buf := NewBlockBuf()
		_, err = r.ReadBlock(blockIdB, buf)
		assert.Error(err, "message authentication failed")
	})

	t.Run("Maximum block size", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		writeData := make([]byte, MaxBlockDataSize)
		_, _ = rand.Read(writeData)
		blockId, _, err := r.WriteBlock(writeData)
		assert.NoError(err)
		buf := NewBlockBuf()
		readData, err := r.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal(writeData, readData)

		// On-disk size must not exceed MaxBlockSize.
		stat := r.Stat(r.Storage.blockPath(blockId))
		assert.Less(stat.Size(), int64(MaxBlockSize)+1)
		// Padmé must be a no-op at the maximum payload size.
		assert.Equal(uint64(MaxBlockDataSize), Padme(uint64(MaxBlockDataSize)))
	})

	t.Run("Exceeding maximum block size", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		writeData := make([]byte, MaxBlockDataSize+1)
		blockId, bytesWritten, err := r.WriteBlock(writeData)
		assert.Error(err, "exceeds maximum block size")
		assert.Equal(BlockId{}, blockId)
		assert.Nil(bytesWritten)
	})

	t.Run("Compression", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// Create good compressible data.
		writeData := make([]byte, MaxBlockDataSize)
		for i := range writeData {
			writeData[i] = byte(i % 32)
		}
		assert.Equal(true, IsCompressible(writeData))
		blockId, bytesWritten, err := r.WriteBlock(writeData)
		assert.NoError(err)
		assert.NotNil(bytesWritten)
		assert.Less(*bytesWritten, len(writeData)/5, "data should be compressed")

		buf := NewBlockBuf()
		readData, err := r.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal(writeData, readData)
	})

	t.Run("Compression is skipped if data is not compressible", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// Create bad compressible data.
		writeData := make([]byte, MaxBlockDataSize)
		_, _ = rand.Read(writeData)
		assert.Equal(false, IsCompressible(writeData))
		blockId, bytesWritten, err := r.WriteBlock(writeData)
		assert.NoError(err)
		assert.NotNil(bytesWritten)
		assert.Equal(len(writeData), *bytesWritten)

		buf := NewBlockBuf()
		readData, err := r.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal(writeData, readData)
	})

	t.Run("Compression is skipped if data is compressible but compression ratio is too low", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// Create a block with random data.
		writeData := make([]byte, MaxBlockDataSize)
		_, _ = rand.Read(writeData)
		// But the first `compressionCheckSize` bytes are very compressible.
		for i := range compressionCheckSize {
			writeData[i] = byte(i % 32)
		}
		assert.Equal(true, IsCompressible(writeData))
		blockId, bytesWritten, err := r.WriteBlock(writeData)
		assert.NoError(err)
		assert.NotNil(bytesWritten)
		assert.Equal(len(writeData), *bytesWritten)

		buf := NewBlockBuf()
		readData, err := r.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal(writeData, readData)
	})
}

func TestRepositoryReadWriteRevision(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		head := r.Head()
		blockId, _, err := r.WriteBlock([]byte{1, 2, 3})
		assert.NoError(err)

		msg := "test message"
		author := "test author"
		revision := Revision{ //nolint:exhaustruct
			Timestamp:        Timestamp{Sec: time.Now().Unix(), Nsec: 1234},
			Message:          &msg,
			Author:           &author,
			BlockIds:         []BlockId{blockId},
			ParentRevisionId: head,
		}
		revisionId, err := r.WriteRevision(&revision)
		assert.NoError(err)

		buf := NewBlockBuf()
		readRevision, err := r.ReadRevision(revisionId, buf)
		assert.NoError(err)
		assert.Equal(revision, readRevision)
	})

	t.Run("Write revision not based on current head", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		// Create a revision that is not based on the current head.
		revisionId := td.RevisionId("1")
		blockId, _, err := r.WriteBlock([]byte{1, 2, 3})
		assert.NoError(err)

		msg := "test message"
		author := "test author"
		revision := Revision{ //nolint:exhaustruct
			Timestamp:        Timestamp{Sec: time.Now().Unix(), Nsec: 1234},
			Message:          &msg,
			Author:           &author,
			BlockIds:         []BlockId{blockId},
			ParentRevisionId: revisionId,
		}
		_, err = r.WriteRevision(&revision)
		assert.ErrorIs(err, ErrHeadChanged)
	})

	t.Run("Read empty root revision", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		r := td.NewTestRepository(t, td.NewFS(t))

		head := r.Head()
		assert.Equal(true, head.IsRoot())

		buf := NewBlockBuf()
		_, err := r.ReadRevision(head, buf)
		assert.Error(err, "root revision cannot be read")
	})
}

func TestPadme(t *testing.T) {
	t.Parallel()
	// Reference values taken from https://lbarman.ch/blog/padme/
	cases := []struct {
		l, want uint64
	}{
		{0, 0}, // l < 2: returned unchanged
		{1, 1}, // l < 2: returned unchanged
		{2, 2}, // smallest value that hits the formula
		{100, 104},
		{900, 928},
		{1024, 1024}, // exact power of 2 is unchanged
		{1025, 1088}, // just past a power of 2 rounds up
		{1_000_000, 1_015_808},
	}
	assert := NewAssert(t)
	for _, c := range cases {
		assert.Equal(c.want, Padme(c.l), "Padme(%d)", c.l)
	}
}

func BenchmarkWriteBlock(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"4KiB", 4 * 1024},
		{"256KiB", 256 * 1024},
		{"max", MaxBlockDataSize},
	}
	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			r := td.NewTestRepository(b, td.NewFS(b)).Repository
			data := make([]byte, s.size)
			_, err := rand.Read(data)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for b.Loop() {
				// Make sure we don't write the same block twice.
				_, err = rand.Read(data[:100])
				if err != nil {
					b.Fatal(err)
				}
				_, bytesWritten, _ := r.WriteBlock(data)
				if bytesWritten == nil {
					b.Fatal("block already existed")
				}
			}
		})
	}
}

func BenchmarkReadBlock(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"4KiB", 4 * 1024},
		{"256KiB", 256 * 1024},
		{"max", MaxBlockDataSize},
	}
	for _, s := range sizes {
		b.Run(s.name, func(b *testing.B) {
			r := td.NewTestRepository(b, td.NewFS(b)).Repository
			data := make([]byte, s.size)
			_, err := rand.Read(data)
			if err != nil {
				b.Fatal(err)
			}
			blockId, _, err := r.WriteBlock(data)
			if err != nil {
				b.Fatal(err)
			}
			buf := NewBlockBuf()
			b.ResetTimer()
			for b.Loop() {
				_, err := r.ReadBlock(blockId, buf)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
