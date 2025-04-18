package lib

import (
	"crypto/cipher"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

const (
	MaxBlockSize     = 4 * 1024 * 1024
	BlockHeaderSize  = 96
	MaxBlockDataSize = MaxBlockSize - BlockHeaderSize - TotalCipherOverhead
)

type BlockBuf [MaxBlockSize]byte

type BlockId Sha256Hmac

const (
	BlockFlagDeflate = 1
)

type RepositoryConfig struct {
	MasterKeyInfo  MasterKeyInfo
	StorageFormat  string
	StorageVersion uint16
}

type BlockHeader struct {
	BlockId      BlockId
	Flags        uint64 // See `BlockFlag` constants.
	EncryptedDEK EncryptedKey
	DataSize     uint32
}

type Block struct {
	Header BlockHeader
	Data   []byte
}

const EncryptionVersion uint16 = 1

type MasterKeyInfo struct {
	EncryptionVersion uint16
	EncryptedKEK      EncryptedKey
	UserKeySalt       Salt
}

type Repository struct {
	storage         Storage
	kekCipher       cipher.AEAD
	metadataHmacKey RawKey
	blockIdHmacKey  RawKey
}

func InitNewRepository(storage Storage, passphrase []byte) (*Repository, error) {
	userKeySalt, err := NewSalt()
	if err != nil {
		return nil, WrapErrorf(err, "failed to generate random user key salt")
	}
	userKey, err := DeriveUserKey(passphrase, userKeySalt)
	if err != nil {
		return nil, WrapErrorf(err, "failed to derive user-key from passphrase")
	}
	cipher, err := NewCipher(userKey)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from user-key")
	}
	kek, err := NewRawKey()
	if err != nil {
		return nil, WrapErrorf(err, "failed to generate random KEK")
	}
	encryptedKEK := make([]byte, EncryptedKeySize)
	encryptedKEK, err = Encrypt(kek[:], cipher, userKeySalt[:], encryptedKEK)
	if err != nil {
		return nil, WrapErrorf(err, "failed to encrypt KEK with user-key")
	}
	if len(encryptedKEK) != EncryptedKeySize {
		return nil, Errorf("encrypted KEK has wrong size, want %d, got %d", EncryptedKeySize, len(encryptedKEK))
	}
	masterKeyInfo := MasterKeyInfo{EncryptionVersion, EncryptedKey(encryptedKEK), userKeySalt}
	if err := storage.Init(masterKeyInfo); err != nil {
		return nil, WrapErrorf(err, "failed to initialize storage")
	}
	return OpenRepository(storage, passphrase)
}

func OpenRepository(storage Storage, passphrase []byte) (*Repository, error) {
	config, err := storage.Open()
	if err != nil {
		return nil, WrapErrorf(err, "failed to open storage")
	}
	masterKeyInfo := config.MasterKeyInfo
	if masterKeyInfo.EncryptionVersion != EncryptionVersion {
		return nil, Errorf("unsupported repository version %d, want %d", masterKeyInfo.EncryptionVersion, EncryptionVersion)
	}
	userKey, err := DeriveUserKey(passphrase, masterKeyInfo.UserKeySalt)
	if err != nil {
		return nil, WrapErrorf(err, "failed to derive user-key from passphrase")
	}
	cipher, err := NewCipher(userKey)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from user-key")
	}
	kek := make([]byte, RawKeySize)
	kek, err = Decrypt(masterKeyInfo.EncryptedKEK[:], cipher, masterKeyInfo.UserKeySalt[:], kek)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt KEK with user-key")
	}
	kekCipher, err := NewCipher(RawKey(kek))
	if err != nil {
		return nil, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from KEK")
	}
	metadataHmacKey := RawKey(CalculateSha256(append(kek, []byte("-metadata")...)))
	blockIdHmacKey := RawKey(CalculateSha256(append(kek, []byte("-blockId")...)))
	return &Repository{storage, kekCipher, metadataHmacKey, blockIdHmacKey}, nil
}

// Return `true` if the block already existed.
func (r *Repository) WriteBlock(data []byte, buf BlockBuf) (bool, BlockHeader, error) {
	if len(data) > MaxBlockDataSize {
		return false, BlockHeader{}, Errorf("data size %d exceeds maximum block size %d", len(data), MaxBlockDataSize)
	}
	blockId := BlockId(CalculateHmac(data, r.blockIdHmacKey))
	readHeader, err := r.storage.ReadBlockHeader(blockId)
	if err == nil {
		return true, readHeader, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, BlockHeader{}, WrapErrorf(err, "failed to read header of block %s", hex.EncodeToString(blockId[:]))
	}
	dek, err := NewRawKey()
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to generate random DEK for block %s", hex.EncodeToString(blockId[:]))
	}
	var header BlockHeader
	header.BlockId = blockId
	header.Flags = 0
	_, err = Encrypt(dek[:], r.kekCipher, blockId[:], header.EncryptedDEK[:])
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to encrypt DEK with KEK for block %s", hex.EncodeToString(blockId[:]))
	}
	dekCypher, err := NewCipher(dek)
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from DEK for block %s", hex.EncodeToString(blockId[:]))
	}
	encryptedData, err := Encrypt(data, dekCypher, nil, buf[:])
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to encrypt data with DEK for block %s", hex.EncodeToString(blockId[:]))
	}
	header.DataSize = uint32(len(encryptedData)) //nolint:gosec
	block := Block{Header: header, Data: encryptedData}
	exists, err := r.storage.WriteBlock(block)
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to write block %s", hex.EncodeToString(blockId[:]))
	}
	if exists {
		header, err := r.storage.ReadBlockHeader(blockId)
		if err != nil {
			return false, BlockHeader{}, WrapErrorf(err, "failed to read header of existing block %s", hex.EncodeToString(blockId[:]))
		}
		return true, header, nil
	}
	return false, block.Header, nil
}

func (r *Repository) ReadBlock(blockId BlockId, buf BlockBuf) ([]byte, BlockHeader, error) {
	encryptedData, header, err := r.storage.ReadBlock(blockId, buf)
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to read block %s", hex.EncodeToString(blockId[:]))
	}
	dek, err := Decrypt(header.EncryptedDEK[:], r.kekCipher, blockId[:], buf[:])
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to decrypt DEK with KEK for block %s", hex.EncodeToString(blockId[:]))
	}
	dekCypher, err := NewCipher(RawKey(dek))
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from DEK for block %s", hex.EncodeToString(blockId[:]))
	}
	data, err := Decrypt(encryptedData, dekCypher, nil, buf[:])
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to decrypt data with DEK for block %s", hex.EncodeToString(blockId[:]))
	}
	return data, header, nil
}

func (r *Repository) WriteFileMetadata(fullPath string, file FileRevision) error {
	return nil
}

func (r *Repository) ReadDir(dir string, dst []string) error {
	return nil
}

func MarshalBlockHeader(header *BlockHeader, w io.Writer) error {
	bw := NewBinaryWriter(w)
	bw.Write(StorageVersion)
	bw.Write(header.Flags)
	bw.Write(header.EncryptedDEK)
	bw.Write(header.DataSize)
	bw.Write([10]byte{}) // padding
	return bw.Err
}

// `BlockHeader.BlockId` is not written in `MarshalBlockHeader` because it is always implied.
// That's why we need to pass the `BlockId` here.
func UnmarshalBlockHeader(blockId BlockId, r io.Reader) (BlockHeader, error) {
	br := NewBinaryReader(r)
	var version uint16
	br.Read(&version)
	if br.Err == nil && version != StorageVersion {
		return BlockHeader{}, Errorf("unsupported block version: %d", version)
	}
	var header BlockHeader
	br.Read(&header.Flags)
	br.Read(&header.EncryptedDEK)
	br.Read(&header.DataSize)
	br.Skip(10) // padding
	header.BlockId = blockId
	return header, br.Err
}
