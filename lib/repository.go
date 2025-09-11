package lib

import (
	"bytes"
	"crypto/cipher"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	MaxBlockSize              = 8 * 1024 * 1024
	BlockHeaderSize           = 96
	MaxEncryptedBlockDataSize = MaxBlockSize - BlockHeaderSize
	MaxBlockDataSize          = MaxEncryptedBlockDataSize - TotalCipherOverhead
)

type BlockId Sha256Hmac

func (id BlockId) String() string {
	return hex.EncodeToString(id[:])
}

const (
	BlockFlagDeflate uint64 = 1
)

type BlockHeader struct {
	BlockId           BlockId
	Flags             uint64 // See `BlockFlag` constants.
	EncryptedDEK      EncryptedKey
	EncryptedDataSize uint32
}

type Block struct {
	Header        BlockHeader
	EncryptedData []byte
}

const (
	EncryptionVersion uint16 = 1
	StorageVersion    uint16 = 1
)

var (
	ErrRootRevision = errors.New("root revision cannot be read")
	ErrHeadChanged  = Errorf("head changed during commit")
)

type MasterKeyInfo struct {
	EncryptionVersion       uint16
	EncryptedKEK            EncryptedKey
	UserKeySalt             Salt
	EncryptedBlockIdHmacKey EncryptedKey
}

type RepositoryKeys struct {
	KEK            RawKey
	BlockIdHmacKey RawKey
}

type Repository struct {
	storage        Storage
	kekCipher      cipher.AEAD
	blockIdHmacKey RawKey
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
	blockIdHmacKey, err := NewRawKey()
	if err != nil {
		return nil, WrapErrorf(err, "failed to generate random block id HMAC key")
	}
	encryptedBlockIdHmacKey := make([]byte, EncryptedKeySize)
	encryptedBlockIdHmacKey, err = Encrypt(blockIdHmacKey[:], cipher, userKeySalt[:], encryptedBlockIdHmacKey)
	if err != nil {
		return nil, WrapErrorf(err, "failed to encrypt block id HMAC key with user-key")
	}
	if len(encryptedBlockIdHmacKey) != EncryptedKeySize {
		return nil, Errorf(
			"encrypted block id HMAC key has wrong size, want %d, got %d",
			EncryptedKeySize,
			len(encryptedBlockIdHmacKey),
		)
	}
	masterKeyInfo := MasterKeyInfo{
		EncryptionVersion,
		EncryptedKey(encryptedKEK),
		userKeySalt,
		EncryptedKey(encryptedBlockIdHmacKey),
	}
	toml, headerComment := createRepositoryConfig(masterKeyInfo)
	if err := storage.Init(toml, headerComment); err != nil {
		return nil, WrapErrorf(err, "failed to initialize storage")
	}
	rootRevisionId := RevisionId{}
	if !rootRevisionId.IsRoot() {
		return nil, Errorf("root revision ID is not zero")
	}
	if err := WriteRef(storage, "head", rootRevisionId); err != nil {
		return nil, WrapErrorf(err, "failed to write head reference")
	}
	return OpenRepository(storage, passphrase)
}

func OpenRepository(storage Storage, passphrase []byte) (*Repository, error) {
	keys, err := DecryptRepositoryKeys(storage, passphrase)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt repository keys")
	}
	return OpenRepositoryWithKeys(storage, keys)
}

func OpenRepositoryWithKeys(storage Storage, keys *RepositoryKeys) (*Repository, error) {
	kekCipher, err := NewCipher(keys.KEK)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from KEK")
	}
	return &Repository{storage, kekCipher, keys.BlockIdHmacKey}, nil
}

// Read the encrypted keys from the storage config (`repository.toml`) and decrypt them.
func DecryptRepositoryKeys(storage Storage, passphrase []byte) (*RepositoryKeys, error) {
	toml, err := storage.Open()
	if err != nil {
		return nil, WrapErrorf(err, "failed to open storage")
	}
	masterKeyInfo, err := parseRepositoryConfig(toml)
	if err != nil {
		return nil, WrapErrorf(err, "failed to parse repository config")
	}
	if masterKeyInfo.EncryptionVersion != EncryptionVersion {
		return nil, Errorf(
			"unsupported repository version %d, want %d",
			masterKeyInfo.EncryptionVersion,
			EncryptionVersion,
		)
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
	blockIdHmacKey := make([]byte, RawKeySize)
	blockIdHmacKey, err = Decrypt(
		masterKeyInfo.EncryptedBlockIdHmacKey[:],
		cipher,
		masterKeyInfo.UserKeySalt[:],
		blockIdHmacKey,
	)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt block id HMAC key with user-key")
	}
	return &RepositoryKeys{
		KEK:            RawKey(kek),
		BlockIdHmacKey: RawKey(blockIdHmacKey),
	}, nil
}

// Return `true` if the block already existed.
func (r *Repository) WriteBlock(data []byte) (bool, BlockHeader, error) {
	if len(data) > MaxBlockDataSize {
		return false, BlockHeader{}, Errorf("data size %d exceeds maximum block size %d", len(data), MaxBlockDataSize)
	}
	blockId := BlockId(CalculateHmac(data, r.blockIdHmacKey))
	readHeader, err := r.storage.ReadBlockHeader(blockId)
	if err == nil {
		return true, readHeader, nil
	} else if !errors.Is(err, ErrBlockNotFound) {
		return false, BlockHeader{}, WrapErrorf(err, "failed to read header of block %s", blockId)
	}
	// Compress data if possible.
	var header BlockHeader
	if IsCompressible(data) {
		compressed, err := Compress(data)
		if err != nil {
			return false, BlockHeader{}, WrapErrorf(err, "failed to compress data")
		}
		compressionRatio := float64(len(compressed)) / float64(len(data))
		// todo: document the compression ratio threshold.
		if compressionRatio < 0.95 {
			// todo: maybe give feedback to the user that compression is skipped.
			header.Flags |= BlockFlagDeflate
			data = compressed
		}
	}
	// Encrypt data.
	dek, err := NewRawKey()
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to generate random DEK for block %s", blockId)
	}
	header.BlockId = blockId
	_, err = Encrypt(dek[:], r.kekCipher, blockId[:], header.EncryptedDEK[:])
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to encrypt DEK with KEK for block %s", blockId)
	}
	dekCypher, err := NewCipher(dek)
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(
			err,
			"failed to create a XChaCha20Poly1305 cipher from DEK for block %s",
			blockId,
		)
	}
	encryptedData := make([]byte, len(data)+TotalCipherOverhead)
	encryptedData, err = Encrypt(data, dekCypher, nil, encryptedData)
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to encrypt data with DEK for block %s", blockId)
	}
	header.EncryptedDataSize = uint32(len(encryptedData)) //nolint:gosec
	block := Block{Header: header, EncryptedData: encryptedData}
	// Write block.
	exists, err := r.storage.WriteBlock(block)
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to write block %s", blockId)
	}
	if exists {
		header, err := r.storage.ReadBlockHeader(blockId)
		if err != nil {
			return false, BlockHeader{}, WrapErrorf(err, "failed to read header of existing block %s", blockId)
		}
		return true, header, nil
	}
	return false, block.Header, nil
}

func (r *Repository) ReadBlock(blockId BlockId) ([]byte, BlockHeader, error) {
	encryptedData, header, err := r.storage.ReadBlock(blockId)
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to read block %s", blockId)
	}
	dek := make([]byte, RawKeySize)
	dek, err = Decrypt(header.EncryptedDEK[:], r.kekCipher, blockId[:], dek)
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to decrypt DEK with KEK for block %s", blockId)
	}
	dekCypher, err := NewCipher(RawKey(dek))
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(
			err,
			"failed to create a XChaCha20Poly1305 cipher from DEK for block %s",
			blockId,
		)
	}
	data, err := DecryptInPlace(encryptedData, dekCypher, nil)
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to decrypt data with DEK for block %s", blockId)
	}
	if header.Flags&BlockFlagDeflate != 0 {
		data, err = Decompress(data)
		if err != nil {
			return nil, BlockHeader{}, WrapErrorf(err, "failed to decompress data")
		}
	}
	return data, header, nil
}

func (r *Repository) Head() (RevisionId, error) {
	ref, err := ReadRef(r.storage, "head")
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to read head reference")
	}
	return ref, nil
}

// Return `ErrRootRevision` if revisionId is the root revisionId.
func (r *Repository) ReadRevision(revisionId RevisionId) (Revision, error) {
	if revisionId.IsRoot() {
		return Revision{}, ErrRootRevision
	}
	data, _, err := r.ReadBlock(BlockId(revisionId))
	if err != nil {
		return Revision{}, WrapErrorf(err, "failed to read revision %s", revisionId)
	}
	rev, err := UnmarshalRevision(bytes.NewReader(data))
	if err != nil {
		return Revision{}, WrapErrorf(err, "failed to unmarshal revision %s", revisionId)
	}
	return *rev, nil
}

// Write a revision and set it as the current HEAD.
// A revision can only reference the current head as their parent.
// Return `ErrHeadChanged` if the head has changed during the commit.
func (r *Repository) WriteRevision(revision *Revision) (RevisionId, error) {
	if len(revision.Blocks) == 0 {
		return RevisionId{}, Errorf("revision is empty")
	}
	for _, blockId := range revision.Blocks {
		exists, err := r.storage.HasBlock(blockId)
		if err != nil {
			return RevisionId{}, WrapErrorf(err, "failed to check if block %s exists", blockId)
		}
		if !exists {
			return RevisionId{}, Errorf("block %s does not exist", blockId)
		}
	}
	// todo: we should really lock the repository here.
	head, err := r.Head()
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to get head revision")
	}
	if revision.Parent != head {
		return RevisionId{}, WrapErrorf(
			ErrHeadChanged,
			"revision parent %s does not match current head %s",
			revision.Parent,
			head,
		)
	}
	revBuf := bytes.NewBuffer([]byte{})
	if err := MarshalRevision(revision, revBuf); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to marshal revision")
	}
	_, blockHeader, err := r.WriteBlock(revBuf.Bytes())
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write revision block")
	}
	revisionId := RevisionId(blockHeader.BlockId)
	if err := WriteRef(r.storage, "head", revisionId); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write head reference")
	}
	return revisionId, nil
}

func WriteRef(storage Storage, name string, revisionId RevisionId) error {
	if err := storage.WriteControlFile(ControlFileSectionRefs, name, []byte(hex.EncodeToString(revisionId[:]))); err != nil {
		return WrapErrorf(err, "failed to write reference %s", name)
	}
	return nil
}

func ReadRef(storage Storage, name string) (RevisionId, error) {
	data, err := storage.ReadControlFile(ControlFileSectionRefs, name)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to read reference %s", name)
	}
	data, err = hex.DecodeString(string(data))
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to decode reference %s", name)
	}
	if len(data) != 32 {
		return RevisionId{}, Errorf("invalid reference size for %s: want %d, got %d", name, 32, len(data))
	}
	return RevisionId(data), nil
}

func MarshalBlockHeader(header *BlockHeader, w io.Writer) error {
	bw := NewBinaryWriter(w)
	bw.Write(StorageVersion)
	bw.Write(header.Flags)
	bw.Write(header.EncryptedDEK)
	bw.Write(header.EncryptedDataSize)
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
	br.Read(&header.EncryptedDataSize)
	br.Skip(10) // padding
	header.BlockId = blockId
	return header, br.Err
}

func parseRepositoryConfig(toml Toml) (*MasterKeyInfo, error) {
	i, ok := toml.GetIntValue("storage", "version")
	if !ok {
		return nil, Errorf("missing or invalid key `storage.version` in repository config")
	}
	if i != int(StorageVersion) {
		return nil, Errorf("unsupported repository version %d, want %d", i, StorageVersion)
	}
	i, ok = toml.GetIntValue("encryption", "version")
	if !ok {
		return nil, Errorf("missing or invalid key `encryption.version` in repository config")
	}
	if i != int(EncryptionVersion) {
		return nil, Errorf("unsupported repository version %d, want %d", i, EncryptionVersion)
	}
	masterKeyInfo := &MasterKeyInfo{ //nolint:exhaustruct
		EncryptionVersion: uint16(i),
	}
	parseRecoveryCode := func(section string, key string, expectedLen int) ([]byte, error) {
		v, ok := toml.GetValue(section, key)
		if !ok {
			return nil, Errorf("missing key `%s.%s` in repository config", section, key)
		}
		c, err := ParseRecoveryCode(v)
		if err != nil {
			return nil, WrapErrorf(err, "invalid key `%s.%s` in repository config", section, key)
		}
		if len(c) != expectedLen {
			return nil, Errorf("invalid key length `%s.%s` in repository config", section, key)
		}
		return c, nil
	}
	c, err := parseRecoveryCode("encryption", "encrypted-kek", EncryptedKeySize)
	if err != nil {
		return nil, err
	}
	masterKeyInfo.EncryptedKEK = EncryptedKey(c)
	c, err = parseRecoveryCode("encryption", "user-key-salt", SaltSize)
	if err != nil {
		return nil, err
	}
	masterKeyInfo.UserKeySalt = Salt(c)
	c, err = parseRecoveryCode("encryption", "encrypted-block-id-hmac", EncryptedKeySize)
	if err != nil {
		return nil, err
	}
	masterKeyInfo.EncryptedBlockIdHmacKey = EncryptedKey(c)
	return masterKeyInfo, nil
}

func createRepositoryConfig(masterKeyInfo MasterKeyInfo) (Toml, string) {
	toml := Toml{
		"encryption": {
			"version":                 fmt.Sprintf("%d", masterKeyInfo.EncryptionVersion),
			"encrypted-kek":           FormatRecoveryCode(masterKeyInfo.EncryptedKEK[:]),
			"user-key-salt":           FormatRecoveryCode(masterKeyInfo.UserKeySalt[:]),
			"encrypted-block-id-hmac": FormatRecoveryCode(masterKeyInfo.EncryptedBlockIdHmacKey[:]),
		},
		"storage": {
			"version": fmt.Sprintf("%d", StorageVersion),
		},
	}
	headerComment := strings.Trim(`
DO NOT DELETE OR CHANGE THIS FILE.

This file contains the configuration of your cling repository including
the master key information.
You need your passphrase to unlock the repository so this file in itself
is not enough to access your data. But without this file all your data is
lost. Forever.

So please back this file up. 
Copy it to a secure place (a password manager might be a good choice) or 
even print it out and keep it somewhere safe.
`, "\n ")
	return toml, headerComment
}

func MarshalRepositoryKeys(keys *RepositoryKeys, w io.Writer) error {
	bw := NewBinaryWriter(w)
	bw.Write(EncryptionVersion)
	bw.Write(keys.KEK[:])
	bw.Write(keys.BlockIdHmacKey[:])
	return bw.Err
}

func UnmarshalRepositoryKeys(r io.Reader) (*RepositoryKeys, error) {
	br := NewBinaryReader(r)
	var version uint16
	br.Read(&version)
	if br.Err != nil {
		return nil, WrapErrorf(br.Err, "failed to parse repository keys")
	}
	if version != EncryptionVersion {
		return nil, Errorf("unsupported repository keys version %d, want %d", version, EncryptionVersion)
	}
	var kek RawKey
	br.Read(&kek)
	var blockIdHmacKey RawKey
	br.Read(&blockIdHmacKey)
	return &RepositoryKeys{
		KEK:            kek,
		BlockIdHmacKey: blockIdHmacKey,
	}, nil
}
