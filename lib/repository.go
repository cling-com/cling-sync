package lib

import (
	"bytes"
	"context"
	"crypto/cipher"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"slices"
	"strings"
)

const (
	// Unencrypted header size.
	RawBlockHeaderSize = 46
	// Encrypted and padded header size.
	BlockHeaderSize            = 86
	MaxEncryptedBlockDataSize  = MaxBlockSize - BlockHeaderSize
	MaxBlockDataSize           = MaxEncryptedBlockDataSize - TotalCipherOverhead
	UpdateHeadRevisionLockName = "head"
)

//nolint:gochecknoglobals
var RepositoryConfigHeaderComment = strings.Trim(`
DO NOT DELETE OR MODIFY THIS FILE.

This file contains the configuration of your cling repository including
the master key information.
You need your passphrase to unlock the repository so this file in itself
is not enough to access your data. But without this file all your data is
lost. Forever.

So please back this file up. 
Copy it to a secure place (a password manager might be a good choice) or 
even print it out and keep it somewhere safe.
`, "\n ")

const (
	BlockFlagDeflate uint64 = 1
)

type BlockHeader struct {
	Flags             uint64 // See `BlockFlag` constants.
	DEK               RawKey
	EncryptedDataSize uint32
}

type RawBlockHeader [RawBlockHeaderSize]byte

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
	Argon2id                Argon2id
	EncryptedBlockIdHmacKey EncryptedKey
	EncryptedGearCDCSeed    EncryptedKey
}

type RepositoryKeys struct {
	KEK            RawKey
	BlockIdHmacKey RawKey
	GearCDCSeed    RawKey
}

type Repository struct {
	storage        Storage
	kekCipher      cipher.AEAD
	blockIdHmacKey RawKey
	gearCDCTable   GearCDCTable
}

func InitNewRepository(storage Storage, passphrase []byte) (*Repository, error) { //nolint:funlen
	userKeySalt, err := NewSalt()
	if err != nil {
		return nil, WrapErrorf(err, "failed to generate random user key salt")
	}
	argon2id := NewArgon2id(userKeySalt)
	userKey, err := DeriveUserKey(passphrase, argon2id)
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
	gearCDCSeed, err := NewRawKey()
	if err != nil {
		return nil, WrapErrorf(err, "failed to generate random GearCDC seed")
	}
	encryptedGearCDCSeed := make([]byte, EncryptedKeySize)
	encryptedGearCDCSeed, err = Encrypt(gearCDCSeed[:], cipher, userKeySalt[:], encryptedGearCDCSeed)
	if err != nil {
		return nil, WrapErrorf(err, "failed to encrypt GearCDC seed with user-key")
	}
	if len(encryptedGearCDCSeed) != EncryptedKeySize {
		return nil, Errorf(
			"encrypted GearCDC seed has wrong size, want %d, got %d",
			EncryptedKeySize,
			len(encryptedGearCDCSeed),
		)
	}
	masterKeyInfo := MasterKeyInfo{
		EncryptionVersion,
		EncryptedKey(encryptedKEK),
		argon2id,
		EncryptedKey(encryptedBlockIdHmacKey),
		EncryptedKey(encryptedGearCDCSeed),
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
	var gearCDCTable GearCDCTable
	if !slices.ContainsFunc(keys.GearCDCSeed[:], func(x byte) bool { return x != 0 }) {
		// All zero GearCDCSeed means we have to fall back to the constant seed
		// we used back in the early days.
		gearCDCTable = version1GearCDCTable
	} else {
		gearCDCTable, err = NewGearCDCTable(keys.GearCDCSeed)
		if err != nil {
			return nil, WrapErrorf(err, "failed to create GearCDCTable")
		}
	}
	return &Repository{storage, kekCipher, keys.BlockIdHmacKey, gearCDCTable}, nil
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
	userKey, err := DeriveUserKey(passphrase, masterKeyInfo.Argon2id)
	if err != nil {
		return nil, WrapErrorf(err, "failed to derive user-key from passphrase")
	}
	cipher, err := NewCipher(userKey)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from user-key")
	}
	kek := make([]byte, RawKeySize)
	kek, err = Decrypt(masterKeyInfo.EncryptedKEK[:], cipher, masterKeyInfo.Argon2id.Salt[:], kek)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt KEK with user-key")
	}
	blockIdHmacKey := make([]byte, RawKeySize)
	blockIdHmacKey, err = Decrypt(
		masterKeyInfo.EncryptedBlockIdHmacKey[:],
		cipher,
		masterKeyInfo.Argon2id.Salt[:],
		blockIdHmacKey,
	)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt block id HMAC key with user-key")
	}
	gearCDCSeed := make([]byte, RawKeySize)
	gearCDCSeed, err = Decrypt(
		masterKeyInfo.EncryptedGearCDCSeed[:],
		cipher,
		masterKeyInfo.Argon2id.Salt[:],
		gearCDCSeed,
	)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt gear-cdc seed with user-key")
	}
	return &RepositoryKeys{
		KEK:            RawKey(kek),
		BlockIdHmacKey: RawKey(blockIdHmacKey),
		GearCDCSeed:    RawKey(gearCDCSeed),
	}, nil
}

func (r *Repository) GearCDCTable() GearCDCTable {
	return r.gearCDCTable
}

// If `dataBytesWritten` is nil then the block already existed. Otherwise it returns the
// size of `data` after being compressed (if applicable).
// Padding is added to the block (using Padmé: https://lbarman.ch/blog/padme) to obfuscate
// its size.
//
//nolint:funlen
func (r *Repository) WriteBlock(data []byte) (blockId BlockId, dataBytesWritten *int, err error) {
	if len(data) > MaxBlockDataSize {
		return BlockId{}, nil, Errorf("data size %d exceeds maximum block size %d", len(data), MaxBlockDataSize)
	}
	blockId = BlockId(CalculateHmac(data, r.blockIdHmacKey))
	ok, err := r.storage.HasBlock(blockId)
	if ok {
		return blockId, nil, nil
	}
	if err != nil {
		return blockId, nil, WrapErrorf(err, "failed to read header of block %s", blockId)
	}

	// Compress data if possible.
	var header BlockHeader
	if IsCompressible(data) {
		compressed, err := Compress(data)
		if err != nil {
			return blockId, nil, WrapErrorf(err, "failed to compress data of block %s", blockId)
		}
		compressionRatio := float64(len(compressed)) / float64(len(data))
		if compressionRatio < 0.95 {
			header.Flags |= BlockFlagDeflate
			data = compressed
		}
	}

	// Add padding.
	encryptedDataSize := len(data)
	paddedSize := min(MaxBlockDataSize, Padme(uint64(encryptedDataSize)))
	data = append(data, make([]byte, paddedSize-uint64(encryptedDataSize))...)

	// Encrypt data.
	dek, err := NewRawKey()
	if err != nil {
		return blockId, nil, WrapErrorf(err, "failed to generate random DEK for block %s", blockId)
	}
	dekCypher, err := NewCipher(dek)
	if err != nil {
		return blockId, nil, WrapErrorf(
			err,
			"failed to create a XChaCha20Poly1305 cipher from DEK for block %s",
			blockId,
		)
	}
	blockData := make([]byte, len(data)+TotalCipherOverhead+BlockHeaderSize)
	_, err = Encrypt(data, dekCypher, nil, blockData[BlockHeaderSize:])
	if err != nil {
		return blockId, nil, WrapErrorf(err, "failed to encrypt data with DEK for block %s", blockId)
	}

	// Encrypt header.
	header.DEK = dek
	header.EncryptedDataSize = uint32(encryptedDataSize) //nolint:gosec
	headerData := RawBlockHeader{}
	if err := MarshalBlockHeader(&header, &headerData); err != nil {
		return blockId, nil, WrapErrorf(err, "failed to marshal block header %s", blockId)
	}
	_, err = Encrypt(headerData[:], r.kekCipher, blockId[:], blockData[:BlockHeaderSize])
	if err != nil {
		return blockId, nil, WrapErrorf(err, "failed to encrypt block header with KEK for block %s", blockId)
	}

	// Write block.
	exists, err := r.storage.WriteBlock(blockId, blockData)
	if err != nil {
		return blockId, nil, WrapErrorf(err, "failed to write block %s", blockId)
	}
	if exists {
		return blockId, nil, nil
	}
	return blockId, &encryptedDataSize, nil
}

func (r *Repository) ReadBlock(blockId BlockId, buf BlockBuf) ([]byte, error) {
	encryptedBlock, err := r.storage.ReadBlock(blockId, buf)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read block %s", blockId)
	}
	rawHeader := encryptedBlock[:BlockHeaderSize]
	rawHeader, err = DecryptInPlace(rawHeader, r.kekCipher, blockId[:])
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt block header with KEK for block %s", blockId)
	}
	header, err := UnmarshalBlockHeader(RawBlockHeader(rawHeader))
	if err != nil {
		return nil, WrapErrorf(err, "failed to unmarshal block header for block %s", blockId)
	}
	dekCypher, err := NewCipher(header.DEK)
	if err != nil {
		return nil, WrapErrorf(
			err,
			"failed to create a XChaCha20Poly1305 cipher from DEK for block %s",
			blockId,
		)
	}
	data, err := DecryptInPlace(encryptedBlock[BlockHeaderSize:], dekCypher, nil)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt data with DEK for block %s", blockId)
	}
	data = data[:header.EncryptedDataSize]
	if header.Flags&BlockFlagDeflate != 0 {
		data, err = Decompress(data)
		if err != nil {
			return nil, WrapErrorf(err, "failed to decompress data")
		}
	}
	return data, nil
}

func (r *Repository) Head() (RevisionId, error) {
	ref, err := ReadRef(r.storage, "head")
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to read head reference")
	}
	return ref, nil
}

// Return `ErrRootRevision` if revisionId is the root revisionId.
func (r *Repository) ReadRevision(revisionId RevisionId, buf BlockBuf) (Revision, error) {
	if revisionId.IsRoot() {
		return Revision{}, ErrRootRevision
	}
	data, err := r.ReadBlock(BlockId(revisionId), buf)
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
	unlock, err := r.storage.Lock(context.Background(), UpdateHeadRevisionLockName)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to create lock")
	}
	defer unlock() //nolint:errcheck
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
	blockId, _, err := r.WriteBlock(revBuf.Bytes())
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write revision block")
	}
	revisionId := RevisionId(blockId)
	if err := WriteRef(r.storage, "head", revisionId); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write head reference")
	}
	return revisionId, nil
}

func WriteRef(storage Storage, name string, revisionId RevisionId) error {
	if err := storage.WriteControlFile(
		ControlFileSectionRefs,
		name,
		[]byte(hex.EncodeToString(revisionId[:])),
	); err != nil {
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

func MarshalBlockHeader(header *BlockHeader, target *RawBlockHeader) error {
	bw := NewBinaryWriter(NewFixedBufWriter(target[:]))
	bw.Write(StorageVersion)
	bw.Write(header.Flags)
	bw.Write(header.DEK)
	bw.Write(header.EncryptedDataSize)
	return bw.Err
}

func UnmarshalBlockHeader(data RawBlockHeader) (BlockHeader, error) {
	br := NewBinaryReader(bytes.NewBuffer(data[:]))
	var version uint16
	br.Read(&version)
	if br.Err == nil && version != StorageVersion {
		return BlockHeader{}, Errorf("unsupported block version: %d", version)
	}
	var header BlockHeader
	br.Read(&header.Flags)
	br.Read(&header.DEK)
	br.Read(&header.EncryptedDataSize)
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
	parseRecoveryCode := func(key string, expectedLen int) ([]byte, error) {
		section := "encryption"
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
	c, err := parseRecoveryCode("encrypted-key-encryption-key", EncryptedKeySize)
	if err != nil {
		return nil, err
	}
	masterKeyInfo.EncryptedKEK = EncryptedKey(c)
	passphraseDerivation, ok := toml.GetValue("encryption", "passphrase-derivation")
	if !ok {
		return nil, Errorf("missing key `encryption.passphrase-derivation`")
	}
	argon2id, err := UnmarshalArgon2idConfig(passphraseDerivation)
	if err != nil {
		return nil, err
	}
	masterKeyInfo.Argon2id = argon2id
	c, err = parseRecoveryCode("encrypted-block-id-hmac", EncryptedKeySize)
	if err != nil {
		return nil, err
	}
	masterKeyInfo.EncryptedBlockIdHmacKey = EncryptedKey(c)
	c, err = parseRecoveryCode("encrypted-gear-cdc-seed", EncryptedKeySize)
	if err != nil {
		return nil, err
	}
	masterKeyInfo.EncryptedGearCDCSeed = EncryptedKey(c)
	return masterKeyInfo, nil
}

func createRepositoryConfig(masterKeyInfo MasterKeyInfo) (Toml, string) {
	toml := Toml{
		"encryption": {
			"version":                      fmt.Sprintf("%d", masterKeyInfo.EncryptionVersion),
			"passphrase-derivation":        masterKeyInfo.Argon2id.Marshal(),
			"encrypted-key-encryption-key": FormatRecoveryCode(masterKeyInfo.EncryptedKEK[:]),
			"encrypted-block-id-hmac":      FormatRecoveryCode(masterKeyInfo.EncryptedBlockIdHmacKey[:]),
			"encrypted-gear-cdc-seed":      FormatRecoveryCode(masterKeyInfo.EncryptedGearCDCSeed[:]),
		},
		"storage": {
			"version": fmt.Sprintf("%d", StorageVersion),
		},
	}
	return toml, RepositoryConfigHeaderComment
}

func MarshalRepositoryKeys(keys *RepositoryKeys, w io.Writer) error {
	bw := NewBinaryWriter(w)
	bw.Write(EncryptionVersion)
	bw.Write(keys.KEK[:])
	bw.Write(keys.BlockIdHmacKey[:])
	bw.Write(keys.GearCDCSeed[:])
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
		// todo: Return a fixed error we can react to.
		return nil, Errorf("unsupported repository keys version %d, want %d", version, EncryptionVersion)
	}
	var kek RawKey
	br.Read(&kek)
	var blockIdHmacKey RawKey
	br.Read(&blockIdHmacKey)
	var gearCDCSeed RawKey
	br.Read(&gearCDCSeed)
	return &RepositoryKeys{
		KEK:            kek,
		BlockIdHmacKey: blockIdHmacKey,
		GearCDCSeed:    gearCDCSeed,
	}, nil
}

// Return the number of bytes to pad the given input size
// according to: https://lbarman.ch/blog/padme
func Padme(l uint64) uint64 {
	if l < 2 {
		return l
	}
	e := bits.Len64(l) - 1
	s := bits.Len(uint(e)) //nolint:gosec
	lastBits := e - s
	bitMask := (uint64(1) << lastBits) - 1
	return (l + bitMask) &^ bitMask
}
