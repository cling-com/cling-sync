package lib

import (
	"context"
	"crypto/cipher"
	"encoding/hex"
	"errors"
	"fmt"
	"math/bits"
	"strings"
)

const (
	// MaxBlockDataSize is the largest plaintext payload a block can carry.
	// It is chosen so that Padmé padding is a no-op at the maximum (i.e.
	// `Padme(MaxBlockDataSize) == MaxBlockDataSize`) and leaves enough room
	// for the encrypted header and the protobuf envelope to fit within
	// `MaxBlockSize`.
	MaxBlockDataSize           = MaxBlockSize - 128*1024
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
	EncryptionVersion uint16 = 1
	StorageVersion    uint16 = 1
)

var (
	ErrRootRevision = errors.New("root revision cannot be read")
	ErrHeadChanged  = Errorf("head changed during commit")
)

type masterKeyInfo struct {
	EncryptionVersion       uint16
	EncryptedKEK            EncryptedKey
	Argon2id                Argon2id
	EncryptedBlockIdHmacKey EncryptedKey
	EncryptedGearCDCSeed    EncryptedKey
}

type repositoryKeys struct {
	KEK            RawKey
	BlockIdHmacKey RawKey
	GearCDCSeed    RawKey
}

//nolint:gochecknoglobals
var (
	aadKEK            = []byte("cling-sync/kek")
	aadBlockIdHmacKey = []byte("cling-sync/blockid-hmac-key")
	aadGearCDCSeed    = []byte("cling-sync/gearcdc-seed")
)

func masterKeyAAD(salt Salt, label []byte) []byte {
	aad := make([]byte, 0, len(salt)+len(label))
	aad = append(aad, salt[:]...)
	aad = append(aad, label...)
	return aad
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
	encryptedKEK, err = Encrypt(kek[:], cipher, masterKeyAAD(userKeySalt, aadKEK), encryptedKEK)
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
	encryptedBlockIdHmacKey, err = Encrypt(
		blockIdHmacKey[:],
		cipher,
		masterKeyAAD(userKeySalt, aadBlockIdHmacKey),
		encryptedBlockIdHmacKey,
	)
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
	encryptedGearCDCSeed, err = Encrypt(
		gearCDCSeed[:],
		cipher,
		masterKeyAAD(userKeySalt, aadGearCDCSeed),
		encryptedGearCDCSeed,
	)
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
	mki := masterKeyInfo{
		EncryptionVersion,
		EncryptedKey(encryptedKEK),
		argon2id,
		EncryptedKey(encryptedBlockIdHmacKey),
		EncryptedKey(encryptedGearCDCSeed),
	}
	toml, headerComment := createRepositoryConfig(mki)
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
	keys, err := decryptrepositoryKeys(storage, passphrase)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt repository keys")
	}
	kekCipher, err := NewCipher(keys.KEK)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from KEK")
	}
	gearCDCTable, err := NewGearCDCTable(keys.GearCDCSeed)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create GearCDCTable")
	}
	return &Repository{storage, kekCipher, keys.BlockIdHmacKey, gearCDCTable}, nil
}

// Read the encrypted keys from the storage config (`repository.toml`) and decrypt them.
func decryptrepositoryKeys(storage Storage, passphrase []byte) (*repositoryKeys, error) {
	toml, err := storage.Open()
	if err != nil {
		return nil, WrapErrorf(err, "failed to open storage")
	}
	mki, err := parseRepositoryConfig(toml)
	if err != nil {
		return nil, WrapErrorf(err, "failed to parse repository config")
	}
	if mki.EncryptionVersion != EncryptionVersion {
		return nil, Errorf(
			"unsupported repository version %d, want %d",
			mki.EncryptionVersion,
			EncryptionVersion,
		)
	}
	userKey, err := DeriveUserKey(passphrase, mki.Argon2id)
	if err != nil {
		return nil, WrapErrorf(err, "failed to derive user-key from passphrase")
	}
	cipher, err := NewCipher(userKey)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from user-key")
	}
	kek := make([]byte, RawKeySize)
	kek, err = Decrypt(mki.EncryptedKEK[:], cipher, masterKeyAAD(mki.Argon2id.Salt, aadKEK), kek)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt KEK with user-key")
	}
	blockIdHmacKey := make([]byte, RawKeySize)
	blockIdHmacKey, err = Decrypt(
		mki.EncryptedBlockIdHmacKey[:],
		cipher,
		masterKeyAAD(mki.Argon2id.Salt, aadBlockIdHmacKey),
		blockIdHmacKey,
	)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt block id HMAC key with user-key")
	}
	gearCDCSeed := make([]byte, RawKeySize)
	gearCDCSeed, err = Decrypt(
		mki.EncryptedGearCDCSeed[:],
		cipher,
		masterKeyAAD(mki.Argon2id.Salt, aadGearCDCSeed),
		gearCDCSeed,
	)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt gear-cdc seed with user-key")
	}
	return &repositoryKeys{
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
	compression := CompressionNone
	if IsCompressible(data) {
		compressed, err := Compress(data)
		if err != nil {
			return blockId, nil, WrapErrorf(err, "failed to compress data of block %s", blockId)
		}
		compressionRatio := float64(len(compressed)) / float64(len(data))
		if compressionRatio < 0.95 {
			compression = CompressionDeflate
			data = compressed
		}
	}

	// Add padding.
	encryptedDataSize := len(data)
	paddedSize := min(uint64(MaxBlockDataSize), Padme(uint64(encryptedDataSize)))
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
	encryptedData := make([]byte, len(data)+TotalCipherOverhead)
	if _, err := Encrypt(data, dekCypher, blockId[:], encryptedData); err != nil {
		return blockId, nil, WrapErrorf(err, "failed to encrypt data with DEK for block %s", blockId)
	}

	// Marshal and encrypt the header.
	header := BlockHeader{
		Version:           uint32(StorageVersion),
		Compression:       compression,
		Dek:               dek,
		EncryptedDataSize: uint32(encryptedDataSize), //nolint:gosec
	}
	headerBuf := make([]byte, header.MarshallSize())
	headerWriter := NewProtobufWriter(headerBuf)
	if err := header.Marshall(headerWriter); err != nil {
		return blockId, nil, WrapErrorf(err, "failed to marshal block header %s", blockId)
	}
	encryptedHeader := make([]byte, len(headerWriter.Bytes())+TotalCipherOverhead)
	if _, err := Encrypt(headerWriter.Bytes(), r.kekCipher, blockId[:], encryptedHeader); err != nil {
		return blockId, nil, WrapErrorf(err, "failed to encrypt block header with KEK for block %s", blockId)
	}

	// Marshal the Block envelope.
	block := Block{EncryptedHeader: encryptedHeader, EncryptedData: encryptedData}
	blockBuf := make([]byte, block.MarshallSize())
	blockWriter := NewProtobufWriter(blockBuf)
	if err := block.Marshall(blockWriter); err != nil {
		return blockId, nil, WrapErrorf(err, "failed to marshal block envelope for %s", blockId)
	}

	// Write block.
	exists, err := r.storage.WriteBlock(blockId, blockWriter.Bytes())
	if err != nil {
		return blockId, nil, WrapErrorf(err, "failed to write block %s", blockId)
	}
	if exists {
		return blockId, nil, nil
	}
	return blockId, &encryptedDataSize, nil
}

func (r *Repository) ReadBlock(blockId BlockId, buf BlockBuf) ([]byte, error) {
	rawBlock, err := r.storage.ReadBlock(blockId, buf)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read block %s", blockId)
	}
	block, err := UnmarshallBlock(NewProtobufReader(rawBlock))
	if err != nil {
		return nil, WrapErrorf(err, "failed to unmarshal block envelope for %s", blockId)
	}
	rawHeader, err := DecryptInPlace(block.EncryptedHeader, r.kekCipher, blockId[:])
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt block header with KEK for block %s", blockId)
	}
	header, err := UnmarshallBlockHeader(NewProtobufReader(rawHeader))
	if err != nil {
		return nil, WrapErrorf(err, "failed to unmarshal block header for block %s", blockId)
	}
	if header.Version != uint32(StorageVersion) {
		return nil, Errorf("unsupported block version %d for block %s", header.Version, blockId)
	}
	dekCypher, err := NewCipher(header.Dek)
	if err != nil {
		return nil, WrapErrorf(
			err,
			"failed to create a XChaCha20Poly1305 cipher from DEK for block %s",
			blockId,
		)
	}
	data, err := DecryptInPlace(block.EncryptedData, dekCypher, blockId[:])
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt data with DEK for block %s", blockId)
	}
	data = data[:header.EncryptedDataSize]
	if header.Compression == CompressionDeflate {
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

// RevisionMagic is the constant string stored as the first field of every
// marshalled `Revision`. It lets a disaster-recovery tool tell revision
// blocks apart from data blocks by decrypting each block and reading the
// first field as a string.
const RevisionMagic = "cling-revision"

// Return `ErrRootRevision` if revisionId is the root revisionId.
func (r *Repository) ReadRevision(revisionId RevisionId, buf BlockBuf) (Revision, error) {
	if revisionId.IsRoot() {
		return Revision{}, ErrRootRevision
	}
	data, err := r.ReadBlock(BlockId(revisionId), buf)
	if err != nil {
		return Revision{}, WrapErrorf(err, "failed to read revision %s", revisionId)
	}
	rev, err := UnmarshallRevision(NewProtobufReader(data))
	if err != nil {
		return Revision{}, WrapErrorf(err, "failed to unmarshal revision %s", revisionId)
	}
	if rev.Magic != RevisionMagic {
		return Revision{}, Errorf(
			"block %s is not a revision (magic %q, want %q)",
			revisionId,
			rev.Magic,
			RevisionMagic,
		)
	}
	return *rev, nil
}

// Write a revision and set it as the current HEAD.
// A revision can only reference the current head as their parent.
// Return `ErrHeadChanged` if the head has changed during the commit.
func (r *Repository) WriteRevision(revision *Revision) (RevisionId, error) {
	if len(revision.BlockIds) == 0 {
		return RevisionId{}, Errorf("revision is empty")
	}
	for _, blockId := range revision.BlockIds {
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
	if revision.ParentRevisionId != head {
		return RevisionId{}, WrapErrorf(
			ErrHeadChanged,
			"revision parent %s does not match current head %s",
			revision.ParentRevisionId,
			head,
		)
	}
	revision.Magic = RevisionMagic
	revBuf := make([]byte, revision.MarshallSize())
	pw := NewProtobufWriter(revBuf)
	if err := revision.Marshall(pw); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to marshal revision")
	}
	blockId, _, err := r.WriteBlock(pw.Bytes())
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

func parseRepositoryConfig(toml Toml) (*masterKeyInfo, error) {
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
	mki := &masterKeyInfo{ //nolint:exhaustruct
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
	mki.EncryptedKEK = EncryptedKey(c)
	passphraseDerivation, ok := toml.GetValue("encryption", "passphrase-derivation")
	if !ok {
		return nil, Errorf("missing key `encryption.passphrase-derivation`")
	}
	argon2id, err := UnmarshalArgon2idConfig(passphraseDerivation)
	if err != nil {
		return nil, err
	}
	mki.Argon2id = argon2id
	c, err = parseRecoveryCode("encrypted-block-id-hmac", EncryptedKeySize)
	if err != nil {
		return nil, err
	}
	mki.EncryptedBlockIdHmacKey = EncryptedKey(c)
	c, err = parseRecoveryCode("encrypted-gear-cdc-seed", EncryptedKeySize)
	if err != nil {
		return nil, err
	}
	mki.EncryptedGearCDCSeed = EncryptedKey(c)
	return mki, nil
}

func createRepositoryConfig(mki masterKeyInfo) (Toml, string) {
	toml := Toml{
		"encryption": {
			"version":                      fmt.Sprintf("%d", mki.EncryptionVersion),
			"passphrase-derivation":        mki.Argon2id.Marshal(),
			"encrypted-key-encryption-key": FormatRecoveryCode(mki.EncryptedKEK[:]),
			"encrypted-block-id-hmac":      FormatRecoveryCode(mki.EncryptedBlockIdHmacKey[:]),
			"encrypted-gear-cdc-seed":      FormatRecoveryCode(mki.EncryptedGearCDCSeed[:]),
		},
		"storage": {
			"version": fmt.Sprintf("%d", StorageVersion),
		},
	}
	return toml, RepositoryConfigHeaderComment
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
