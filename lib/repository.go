package lib

import (
	"bytes"
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

func (id BlockId) String() string {
	return hex.EncodeToString(id[:])
}

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

var ErrRootRevision = errors.New("root revision cannot be read")

type MasterKeyInfo struct {
	EncryptionVersion       uint16
	EncryptedKEK            EncryptedKey
	UserKeySalt             Salt
	EncryptedBlockIdHmacKey EncryptedKey
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
	if err := storage.Init(masterKeyInfo); err != nil {
		return nil, WrapErrorf(err, "failed to initialize storage")
	}
	rootRevisionId := RevisionId{}
	if !rootRevisionId.IsRoot() {
		return nil, Errorf("root revision ID is not zero")
	}
	if err := writeRef(storage, "head", rootRevisionId); err != nil {
		return nil, WrapErrorf(err, "failed to write head reference")
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
	kekCipher, err := NewCipher(RawKey(kek))
	if err != nil {
		return nil, WrapErrorf(err, "failed to create a XChaCha20Poly1305 cipher from KEK")
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
	return &Repository{storage, kekCipher, RawKey(blockIdHmacKey)}, nil
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
		return false, BlockHeader{}, WrapErrorf(err, "failed to read header of block %s", blockId)
	}
	dek, err := NewRawKey()
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to generate random DEK for block %s", blockId)
	}
	var header BlockHeader
	header.BlockId = blockId
	header.Flags = 0
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
	encryptedData, err := Encrypt(data, dekCypher, nil, buf[:])
	if err != nil {
		return false, BlockHeader{}, WrapErrorf(err, "failed to encrypt data with DEK for block %s", blockId)
	}
	header.DataSize = uint32(len(encryptedData)) //nolint:gosec
	block := Block{Header: header, Data: encryptedData}
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

func (r *Repository) ReadBlock(blockId BlockId, buf BlockBuf) ([]byte, BlockHeader, error) {
	encryptedData, header, err := r.storage.ReadBlock(blockId, buf)
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to read block %s", blockId)
	}
	dek, err := Decrypt(header.EncryptedDEK[:], r.kekCipher, blockId[:], buf[:])
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
	data, err := Decrypt(encryptedData, dekCypher, nil, buf[:])
	if err != nil {
		return nil, BlockHeader{}, WrapErrorf(err, "failed to decrypt data with DEK for block %s", blockId)
	}
	return data, header, nil
}

func (r *Repository) Head() (RevisionId, error) {
	ref, err := readRef(r.storage, "head")
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
	data, _, err := r.ReadBlock(BlockId(revisionId), buf)
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
func (r *Repository) WriteRevision(revision *Revision, buf BlockBuf) (RevisionId, error) {
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
		return RevisionId{}, Errorf("revision parent %s does not match current head %s", revision.Parent, head)
	}
	revBuf := bytes.NewBuffer([]byte{})
	if err := MarshalRevision(revision, revBuf); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to marshal revision")
	}
	_, blockHeader, err := r.WriteBlock(revBuf.Bytes(), buf)
	if err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write revision block")
	}
	revisionId := RevisionId(blockHeader.BlockId)
	if err := writeRef(r.storage, "head", revisionId); err != nil {
		return RevisionId{}, WrapErrorf(err, "failed to write head reference")
	}
	return revisionId, nil
}

func writeRef(storage Storage, name string, revisionId RevisionId) error {
	if err := storage.WriteControlFile(ControlFileSectionRefs, name, []byte(hex.EncodeToString(revisionId[:]))); err != nil {
		return WrapErrorf(err, "failed to write reference %s", name)
	}
	return nil
}

func readRef(storage Storage, name string) (RevisionId, error) {
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
