package lib

import (
	cryptoCipher "crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"

	"golang.org/x/crypto/argon2"
	cha "golang.org/x/crypto/chacha20poly1305"
)

const (
	RawKeySize          = 32
	SaltSize            = RawKeySize
	nonceSize           = 24
	TotalCipherOverhead = nonceSize + 16
	EncryptedKeySize    = RawKeySize + TotalCipherOverhead
)

// This is the key derived from the user's passphrase that is used to encrypt the KEK.
type UserKey RawKey

type EncryptedKey [EncryptedKeySize]byte

type Salt [32]byte

type Sha256 [32]byte

type Sha256Hmac Sha256

type RawKey [RawKeySize]byte

func NewRawKey() (RawKey, error) {
	key := make([]byte, RawKeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return RawKey{}, WrapErrorf(err, "failed to generate random key")
	}
	return RawKey(key), nil
}

func RandStr(n int) (string, error) {
	b := make([]byte, n/2+1)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", WrapErrorf(err, "failed to generate random string")
	}
	return hex.EncodeToString(b)[:n], nil
}

func NewSalt() (Salt, error) {
	key := make([]byte, SaltSize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return Salt{}, WrapErrorf(err, "failed to generate random key")
	}
	return Salt(key), nil
}

// Derive the user's UserKey from the given passphrase using Argon2id.
func DeriveUserKey(passphrase []byte, salt Salt) (RawKey, error) {
	// Exceeding OWASP recommendations of 12MB RAM, 3 iterations, 1 thread.
	key := argon2.IDKey(passphrase, salt[:], 5, 65535, 1, RawKeySize)
	return RawKey(key), nil
}

// Create an XChaChaPoly1305 cipher from the given raw key.
func NewCipher(key RawKey) (cryptoCipher.AEAD, error) {
	cipher, err := cha.NewX(key[:])
	if err != nil {
		return nil, WrapErrorf(err, "failed to create XChaChaPoly1305 cipher")
	}
	if cipher.NonceSize() != nonceSize {
		return nil, Errorf("invalid cipher nonce size: want %d, got %d", nonceSize, cipher.NonceSize())
	}
	if cipher.Overhead() != TotalCipherOverhead-nonceSize {
		return nil, Errorf(
			"invalid cipher overhead size: want %d, got %d",
			TotalCipherOverhead-nonceSize,
			cipher.Overhead(),
		)
	}
	return cipher, nil
}

// dst - must be large enough to hold the ciphertext, nonce, and cipher overhead
//
//	('len(plaintext) + TotalCipherOverhead')
func Encrypt(plaintext []byte, cipher cryptoCipher.AEAD, associatedData []byte, dst []byte) ([]byte, error) {
	targetSize := len(plaintext) + TotalCipherOverhead
	if len(dst) < targetSize {
		return nil, Errorf("target buffer too small, want %d, got %d", targetSize, len(dst))
	}
	nonce := dst[0:nonceSize]
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, WrapErrorf(err, "failed to read random bytes for nonce")
	}
	ciphertext := cipher.Seal(dst[:nonceSize], nonce, plaintext, associatedData)
	if len(ciphertext) < nonceSize {
		return nil, Errorf("ciphertext is too short")
	}
	return dst[:targetSize], nil
}

// dst - must be large enough to hold the plaintext (`len(ciphertext) - TotalCipherOverhead`).
func Decrypt(ciphertext []byte, cipher cryptoCipher.AEAD, associatedData []byte, dst []byte) ([]byte, error) {
	if len(ciphertext) < nonceSize {
		return nil, Errorf("payload too short")
	}
	nonce, encryptedData := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintextSize := len(encryptedData) - TotalCipherOverhead
	if len(dst) < plaintextSize {
		return nil, Errorf("target buffer too small, want %d, got %d", plaintextSize, len(dst))
	}
	plaintext, err := cipher.Open(dst[:0], nonce, encryptedData, associatedData)
	if err != nil {
		return nil, WrapErrorf(err, "failed to decrypt data")
	}
	return plaintext, nil
}

// Re-uses the ciphertext buffer.
func DecryptInPlace(ciphertext []byte, cipher cryptoCipher.AEAD, associatedData []byte) ([]byte, error) {
	return Decrypt(ciphertext, cipher, associatedData, ciphertext[nonceSize:])
}

func CalculateSha256(data []byte) Sha256 {
	sha := sha256.New()
	sha.Write(data)
	return Sha256(sha.Sum(nil))
}

func CalculateHmac(data []byte, key RawKey) Sha256Hmac {
	sha := hmac.New(sha256.New, key[:])
	sha.Write(data)
	return Sha256Hmac(sha.Sum(nil))
}

func CheckPassphraseStrength(phrase []byte) error {
	// OWASP recommendations:
	// https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
	if len(phrase) < 12 {
		return Errorf("passphrase must be at least 12 characters long")
	}
	if len(phrase) > 256 {
		return Errorf("passphrase must be at most 256 characters long")
	}
	// todo: Implement more checks?
	return nil
}
