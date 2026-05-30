package lib

import (
	cryptoCipher "crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

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

// Rand returns n cryptographically random bytes from the system CSPRNG.
func Rand(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return nil, WrapErrorf(err, "failed to read %d random bytes", n)
	}
	return b, nil
}

func NewRawKey() (RawKey, error) {
	var key RawKey
	if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
		return RawKey{}, WrapErrorf(err, "failed to generate random key")
	}
	return key, nil
}

// RandStr returns a string of n hex characters of entropy (n/2 random bytes
// hex-encoded).
func RandStr(n int) (string, error) {
	b, err := Rand(n/2 + 1)
	if err != nil {
		return "", err
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
func DeriveUserKey(passphrase []byte, argon2id Argon2id) (RawKey, error) {
	key := argon2.IDKey(passphrase, argon2id.Salt[:], argon2id.Time, argon2id.Memory, argon2id.Parallelism, RawKeySize)
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
	plaintextSize := len(ciphertext) - TotalCipherOverhead
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

type Argon2id struct {
	Time        uint32
	Memory      uint32
	Parallelism uint8
	Salt        Salt
}

// todo: measure on a phone or raspberry.
// Create a default Argon2id config with time=4, memory=128MiB, parallelism=2.
func NewArgon2id(salt Salt) Argon2id {
	return Argon2id{Time: 4, Memory: 128 * 1024, Parallelism: 2, Salt: salt}
}

func (a Argon2id) Marshal() string {
	return fmt.Sprintf(
		"$argon2id$v=19$m=%d,t=%d,p=%d$%s",
		a.Memory,
		a.Time,
		a.Parallelism,
		base64.RawStdEncoding.EncodeToString(a.Salt[:]),
	)
}

// Parse the PHC password format but we expect a strict format like this:
//
// $argon2id$v=19$m=<memory>,t=<time>,p=<parallelism>$<salt>
//
// Needs to at least meet OWASP recommendations of 12MB RAM, 3 iterations, 1 thread.
//
// PHC format: https://github.com/P-H-C/phc-string-format/blob/master/phc-sf-spec.md
func UnmarshalArgon2idConfig(s string) (Argon2id, error) {
	parts := strings.Split(s, "$")
	if len(parts) != 5 {
		return Argon2id{}, Errorf("expecting 4 parts")
	}
	if parts[0] != "" || parts[1] != "argon2id" || parts[2] != "v=19" {
		return Argon2id{}, Errorf("expecting argon2id, version 19")
	}
	params := strings.Split(parts[3], ",")
	if len(params) != 3 {
		return Argon2id{}, Errorf("expecting 3 parameters")
	}
	parseParam := func(s string, param string) (uint32, error) {
		s, ok := strings.CutPrefix(s, param+"=")
		if !ok {
			return 0, Errorf("expected parameter %s", param)
		}
		i, err := strconv.Atoi(s)
		// `int64(i)` so the bound does not overflow `int` on 32-bit targets (TinyGo wasm).
		if err != nil || i < 0 || int64(i) >= 1<<32 {
			return 0, Errorf("invalid value for parameter %s", param)
		}
		return uint32(i), nil
	}
	memory, err := parseParam(params[0], "m")
	if err != nil {
		return Argon2id{}, err
	}
	time, err := parseParam(params[1], "t")
	if err != nil {
		return Argon2id{}, err
	}
	parallelism, err := parseParam(params[2], "p")
	if err != nil {
		return Argon2id{}, err
	}
	// We use `RawStdEncoding` because the PHC "standard" says that base64 padding is omitted.
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) != 32 {
		return Argon2id{}, WrapErrorf(err, "invalid salt")
	}
	if parallelism < 1 || parallelism > 255 {
		return Argon2id{}, Errorf("parallelism must be at least 1")
	}
	if memory < 12*1024 {
		return Argon2id{}, Errorf("memory must be at least 12MiB")
	}
	if time < 3 {
		return Argon2id{}, Errorf("time must be at least 3")
	}
	return Argon2id{
		Time:        time,
		Memory:      memory,
		Parallelism: uint8(parallelism),
		Salt:        Salt(salt),
	}, nil
}
