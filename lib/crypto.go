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
	if len(ciphertext) < nonceSize {
		return nil, Errorf("payload too short")
	}
	return Decrypt(ciphertext, cipher, associatedData, ciphertext[nonceSize:])
}

func (s Sha256) String() string {
	return hex.EncodeToString(s[:])
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

// The cost of deriving the user key, without the salt, which is generated when
// the repository is created.
type Argon2idParams struct {
	Time        uint32
	Memory      uint32
	Parallelism uint8
}

func NewArgon2id(salt Salt, params Argon2idParams) Argon2id {
	return Argon2id{Time: params.Time, Memory: params.Memory, Parallelism: params.Parallelism, Salt: salt}
}

// todo: measure on a phone or raspberry.
// The default cost: time=4, memory=128MiB, parallelism=2.
func DefaultArgon2idParams() Argon2idParams {
	return Argon2idParams{Time: 4, Memory: 128 * 1024, Parallelism: 2}
}

// Higher is slower to derive and slower to attack, `memory` is in KiB.
//
// The cost has to meet the OWASP recommendation of 12MiB memory, 3 iterations and
// 1 thread, and has to stay within what a client can realistically derive with.
func NewArgon2idParams(time uint32, memory uint32, parallelism uint8) (Argon2idParams, error) {
	switch {
	case memory < 12*1024:
		return Argon2idParams{}, Errorf("memory must be at least 12MiB")
	case memory > 1024*1024:
		return Argon2idParams{}, Errorf("memory must be at most 1GiB")
	case time < 3:
		return Argon2idParams{}, Errorf("time must be at least 3")
	case time > 64:
		return Argon2idParams{}, Errorf("time must be at most 64")
	case parallelism < 1:
		return Argon2idParams{}, Errorf("parallelism must be at least 1")
	case parallelism > 64:
		return Argon2idParams{}, Errorf("parallelism must be at most 64")
	}
	return Argon2idParams{Time: time, Memory: memory, Parallelism: parallelism}, nil
}

// Parse `m=<memory>,t=<time>,p=<parallelism>`, the parameter section of the PHC
// format.
func ParseArgon2idParams(s string) (Argon2idParams, error) {
	params := strings.Split(s, ",")
	if len(params) != 3 {
		return Argon2idParams{}, Errorf("expecting 3 parameters")
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
		return Argon2idParams{}, err
	}
	time, err := parseParam(params[1], "t")
	if err != nil {
		return Argon2idParams{}, err
	}
	parallelism, err := parseParam(params[2], "p")
	if err != nil {
		return Argon2idParams{}, err
	}
	if parallelism > 255 {
		return Argon2idParams{}, Errorf("parallelism must be at most 64")
	}
	return NewArgon2idParams(time, memory, uint8(parallelism))
}

func (p Argon2idParams) Marshal() string {
	return fmt.Sprintf("m=%d,t=%d,p=%d", p.Memory, p.Time, p.Parallelism)
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
	params, err := ParseArgon2idParams(parts[3])
	if err != nil {
		return Argon2id{}, err
	}
	// We use `RawStdEncoding` because the PHC "standard" says that base64 padding is omitted.
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) != 32 {
		return Argon2id{}, WrapErrorf(err, "invalid salt")
	}
	return NewArgon2id(Salt(salt), params), nil
}
