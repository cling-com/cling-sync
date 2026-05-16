package lib

import (
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		key := RawKey([]byte("0123456789abcdef0123456789abcdef"))
		cipher, err := NewCipher(key)
		assert.NoError(err)
		plaintext := "This is a test."
		ad := []byte("Some associated data")
		ciphertext := make([]byte, len(plaintext)+TotalCipherOverhead)
		ciphertext, err = Encrypt([]byte(plaintext), cipher, ad, ciphertext)
		assert.NoError(err)
		decrypted := make([]byte, len(plaintext)+len(ad))
		decrypted, err = Decrypt(ciphertext, cipher, ad, decrypted)
		assert.NoError(err)
		assert.Equal(plaintext, string(decrypted))
	})
	t.Run("Manipulation to the associated data is detected", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		key := RawKey([]byte("0123456789abcdef0123456789abcdef"))
		cipher, err := NewCipher(key)
		assert.NoError(err)
		plaintext := "This is a test."
		ad := []byte("Some associated data")
		ciphertext := make([]byte, len(plaintext)+TotalCipherOverhead)
		ciphertext, err = Encrypt([]byte(plaintext), cipher, ad, ciphertext)
		assert.NoError(err)
		ad[0] = ^ad[0]
		decrypted := make([]byte, len(plaintext)+len(ad))
		_, err = Decrypt(ciphertext, cipher, ad, decrypted)
		assert.Error(err, "message authentication failed")
	})
	t.Run("Decrypt rejects too-small dst buffer", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		key := RawKey([]byte("0123456789abcdef0123456789abcdef"))
		cipher, err := NewCipher(key)
		assert.NoError(err)
		plaintext := "This is a test."
		ad := []byte("Some associated data")
		ciphertext := make([]byte, len(plaintext)+TotalCipherOverhead)
		ciphertext, err = Encrypt([]byte(plaintext), cipher, ad, ciphertext)
		assert.NoError(err)

		// 10 bytes is too small.
		tooSmall := make([]byte, 10)
		_, err = Decrypt(ciphertext, cipher, ad, tooSmall)
		assert.Error(err, "target buffer too small")
	})
}

func TestDeriveUserKey(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		salt := [32]byte([]byte("0123456789abcdef0123456789abcdef"))
		argon2id := NewArgon2id(salt)
		passphrase := []byte("This is a test.")
		key, err := DeriveUserKey(passphrase, argon2id)
		assert.NoError(err)
		key2, err := DeriveUserKey(passphrase, argon2id)
		assert.NoError(err)
		assert.Equal(key, key2)
	})
}

func TestMarshalArgon2id(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		s := "$argon2id$v=19$m=65536,t=16,p=2$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY"
		argon2id, err := UnmarshalArgon2idConfig(s)
		assert.NoError(err)
		assert.Equal(Argon2id{
			Time:        16,
			Memory:      65536,
			Parallelism: 2,
			Salt:        [32]byte([]byte("0123456789abcdef0123456789abcdef")),
		}, argon2id)
		assert.Equal(s, argon2id.Marshal())
	})
}

func FuzzUnmarshalArgon2idConfig(f *testing.F) {
	f.Add("")
	f.Add("$argon2id$v=19$m=131072,t=4,p=2$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = UnmarshalArgon2idConfig(s)
	})
}
