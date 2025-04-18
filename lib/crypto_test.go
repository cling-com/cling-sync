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
}

func TestDeriveUserKey(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		assert := NewAssert(t)
		t.Parallel()
		salt := [32]byte([]byte("0123456789abcdef0123456789abcdef"))
		passphrase := []byte("This is a test.")
		key, err := DeriveUserKey(passphrase, salt)
		assert.NoError(err)
		key2, err := DeriveUserKey(passphrase, salt)
		assert.NoError(err)
		assert.Equal(key, key2)
	})
}
