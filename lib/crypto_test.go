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
		argon2id := NewArgon2id(salt, td.Argon2idParams())
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

func TestParseArgon2idParams(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		params, err := ParseArgon2idParams("m=65536,t=16,p=2")
		assert.NoError(err)
		assert.Equal(Argon2idParams{Time: 16, Memory: 65536, Parallelism: 2}, params)
	})

	t.Run("Parameters below the OWASP minimum should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		// The minimum itself is accepted, which is what the tests run at.
		_, err := ParseArgon2idParams("m=12288,t=3,p=1")
		assert.NoError(err)
		_, err = ParseArgon2idParams("m=12287,t=3,p=1")
		assert.Error(err, "memory must be at least 12MiB")
		_, err = ParseArgon2idParams("m=12288,t=2,p=1")
		assert.Error(err, "time must be at least 3")
		_, err = ParseArgon2idParams("m=12288,t=3,p=0")
		assert.Error(err, "parallelism must be at least 1")
	})

	t.Run("Parameters above the maximum should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		// The maximum itself is accepted.
		_, err := ParseArgon2idParams("m=1048576,t=64,p=64")
		assert.NoError(err)
		_, err = ParseArgon2idParams("m=1048577,t=64,p=64")
		assert.Error(err, "memory must be at most 1GiB")
		_, err = ParseArgon2idParams("m=1048576,t=65,p=64")
		assert.Error(err, "time must be at most 64")
		_, err = ParseArgon2idParams("m=1048576,t=64,p=65")
		assert.Error(err, "parallelism must be at most 64")
		// `p` does not even fit into the `uint8` it is stored in.
		_, err = ParseArgon2idParams("m=1048576,t=64,p=256")
		assert.Error(err, "parallelism must be at most 64")
	})
}

func TestNewArgon2idParams(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		params, err := NewArgon2idParams(16, 65536, 2)
		assert.NoError(err)
		assert.Equal(Argon2idParams{Time: 16, Memory: 65536, Parallelism: 2}, params)
		assert.Equal("m=65536,t=16,p=2", params.Marshal())
	})

	t.Run("The defaults are within the accepted range", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		defaults := DefaultArgon2idParams()
		params, err := NewArgon2idParams(defaults.Time, defaults.Memory, defaults.Parallelism)
		assert.NoError(err)
		assert.Equal(defaults, params)
	})

	t.Run("Parameters outside the accepted range should fail", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		_, err := NewArgon2idParams(3, 12*1024-1, 1)
		assert.Error(err, "memory must be at least 12MiB")
		_, err = NewArgon2idParams(3, 1024*1024+1, 1)
		assert.Error(err, "memory must be at most 1GiB")
		_, err = NewArgon2idParams(2, 12*1024, 1)
		assert.Error(err, "time must be at least 3")
		_, err = NewArgon2idParams(65, 12*1024, 1)
		assert.Error(err, "time must be at most 64")
		_, err = NewArgon2idParams(3, 12*1024, 0)
		assert.Error(err, "parallelism must be at least 1")
		_, err = NewArgon2idParams(3, 12*1024, 65)
		assert.Error(err, "parallelism must be at most 64")
	})
}

func FuzzUnmarshalArgon2idConfig(f *testing.F) {
	f.Add("")
	f.Add("$argon2id$v=19$m=131072,t=4,p=2$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY")
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = UnmarshalArgon2idConfig(s)
	})
}
