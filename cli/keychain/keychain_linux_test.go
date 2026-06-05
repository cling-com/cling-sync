//go:build linux && !mock

package keychain

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

// Exercises the real secret-tool backend, so it only runs where a keyring is
// available. test/test_linux.sh provides one.
func TestKeychainLinux(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("secret-tool"); err != nil {
		t.Skip("secret-tool not installed; run via test/test_linux.sh")
	}
	const service = "cling-sync-keychain-test"

	t.Run("Lookup of a missing entry should return ErrKeychainEntryNotFound", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		ctx := t.Context()
		_ = DeleteKeychainEntry(ctx, service, "missing")
		_, err := GetKeychainEntry(ctx, service, "missing")
		assert.ErrorIs(err, ErrKeychainEntryNotFound)
	})

	t.Run("Store then lookup should round-trip the secret", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		ctx := t.Context()
		account := "roundtrip"
		secret := "round-trip-secret-value"
		t.Cleanup(func() { _ = DeleteKeychainEntry(ctx, service, account) })
		_ = DeleteKeychainEntry(ctx, service, account)
		assert.NoError(AddKeychainEntry(ctx, service, account, secret))
		got, err := GetKeychainEntry(ctx, service, account)
		assert.NoError(err)
		assert.Equal(secret, got)
	})

	t.Run("Storing a duplicate should return ErrKeychainEntryAlreadyExists", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		ctx := t.Context()
		account := "duplicate"
		t.Cleanup(func() { _ = DeleteKeychainEntry(ctx, service, account) })
		_ = DeleteKeychainEntry(ctx, service, account)
		assert.NoError(AddKeychainEntry(ctx, service, account, "first"))
		assert.ErrorIs(AddKeychainEntry(ctx, service, account, "second"), ErrKeychainEntryAlreadyExists)
	})

	t.Run("Deleting a missing entry should not fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		assert.NoError(DeleteKeychainEntry(t.Context(), service, "absent"))
	})
}

// An expired context drives the same timeout path a locked keyring triggers,
// without needing a real (here unreproducible, it auto-unlocks) lock.
func TestKeychainLinuxLockedTimeout(t *testing.T) {
	t.Parallel()
	assert := lib.NewAssert(t)
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := GetKeychainEntry(ctx, "cling-sync-keychain-test", "locked")
	assert.ErrorIs(err, ErrKeychainLocked)
}
