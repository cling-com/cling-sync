//go:build darwin && !mock

package keychain

import (
	"errors"
	"strings"
	"testing"
)

func TestKeychainError(t *testing.T) {
	t.Parallel()
	t.Run("Locked keychain should map to ErrKeychainLocked with an unlock hint", func(t *testing.T) {
		t.Parallel()
		for _, status := range []int32{errSecInteractionNotAllowed, errSecInteractionRequired} {
			err := keychainError(status, "lookup")
			if !errors.Is(err, ErrKeychainLocked) {
				t.Fatalf("status %d: expected ErrKeychainLocked, got %v", status, err)
			}
			if !strings.Contains(err.Error(), "security unlock-keychain") {
				t.Fatalf("status %d: expected unlock hint, got %q", status, err.Error())
			}
		}
	})
	t.Run("Other codes should carry Apple's message and the numeric code", func(t *testing.T) {
		t.Parallel()
		err := keychainError(-50, "lookup") // errSecParam
		if errors.Is(err, ErrKeychainLocked) {
			t.Fatalf("errSecParam (-50) should not map to locked: %v", err)
		}
		msg := err.Error()
		if !strings.Contains(msg, "(-50)") {
			t.Fatalf("expected the numeric code in %q", msg)
		}
		// SecCopyErrorMessageString resolves a human-readable string for -50, so the
		// error must be more than just the bare number.
		if strings.HasSuffix(msg, "entry: -50") {
			t.Fatalf("expected a human-readable description, got %q", msg)
		}
	})
}
