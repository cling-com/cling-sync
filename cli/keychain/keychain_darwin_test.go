//go:build darwin && !mock

package keychain

import (
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestKeychainError(t *testing.T) {
	t.Parallel()
	t.Run("Locked keychain should map to ErrKeychainLocked with an unlock hint", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		for _, status := range []int32{errSecInteractionNotAllowed, errSecInteractionRequired} {
			err := keychainError(status, "lookup")
			assert.ErrorIs(err, ErrKeychainLocked, status)
			assert.Error(err, "security unlock-keychain", status)
		}
	})
	t.Run("Other codes should carry Apple's message and the numeric code", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		// The "(<code>)" form only appears alongside SecCopyErrorMessageString's text,
		// so this also proves the code wasn't left bare or mapped to locked.
		assert.Error(keychainError(-50, "lookup"), "(-50)") // errSecParam
	})
}
