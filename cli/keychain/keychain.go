//go:build !darwin && !linux

package keychain

import (
	"context"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	ErrKeychainEntryNotFound      = lib.Errorf("keychain entry not found")
	ErrKeychainEntryAlreadyExists = lib.Errorf("keychain entry already exists")
)

func AddKeychainEntry(ctx context.Context, service, account, secret string) error {
	return lib.Errorf("not implemented")
}

func GetKeychainEntry(ctx context.Context, service, account string) (string, error) {
	return "", lib.Errorf("not implemented")
}

func DeleteKeychainEntry(ctx context.Context, service, account string) error {
	return lib.Errorf("not implemented")
}
