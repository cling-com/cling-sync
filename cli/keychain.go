//go:build !darwin && !linux

package main

import (
	"github.com/flunderpero/cling-sync/lib"
)

var (
	ErrKeychainEntryNotFound      = lib.Errorf("keychain entry not found")
	ErrKeychainEntryAlreadyExists = lib.Errorf("keychain entry already exists")
)

func AddKeychainEntry(service, account, secret string) error {
	return lib.Errorf("not implemented")
}

func GetKeychainEntry(service, account string) (string, error) {
	return "", lib.Errorf("not implemented")
}

func DeleteKeychainEntry(service, account string) error {
	return lib.Errorf("not implemented")
}
