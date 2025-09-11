//go:build mock

package main

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	ErrKeychainEntryNotFound      = lib.Errorf("keychain entry not found")
	ErrKeychainEntryAlreadyExists = lib.Errorf("keychain entry already exists")
)

func AddKeychainEntry(service, account, secret string) error {
	entries, err := readKeychainEntries()
	if err != nil {
		return err
	}
	_, ok := entries[service+":"+account]
	if ok {
		return ErrKeychainEntryAlreadyExists
	}
	entries[service+":"+account] = secret
	return writeKeychainEntries(entries)
}

func GetKeychainEntry(service, account string) (string, error) {
	entries, err := readKeychainEntries()
	if err != nil {
		return "", err
	}
	entry, ok := entries[service+":"+account]
	if !ok {
		return "", ErrKeychainEntryNotFound
	}
	return entry, nil
}

func DeleteKeychainEntry(service, account string) error {
	entries, err := readKeychainEntries()
	if err != nil {
		return err
	}
	delete(entries, service+":"+account)
	return writeKeychainEntries(entries)
}

func readKeychainEntries() (map[string]string, error) {
	data, err := os.ReadFile(filename())
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	var entries map[string]string
	err = json.Unmarshal(data, &entries)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func writeKeychainEntries(entries map[string]string) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	return os.WriteFile(filename(), data, 0o600)
}

func filename() string {
	return os.TempDir() + "/mock_keychain.txt"
}
