//go:build linux && !mock

package main

import (
	"bytes"
	"os/exec"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	ErrKeychainEntryNotFound      = lib.Errorf("keychain entry not found")
	ErrKeychainEntryAlreadyExists = lib.Errorf("keychain entry already exists")
)

func AddKeychainEntry(service, account, secret string) error {
	_, err := GetKeychainEntry(service, account)
	if err == nil {
		return ErrKeychainEntryAlreadyExists
	}
	cmd := exec.Command(
		"secret-tool",
		"store",
		"--label",
		"Secret for cling-sync",
		"service",
		service,
		"account",
		account,
	)
	cmd.Stdin = strings.NewReader(secret)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		return lib.Errorf("failed to store keychain entry: %v: %s", err, stderr.String())
	}
	return nil
}

func GetKeychainEntry(service, account string) (string, error) {
	cmd := exec.Command("secret-tool", "lookup", "service", service, "account", account)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	output, err := cmd.Output()
	if err != nil {
		// secret-tool returns a non-zero exit code if the secret is not found.
		if strings.Contains(stderr.String(), "No matching secrets") {
			return "", ErrKeychainEntryNotFound
		}
		return "", lib.Errorf("failed to lookup keychain entry: %v: %s", err, stderr.String())
	}

	return string(output), nil
}

func DeleteKeychainEntry(service, account string) error {
	cmd := exec.Command("secret-tool", "clear", "service", service, "account", account)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		if strings.Contains(stderr.String(), "No matching secrets") {
			return nil
		}
		return lib.Errorf("failed to delete keychain entry: %v: %s", err, stderr.String())
	}
	return nil
}
