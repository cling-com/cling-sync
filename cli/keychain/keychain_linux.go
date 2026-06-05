//go:build linux && !mock

package keychain

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"strings"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

var (
	ErrKeychainEntryNotFound      = lib.Errorf("keychain entry not found")
	ErrKeychainEntryAlreadyExists = lib.Errorf("keychain entry already exists")
	ErrKeychainLocked             = lib.Errorf("login keyring is locked or unavailable, unlock it and retry")
)

// A locked keyring makes secret-tool hang on an unlock prompt (forever when
// headless). 30s still leaves room for a real interactive unlock.
const keychainTimeout = 30 * time.Second

func AddKeychainEntry(ctx context.Context, service, account, secret string) error {
	_, err := GetKeychainEntry(ctx, service, account)
	if err == nil {
		return ErrKeychainEntryAlreadyExists
	}
	if errors.Is(err, ErrKeychainLocked) {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, keychainTimeout)
	defer cancel()
	cmd := exec.CommandContext(
		ctx,
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
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ErrKeychainLocked
		}
		return lib.Errorf("failed to store keychain entry: %v: %s", err, stderr.String())
	}
	return nil
}

func GetKeychainEntry(ctx context.Context, service, account string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, keychainTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "secret-tool", "lookup", "service", service, "account", account)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	output, err := cmd.Output()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", ErrKeychainLocked
		}
		if isSecretNotFound(stderr.String()) {
			return "", ErrKeychainEntryNotFound
		}
		return "", lib.Errorf("failed to lookup keychain entry: %v: %s", err, stderr.String())
	}

	return string(output), nil
}

// secret-tool exits non-zero for a missing item: newer versions print nothing,
// older ones print "No matching secrets". A genuine failure (no D-Bus, locked
// keyring) always writes a message to stderr, so empty stderr means not found.
func isSecretNotFound(stderr string) bool {
	stderr = strings.TrimSpace(stderr)
	return stderr == "" || strings.Contains(stderr, "No matching secrets")
}

func DeleteKeychainEntry(ctx context.Context, service, account string) error {
	ctx, cancel := context.WithTimeout(ctx, keychainTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "secret-tool", "clear", "service", service, "account", account)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ErrKeychainLocked
		}
		if isSecretNotFound(stderr.String()) {
			return nil
		}
		return lib.Errorf("failed to delete keychain entry: %v: %s", err, stderr.String())
	}
	return nil
}
