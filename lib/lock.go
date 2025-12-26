//go:build !wasm

package lib

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

var ErrLockAlreadyAcquired = errors.New("lock already acquired")

type FileLock struct {
	path string
	f    *os.File //nolint:forbidigo
}

func NewLock(path string) *FileLock {
	return &FileLock{path: path, f: nil}
}

// Block until the lock can be acquired or the context is cancelled.
func (l *FileLock) Lock(ctx context.Context) error {
	// Try immediately.
	ok, err := l.TryLock()
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	// Lock not acquired, try every 100ms.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ok, err := l.TryLock()
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-ctx.Done():
			return WrapErrorf(ctx.Err(), "failed to acquire lock %s", l.path)
		}
	}
}

func (l *FileLock) TryLock() (bool, error) {
	err := l.acquire()
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (l *FileLock) Unlock() error {
	if l.f == nil {
		return nil
	}
	f := l.f
	l.f = nil
	if err := unix.Flock(int(f.Fd()), unix.LOCK_UN); err != nil {
		_ = f.Close()
		return WrapErrorf(err, "failed to unlock %s", l.path)
	}
	if err := f.Close(); err != nil {
		return WrapErrorf(err, "failed to close %s", l.path)
	}
	return nil
}

func (l *FileLock) acquire() error {
	if l.f != nil {
		return WrapErrorf(ErrLockAlreadyAcquired, "lock %s is already acquired", l.path)
	}
	f, err := os.OpenFile(l.path, os.O_RDWR|os.O_CREATE, 0o600) //nolint:forbidigo
	if err != nil {
		return WrapErrorf(err, "failed to open lock file %s", l.path)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return WrapErrorf(err, "failed to acquire lock %s", l.path)
	}
	// Write the pid and timestamp to the file. We ignore errors because it's only debug info.
	if err := f.Truncate(0); err == nil {
		content := fmt.Sprintf("%d %s\n", os.Getpid(), time.Now().Format(time.RFC3339Nano)) //nolint:forbidigo
		if _, err := f.WriteString(content); err == nil {
			_ = f.Sync()
		}
	}
	l.f = f
	return nil
}
