//nolint:forbidigo
package lib

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestFileLock(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		lockPath := filepath.Join(t.TempDir(), "lock")

		lock := NewLock(lockPath)
		err := lock.Lock(t.Context())
		assert.NoError(err)

		// Try to acquire the lock again but cancel after 100ms.
		lock2 := NewLock(lockPath)
		ctx2, cancel2 := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel2()
		err = lock2.Lock(ctx2)
		assert.ErrorIs(err, context.DeadlineExceeded)

		err = lock.Unlock()
		assert.NoError(err)
		err = lock2.Lock(t.Context())
		assert.NoError(err)
	})

	t.Run("TryLock", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		lockPath := filepath.Join(t.TempDir(), "lock")

		lock := NewLock(lockPath)
		ok, err := lock.TryLock()
		assert.NoError(err)
		assert.Equal(true, ok)

		lock2 := NewLock(lockPath)
		ok, err = lock2.TryLock()
		assert.NoError(err)
		assert.Equal(false, ok)
	})

	t.Run("Lock already acquired", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		lockPath := filepath.Join(t.TempDir(), "lock")

		lock := NewLock(lockPath)
		err := lock.Lock(t.Context())
		assert.NoError(err)

		err = lock.Lock(t.Context())
		assert.ErrorIs(err, ErrLockAlreadyAcquired)
	})

	t.Run("Double close should be fine", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		lockPath := filepath.Join(t.TempDir(), "lock")

		lock := NewLock(lockPath)
		err := lock.Lock(t.Context())
		assert.NoError(err)

		err = lock.Unlock()
		assert.NoError(err)

		err = lock.Unlock()
		assert.NoError(err)
	})
}

func TestFileLockInterProcess(t *testing.T) { //nolint:paralleltest
	if os.Getenv("LOCK_TEST_INTERPROCESS") == "1" {
		// We are in the second process.
		lockPath := os.Getenv("LOCK_TEST_LOCK_PATH")
		readyPath := lockPath + ".ready"
		lock := NewLock(lockPath)
		if err := lock.Lock(t.Context()); err != nil {
			os.Exit(1)
		}
		if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
			os.Exit(2)
		}
		time.Sleep(time.Hour)
		os.Exit(3)
	}

	assert := NewAssert(t)
	lockPath := filepath.Join(t.TempDir(), "lock_interprocess")
	readyPath := lockPath + ".ready"

	// Run the test itself as a subprocess.
	second := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestFileLockInterProcess$") //nolint:gosec
	second.Env = append(os.Environ(), "LOCK_TEST_INTERPROCESS=1", "LOCK_TEST_LOCK_PATH="+lockPath)
	second.Stdout = os.Stdout
	second.Stderr = os.Stderr
	err := second.Start()
	assert.NoError(err)
	defer func() {
		_ = second.Process.Kill()
	}()

	// Wait for child to acquire lock (wait for ready file).
	ready := false
	for range 50 {
		if _, err := os.Stat(readyPath); err == nil {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.Equal(true, ready, "Second process did not acquire lock in time")

	// Now try to acquire lock. It should fail (timeout).
	lock := NewLock(lockPath)
	failCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err = lock.Lock(failCtx)
	assert.ErrorIs(err, context.DeadlineExceeded)

	// Now kill the second process after a bit.
	done := make(chan error, 1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		err := second.Process.Kill()
		_ = second.Wait()
		done <- err
	}()

	// Now try to acquire lock. It should succeed but not immediately.
	t0 := time.Now()
	err = lock.Lock(t.Context())
	assert.NoError(<-done)
	assert.NoError(err)
	assert.Greater(time.Since(t0), 199*time.Millisecond)
}
