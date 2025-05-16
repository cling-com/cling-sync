package workspace

import (
	"path/filepath"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestWorkspaceNewAndOpen(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := filepath.Join(t.TempDir(), "local")
		remote := filepath.Join(t.TempDir(), "remote")

		// Create new workspace.
		ws, err := NewWorkspace(local, RemoteRepository(remote))
		assert.NoError(err)
		assert.Equal(local, ws.WorkspacePath)
		assert.Equal(remote, string(ws.RemoteRepository))

		// Open workspace.
		open, err := OpenWorkspace(local)
		assert.NoError(err)
		assert.Equal(ws, open)
	})

	t.Run("Try to open non existing workspace", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := filepath.Join(t.TempDir(), "local")

		// Open workspace.
		_, err := OpenWorkspace(local)
		assert.Equal(ErrNoWorkspace, err)
	})

	t.Run("Try to create workspace in existing workspace", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := filepath.Join(t.TempDir(), "local")
		remote := filepath.Join(t.TempDir(), "remote")

		// Create new workspace.
		_, err := NewWorkspace(local, RemoteRepository(remote))
		assert.NoError(err)

		// Try to create new workspace inside existing workspace.
		_, err = NewWorkspace(local, RemoteRepository(remote))
		assert.Error(err, "a workspace already exists")

		// Try to create new workspace in a sub directory.
		localSub := filepath.Join(local, "sub")
		_, err = NewWorkspace(localSub, RemoteRepository(remote))
		assert.Error(err, "a workspace already exists")
	})
}

// This test has to stand alone, because you cannot use `t.Chdir` with `t.Parallel`.
func TestWorkspaceNewUsesAbsolutePaths(t *testing.T) { //nolint:paralleltest
	assert := lib.NewAssert(t)
	local := "local"
	remote := "remote"
	t.Chdir(t.TempDir())

	// Create new workspace.
	ws, err := NewWorkspace(local, RemoteRepository(remote))
	assert.NoError(err)

	// Open workspace.
	open, err := OpenWorkspace(local)
	assert.NoError(err)
	assert.Equal(ws, open)

	localAbs, err := filepath.Abs(local)
	assert.NoError(err)
	remoteAbs, err := filepath.Abs(remote)
	assert.NoError(err)
	assert.Equal(localAbs, ws.WorkspacePath)
	assert.Equal(remoteAbs, string(ws.RemoteRepository))
}
