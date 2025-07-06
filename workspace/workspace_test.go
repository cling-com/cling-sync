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
		local := t.TempDir()
		remote := t.TempDir()

		// Create new workspace.
		ws, err := NewWorkspace(local, RemoteRepository(remote))
		assert.NoError(err)
		assert.Equal(local, ws.WorkspacePath)
		assert.Equal(remote, string(ws.RemoteRepository))
		head, err := ws.Head()
		assert.NoError(err)
		assert.Equal(true, head.IsRoot())

		// Open workspace.
		open, err := OpenWorkspace(local)
		assert.NoError(err)
		assert.Equal(ws.RemoteRepository, open.RemoteRepository)
		assert.Equal(ws.WorkspacePath, open.WorkspacePath)
	})

	t.Run("Non existing workspace", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := filepath.Join(t.TempDir(), "local")

		// Open workspace.
		_, err := OpenWorkspace(local)
		assert.Equal(lib.ErrStorageNotFound, err)
	})

	t.Run("Call NewWorkspace on existing workspace", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := t.TempDir()
		remote := t.TempDir()

		// Create new workspace.
		_, err := NewWorkspace(local, RemoteRepository(remote))
		assert.NoError(err)

		// Try to create new workspace inside existing workspace.
		_, err = NewWorkspace(local, RemoteRepository(remote))
		assert.ErrorIs(err, lib.ErrStorageAlreadyExists)
	})

	t.Run("Workspaces can be nested", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := t.TempDir()
		remote := t.TempDir()

		// Create new workspace.
		_, err := NewWorkspace(local, RemoteRepository(remote))
		assert.NoError(err)

		// Try to create new workspace in a sub directory.
		localSub := filepath.Join(local, "sub")
		_, err = NewWorkspace(localSub, RemoteRepository(remote))
		assert.NoError(err)
	})

	t.Run("Workspace alongside repository", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		dir := t.TempDir()

		repositoryStorage, err := lib.NewFileStorage(dir, lib.StoragePurposeRepository)
		assert.NoError(err)
		err = repositoryStorage.Init(lib.Toml{"encryption": {"version": "1"}}, "header comment")
		assert.NoError(err)

		_, err = NewWorkspace(dir, RemoteRepository(dir))
		assert.NoError(err)
	})
}
