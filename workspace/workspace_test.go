package workspace

import (
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestWorkspaceNewAndOpen(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)
		remote := "the/remote/repository"

		// Create new workspace.
		ws, err := NewWorkspace(local, td.NewFS(t), RemoteRepository(remote))
		assert.NoError(err)
		assert.Equal(remote, string(ws.RemoteRepository))
		head, err := ws.Head()
		assert.NoError(err)
		assert.Equal(true, head.IsRoot())

		// Open workspace.
		open, err := OpenWorkspace(local, td.NewFS(t))
		assert.NoError(err)
		assert.Equal(ws.RemoteRepository, open.RemoteRepository)
	})

	t.Run("Non existing workspace", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)

		// Open workspace.
		_, err := OpenWorkspace(local, td.NewFS(t))
		assert.Equal(lib.ErrStorageNotFound, err)
	})

	t.Run("Call NewWorkspace on existing workspace", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)
		remote := "the/remote/repository"

		// Create new workspace.
		_, err := NewWorkspace(local, td.NewFS(t), RemoteRepository(remote))
		assert.NoError(err)

		// Try to create new workspace inside existing workspace.
		_, err = NewWorkspace(local, td.NewFS(t), RemoteRepository(remote))
		assert.ErrorIs(err, lib.ErrStorageAlreadyExists)
	})

	t.Run("Workspaces can be nested", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)
		remote := "the/remote/repository"

		// Create new workspace.
		_, err := NewWorkspace(local, td.NewFS(t), RemoteRepository(remote))
		assert.NoError(err)

		// Try to create new workspace in a sub directory.
		localSub, err := local.MkSub("sub")
		assert.NoError(err)
		_, err = NewWorkspace(localSub, td.NewFS(t), RemoteRepository(remote))
		assert.NoError(err)
	})

	t.Run("Workspace alongside repository", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		fs := td.NewFS(t)
		remote := "the/remote/repository"

		repositoryStorage, err := lib.NewFileStorage(fs, lib.StoragePurposeRepository)
		assert.NoError(err)
		err = repositoryStorage.Init(lib.Toml{"encryption": {"version": "1"}}, "header comment")
		assert.NoError(err)

		_, err = NewWorkspace(fs, td.NewFS(t), RemoteRepository(remote))
		assert.NoError(err)
	})
}
