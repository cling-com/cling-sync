package workspace

import (
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestWorkspaceNewAndOpen(t *testing.T) {
	remote := "the/remote/repository" // This is only ever written to the workspace config.
	pathPrefix, _ := lib.NewPath("")
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)

		// Create new workspace.
		ws, err := NewWorkspace(t.Context(), local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)
		assert.Equal(remote, string(ws.RemoteRepository))
		head, err := ws.Head(t.Context())
		assert.NoError(err)
		assert.Equal(true, head.IsRoot())

		// Open workspace.
		open, err := OpenWorkspace(t.Context(), local, td.NewFS(t))
		assert.NoError(err)
		assert.Equal(ws.RemoteRepository, open.RemoteRepository)
	})

	t.Run("Happy path with path prefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)
		pathPrefix, err := lib.NewPath("some/path/inside/the/repository")
		assert.NoError(err)

		// Create new workspace.
		ws, err := NewWorkspace(t.Context(), local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)
		assert.Equal(remote, string(ws.RemoteRepository))
		head, err := ws.Head(t.Context())
		assert.NoError(err)
		assert.Equal(true, head.IsRoot())

		// Open workspace.
		open, err := OpenWorkspace(t.Context(), local, td.NewFS(t))
		assert.NoError(err)
		assert.Equal(ws.RemoteRepository, open.RemoteRepository)
		assert.Equal("some/path/inside/the/repository", open.PathPrefix.String())
	})

	t.Run("Non existing workspace", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)

		// Open workspace.
		_, err := OpenWorkspace(t.Context(), local, td.NewFS(t))
		assert.Equal(lib.ErrStorageNotFound, err)
	})

	t.Run("Call NewWorkspace on existing workspace", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)

		// Create new workspace.
		_, err := NewWorkspace(t.Context(), local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)

		// Try to create new workspace inside existing workspace.
		_, err = NewWorkspace(t.Context(), local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.ErrorIs(err, lib.ErrStorageAlreadyExists)
	})

	t.Run("Workspaces can be nested", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)

		// Create new workspace.
		_, err := NewWorkspace(t.Context(), local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)

		// Try to create new workspace in a sub directory.
		localSub, err := local.MkSub("sub")
		assert.NoError(err)
		_, err = NewWorkspace(t.Context(), localSub, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)
	})

	t.Run("Workspace alongside repository", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		fs := td.NewFS(t)

		repositoryStorage, err := lib.NewFileStorage(fs, lib.StoragePurposeRepository)
		assert.NoError(err)
		err = repositoryStorage.Init(t.Context(), lib.Toml{"encryption": {"version": "1"}}, "header comment")
		assert.NoError(err)

		_, err = NewWorkspace(t.Context(), fs, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)
	})
}
