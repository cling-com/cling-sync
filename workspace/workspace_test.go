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
		ws, err := NewWorkspace(local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
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

	t.Run("Happy path with path prefix", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)
		pathPrefix, err := lib.NewPath("some/path/inside/the/repository")
		assert.NoError(err)

		// Create new workspace.
		ws, err := NewWorkspace(local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)
		assert.Equal(remote, string(ws.RemoteRepository))
		head, err := ws.Head()
		assert.NoError(err)
		assert.Equal(true, head.IsRoot())

		// Open workspace.
		open, err := OpenWorkspace(local, td.NewFS(t))
		assert.NoError(err)
		assert.Equal(ws.RemoteRepository, open.RemoteRepository)
		assert.Equal("some/path/inside/the/repository", open.PathPrefix.String())
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

		// Create new workspace.
		_, err := NewWorkspace(local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)

		// Try to create new workspace inside existing workspace.
		_, err = NewWorkspace(local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.ErrorIs(err, lib.ErrStorageAlreadyExists)
	})

	t.Run("Workspaces can be nested", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		local := td.NewFS(t)

		// Create new workspace.
		_, err := NewWorkspace(local, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)

		// Try to create new workspace in a sub directory.
		localSub, err := local.MkSub("sub")
		assert.NoError(err)
		_, err = NewWorkspace(localSub, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)
	})

	t.Run("Workspace alongside repository", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		fs := td.NewFS(t)

		repositoryStorage, err := lib.NewFileStorage(fs, lib.StoragePurposeRepository)
		assert.NoError(err)
		err = repositoryStorage.Init(lib.Toml{"encryption": {"version": "1"}}, "header comment")
		assert.NoError(err)

		_, err = NewWorkspace(fs, td.NewFS(t), RemoteRepository(remote), pathPrefix)
		assert.NoError(err)
	})
}

func TestReadWriteDeleteRepositoryKeys(t *testing.T) { //nolint:tparallel
	assert := lib.NewAssert(t)
	kek, err := lib.NewRawKey()
	assert.NoError(err)
	blockIdHmacKey, err := lib.NewRawKey()
	assert.NoError(err)
	keys := &lib.RepositoryKeys{
		KEK:            kek,
		BlockIdHmacKey: blockIdHmacKey,
	}
	encKey, err := lib.NewRawKey()
	assert.NoError(err)
	encKeyCipher, err := lib.NewCipher(encKey)
	assert.NoError(err)

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		remote := "the/remote/repository1"

		ws, err := NewWorkspace(td.NewFS(t), td.NewFS(t), RemoteRepository(remote), lib.Path{})
		assert.NoError(err)

		// Write keys.
		err = ws.WriteRepositoryKeys(keys, encKeyCipher)
		assert.NoError(err)

		// Read keys.
		readKeys, err := ws.ReadRepositoryKeys(encKeyCipher)
		assert.NoError(err)
		assert.Equal(keys, readKeys)

		// Delete keys.
		err = ws.DeleteRepositoryKeys()
		assert.NoError(err)

		// Try to read keys again.
		_, err = ws.ReadRepositoryKeys(encKeyCipher)
		assert.ErrorIs(err, ErrRepositoryKeysNotFound)
	})

	t.Run("If keychain entry already exists, use it", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		remote := "the/remote/repository2"

		ws, err := NewWorkspace(td.NewFS(t), td.NewFS(t), RemoteRepository(remote), lib.Path{})
		assert.NoError(err)

		// Write keys.
		err = ws.WriteRepositoryKeys(keys, encKeyCipher)
		assert.NoError(err)

		// Try to write keys for a second workspace tied to the same repository.
		ws2, err := NewWorkspace(td.NewFS(t), td.NewFS(t), RemoteRepository(remote), lib.Path{})
		assert.NoError(err)

		// Write keys.
		err = ws2.WriteRepositoryKeys(keys, encKeyCipher)
		assert.NoError(err)

		readKeys, err := ws2.ReadRepositoryKeys(encKeyCipher)
		assert.NoError(err)
		assert.Equal(keys, readKeys)

		readKeys, err = ws.ReadRepositoryKeys(encKeyCipher)
		assert.NoError(err)
		assert.Equal(keys, readKeys)
	})

	t.Run("Tamper with the encrypted keys", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		remote := "the/remote/repository3"

		ws, err := NewWorkspace(td.NewFS(t), td.NewFS(t), RemoteRepository(remote), lib.Path{})
		assert.NoError(err)

		// Write keys.
		err = ws.WriteRepositoryKeys(keys, encKeyCipher)
		assert.NoError(err)

		// Manipulate key.
		tamperedKey := lib.RawKey(append([]byte{}, encKey[:]...))
		tamperedKey[0] ^= 0x01
		tamperedCipher, err := lib.NewCipher(tamperedKey)
		assert.NoError(err)
		_, err = ws.ReadRepositoryKeys(tamperedCipher)
		assert.Error(err, "message authentication failed")
	})
}
