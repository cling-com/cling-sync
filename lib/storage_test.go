package lib

import (
	"bytes"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"testing"
)

func TestFileStorageInit(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(Toml{"encryption": {"version": "1"}}, "header comment")
		assert.NoError(err)
		// Make sure the storage config file has been written.
		files, err := sut.FS.ReadDir(".")
		assert.NoError(err)
		assert.Equal(1, len(files))
		clingDir := files[0]
		assert.Equal(".cling", clingDir.Name())
		assert.Equal(true, clingDir.IsDir())
		files, err = sut.FS.ReadDir(".cling")
		assert.NoError(err)
		assert.Equal(2, len(files))
		configFile := files[slices.IndexFunc(files, func(f fs.DirEntry) bool { return !f.IsDir() })]

		// Test that the storage config file has been written.
		assert.Equal("repository.txt", configFile.Name())
		f, err := sut.FS.OpenRead(filepath.Join(".cling", configFile.Name()))
		assert.NoError(err)
		defer f.Close() //nolint:errcheck
		toml, err := ReadToml(f)
		assert.NoError(err)
		assert.Equal(Toml{"encryption": {"version": "1"}}, toml)

		files, err = sut.FS.ReadDir(filepath.Join(".cling", "repository"))
		assert.NoError(err)
		assert.Equal(2, len(files))
		names := make([]string, len(files))
		for i, f := range files {
			names[i] = f.Name()
		}
		slices.Sort(names)
		assert.Equal([]string{"objects", "refs"}, names)
	})

	t.Run("Storage already exists", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(Toml{"encryption": {"version": "1"}}, "header comment")
		assert.NoError(err)
		err = sut.Init(Toml{"encryption": {"version": "1"}}, "header comment")
		assert.ErrorIs(err, ErrStorageAlreadyExists)
	})
}

func TestFileStorageOpen(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut, err := NewFileStorage(fs, StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(Toml{"encryption": {"version": "1"}}, "header comment")
		assert.NoError(err)

		sut, err = NewFileStorage(fs, StoragePurposeRepository)
		assert.NoError(err)
		toml, err := sut.Open()
		assert.NoError(err)
		assert.Equal(Toml{"encryption": {"version": "1"}}, toml)
	})

	t.Run("Repository does not exist", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		_, err = sut.Open()
		assert.ErrorIs(err, ErrStorageNotFound)
	})

	t.Run("Repository config file is missing", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut, err := NewFileStorage(fs, StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(nil, "")
		assert.NoError(err)
		err = fs.Remove(filepath.Join(".cling", "repository.txt"))
		assert.NoError(err)

		sut, err = NewFileStorage(fs, StoragePurposeRepository)
		assert.NoError(err)
		_, err = sut.Open()
		assert.ErrorIs(err, ErrStorageNotFound)
	})
}

func TestFileStorageMultiPurpose(t *testing.T) {
	t.Parallel()
	assert := NewAssert(t)
	commonFS := td.NewFS(t)
	repo, err := NewFileStorage(commonFS, StoragePurposeRepository)
	assert.NoError(err)
	workspace, err := NewFileStorage(commonFS, StoragePurposeWorkspace)
	assert.NoError(err)

	t.Run("Init", func(t *testing.T) { //nolint:paralleltest
		err = repo.Init(Toml{"repository": {"version": "1"}}, "repository header comment")
		assert.NoError(err)
		err = workspace.Init(Toml{"workspace": {"version": "1"}}, "workspace header comment")
		assert.NoError(err)
	})

	t.Run("Control files", func(t *testing.T) { //nolint:paralleltest
		hasControlFile, err := repo.HasControlFile(ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(false, hasControlFile)

		err = repo.WriteControlFile(ControlFileSectionRefs, "head", []byte("1234"))
		assert.NoError(err)
		hasControlFile, err = repo.HasControlFile(ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(true, hasControlFile)
		err = workspace.WriteControlFile(ControlFileSectionRefs, "head", []byte("5678"))
		assert.NoError(err)

		// Verify permissions.
		stat, err := commonFS.Stat(filepath.Join(".cling", "repository", "refs", "head"))
		assert.NoError(err)
		assert.Equal(fs.FileMode(0o600), stat.Mode().Perm())

		repoCtrlContent, err := repo.ReadControlFile(ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("1234"), repoCtrlContent)
		workspaceCtrlContent, err := workspace.ReadControlFile(ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("5678"), workspaceCtrlContent)

		err = repo.DeleteControlFile(ControlFileSectionRefs, "head")
		assert.NoError(err)
		hasControlFile, err = repo.HasControlFile(ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(false, hasControlFile)
	})

	t.Run("ReadControlFile should return ErrControlFileNotFound", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		_, err = sut.ReadControlFile(ControlFileSectionRefs, "head")
		assert.ErrorIs(err, ErrControlFileNotFound)
	})

	t.Run("DeleteControlFile should return ErrControlFileNotFound", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.DeleteControlFile(ControlFileSectionRefs, "head")
		assert.ErrorIs(err, ErrControlFileNotFound)
	})

	t.Run("Read and write block", func(t *testing.T) { //nolint:paralleltest
		// Write a block with the same block id for each purpose.
		repoBlock := []byte("a repo block")
		workspaceBlock := []byte("a workspace block")
		blockId := td.BlockId("1")

		ok, err := repo.WriteBlock(blockId, repoBlock)
		assert.NoError(err)
		assert.Equal(false, ok)

		ok, err = workspace.WriteBlock(blockId, workspaceBlock)
		assert.NoError(err)
		assert.Equal(false, ok)

		buf := NewBlockBuf()
		data, err := repo.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal(repoBlock, data)

		data, err = workspace.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal(workspaceBlock, data)
	})
}

func TestFileStorageBlocks(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(nil, "")
		assert.NoError(err)

		blockId := td.BlockId("1")
		data := []byte("block 1 data")
		ok, err := sut.HasBlock(blockId)
		assert.NoError(err)
		assert.Equal(false, ok)

		// Write the block and verify its format on disk.
		existed, err := sut.WriteBlock(blockId, data)
		assert.NoError(err)
		assert.Equal(false, existed)

		// Verify size and permissions.
		stat, err := sut.FS.Stat(sut.blockPath(blockId))
		assert.NoError(err)
		assert.Equal(int64(len(data)), stat.Size())
		assert.Equal(fs.FileMode(0o400), stat.Mode().Perm())

		f, err := sut.FS.OpenRead(sut.blockPath(blockId))
		assert.NoError(err)
		defer f.Close() //nolint:errcheck
		onDisk, err := io.ReadAll(f)
		assert.NoError(err)
		assert.Equal(data, onDisk)
		_ = f.Close()

		// Now `HasBlock` should return `true`.
		ok, err = sut.HasBlock(blockId)
		assert.NoError(err)
		assert.Equal(true, ok)

		// Read back the whole block with `ReadBlock`.
		buf := NewBlockBuf()
		readData, err := sut.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal(data, readData)

		// Write the block again - it should be seen as already existing.
		existed, err = sut.WriteBlock(blockId, data)
		assert.NoError(err)
		assert.Equal(true, existed)
	})

	t.Run("ReadBlock not found", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(nil, "")
		assert.NoError(err)

		buf := NewBlockBuf()
		_, err = sut.ReadBlock(td.BlockId("1"), buf)
		assert.ErrorIs(err, ErrBlockNotFound)
	})

	t.Run("WriteBlock: data length must not exceed limits", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(nil, "")
		assert.NoError(err)
		blockId := td.BlockId("1")
		data := make([]byte, MaxBlockSize+1)
		_, err = sut.WriteBlock(blockId, data)
		assert.Error(err, "is too large")
		_, err = sut.FS.Stat(sut.blockPath(blockId))
		assert.ErrorIs(err, fs.ErrNotExist)
	})
}

func TestBlockBuf(t *testing.T) {
	t.Parallel()

	t.Run("reads short input without error", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)

		buf := NewBlockBuf()
		src := bytes.NewReader([]byte("hello"))

		data, err := buf.Read(src)
		assert.NoError(err)
		assert.Equal([]byte("hello"), data)
		assert.Equal(0, src.Len())
	})
}
