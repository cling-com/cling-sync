package lib

import (
	"bytes"
	"encoding/binary"
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
		names := []string{}
		for _, f := range files {
			names = append(names, f.Name())
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
		repoBlock := Block{
			Header: BlockHeader{
				BlockId:           td.BlockId("1"),
				Flags:             0,
				EncryptedDEK:      td.EncryptedKey("1"),
				EncryptedDataSize: 0, // is set below
			},
			EncryptedData: []byte("repository data"),
		}
		repoBlock.Header.EncryptedDataSize = uint32(len(repoBlock.EncryptedData)) //nolint:gosec
		workspaceBlock := repoBlock
		workspaceBlock.EncryptedData = []byte("workspace data")
		workspaceBlock.Header.EncryptedDataSize = uint32(len(workspaceBlock.EncryptedData)) //nolint:gosec

		ok, err := repo.WriteBlock(repoBlock)
		assert.NoError(err)
		assert.Equal(false, ok)

		ok, err = workspace.WriteBlock(workspaceBlock)
		assert.NoError(err)
		assert.Equal(false, ok)

		data, header, err := repo.ReadBlock(repoBlock.Header.BlockId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(repoBlock.Header, header)
		assert.Equal(repoBlock.EncryptedData, data)

		data, header, err = workspace.ReadBlock(workspaceBlock.Header.BlockId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(workspaceBlock.Header, header)
		assert.Equal(workspaceBlock.EncryptedData, data)
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

		block := Block{
			Header: BlockHeader{
				EncryptedDEK:      td.EncryptedKey("1"),
				BlockId:           td.BlockId("1"),
				Flags:             0xffffffffffffffff,
				EncryptedDataSize: 0, // is set below
			},
			EncryptedData: []byte("block 1 data"),
		}
		block.Header.EncryptedDataSize = uint32(len(block.EncryptedData)) //nolint:gosec
		ok, err := sut.HasBlock(block.Header.BlockId)
		assert.NoError(err)
		assert.Equal(false, ok)

		// Write the block and verify its format on disk.
		existed, err := sut.WriteBlock(block)
		assert.NoError(err)
		assert.Equal(false, existed)

		// Verify size and permissions.
		stat, err := sut.FS.Stat(sut.blockPath(block.Header.BlockId))
		assert.NoError(err)
		assert.Equal(int64(len(block.EncryptedData))+BlockHeaderSize, stat.Size())
		assert.Equal(fs.FileMode(0o400), stat.Mode().Perm())

		f, err := sut.FS.OpenRead(sut.blockPath(block.Header.BlockId))
		assert.NoError(err)
		defer f.Close() //nolint:errcheck
		// Read header.
		// We don't use the const `BlockHeaderSize` here so the test
		// also acts as a kind of regression test.
		headerBuf := make([]byte, 96)
		_, err = f.Read(headerBuf)
		assert.NoError(err)
		header := bytes.NewReader(headerBuf)
		var storageVersion uint16
		err = binary.Read(header, binary.LittleEndian, &storageVersion)
		assert.NoError(err)
		assert.Equal(StorageVersion, storageVersion)
		var flags uint64
		err = binary.Read(header, binary.LittleEndian, &flags)
		assert.NoError(err)
		assert.Equal(block.Header.Flags, flags)
		var encryptedDEK EncryptedKey
		err = binary.Read(header, binary.LittleEndian, &encryptedDEK)
		assert.NoError(err)
		assert.Equal(block.Header.EncryptedDEK, encryptedDEK)
		var dataSize uint32
		err = binary.Read(header, binary.LittleEndian, &dataSize)
		assert.NoError(err)
		assert.Equal(len(block.EncryptedData), int(dataSize))
		// Read data.
		data, err := io.ReadAll(f)
		assert.NoError(err)
		assert.Equal(block.EncryptedData, data)
		_ = f.Close()

		// Now `HasBlock` should return `true`.
		ok, err = sut.HasBlock(block.Header.BlockId)
		assert.NoError(err)
		assert.Equal(true, ok)

		// Read back the whole block with `ReadBlock`.
		readData, readHeader, err := sut.ReadBlock(block.Header.BlockId, BlockBuf{})
		assert.NoError(err)
		assert.Equal(block.Header, readHeader)
		assert.Equal(block.EncryptedData, readData)

		// Write the block again - it should be seen as already existing.
		existed, err = sut.WriteBlock(block)
		assert.NoError(err)
		assert.Equal(true, existed)
	})

	t.Run("ReadBlock and ReadBlockHeader not found", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(nil, "")
		assert.NoError(err)

		_, _, err = sut.ReadBlock(td.BlockId("1"), BlockBuf{})
		assert.ErrorIs(err, ErrBlockNotFound)

		_, err = sut.ReadBlockHeader(td.BlockId("1"))
		assert.ErrorIs(err, ErrBlockNotFound)
	})

	t.Run("WriteBlock: Block.Data length must not exceed limits", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(nil, "")
		assert.NoError(err)
		block := Block{
			Header: BlockHeader{
				EncryptedDEK:      td.EncryptedKey("1"),
				BlockId:           td.BlockId("1"),
				Flags:             0,
				EncryptedDataSize: 0, // is set below
			},
			EncryptedData: make([]byte, MaxEncryptedBlockDataSize+1),
		}
		block.Header.EncryptedDataSize = uint32(len(block.EncryptedData)) //nolint:gosec
		_, err = sut.WriteBlock(block)
		assert.Error(err, "block data too large")
		_, err = sut.FS.Stat(sut.blockPath(block.Header.BlockId))
		assert.ErrorIs(err, fs.ErrNotExist)
	})
}

func TestBlockHeaderMarshalUnmarshal(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		header := BlockHeader{
			EncryptedDEK:      td.EncryptedKey("1"),
			BlockId:           td.BlockId("1"),
			Flags:             0,
			EncryptedDataSize: 1234,
		}
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, header)
		assert.NoError(err)
		var header2 BlockHeader
		err = binary.Read(buf, binary.LittleEndian, &header2)
		assert.NoError(err)
		assert.Equal(header, header2)
	})
}
