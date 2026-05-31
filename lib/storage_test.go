package lib

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	mrand "math/rand/v2"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestFileStorageInit(t *testing.T) {
	t.Parallel()
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(t.Context(), Toml{"encryption": {"version": "1"}}, "header comment")
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
		err = sut.Init(t.Context(), Toml{"encryption": {"version": "1"}}, "header comment")
		assert.NoError(err)
		err = sut.Init(t.Context(), Toml{"encryption": {"version": "1"}}, "header comment")
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
		err = sut.Init(t.Context(), Toml{"encryption": {"version": "1"}}, "header comment")
		assert.NoError(err)

		sut, err = NewFileStorage(fs, StoragePurposeRepository)
		assert.NoError(err)
		toml, err := sut.Open(t.Context())
		assert.NoError(err)
		assert.Equal(Toml{"encryption": {"version": "1"}}, toml)
	})

	t.Run("Repository does not exist", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		_, err = sut.Open(t.Context())
		assert.ErrorIs(err, ErrStorageNotFound)
	})

	t.Run("Repository config file is missing", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		fs := td.NewFS(t)
		sut, err := NewFileStorage(fs, StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(t.Context(), nil, "")
		assert.NoError(err)
		err = fs.Remove(filepath.Join(".cling", "repository.txt"))
		assert.NoError(err)

		sut, err = NewFileStorage(fs, StoragePurposeRepository)
		assert.NoError(err)
		_, err = sut.Open(t.Context())
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
		err = repo.Init(t.Context(), Toml{"repository": {"version": "1"}}, "repository header comment")
		assert.NoError(err)
		err = workspace.Init(t.Context(), Toml{"workspace": {"version": "1"}}, "workspace header comment")
		assert.NoError(err)
	})

	t.Run("Control files", func(t *testing.T) { //nolint:paralleltest
		hasControlFile, err := repo.HasControlFile(t.Context(), ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(false, hasControlFile)

		err = repo.WriteControlFile(t.Context(), ControlFileSectionRefs, "head", []byte("1234"))
		assert.NoError(err)
		hasControlFile, err = repo.HasControlFile(t.Context(), ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(true, hasControlFile)
		err = workspace.WriteControlFile(t.Context(), ControlFileSectionRefs, "head", []byte("5678"))
		assert.NoError(err)

		// Verify permissions.
		stat, err := commonFS.Stat(filepath.Join(".cling", "repository", "refs", "head"))
		assert.NoError(err)
		assert.Equal(fs.FileMode(0o600), stat.Mode().Perm())

		repoCtrlContent, err := repo.ReadControlFile(t.Context(), ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("1234"), repoCtrlContent)
		workspaceCtrlContent, err := workspace.ReadControlFile(t.Context(), ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("5678"), workspaceCtrlContent)

		err = repo.DeleteControlFile(t.Context(), ControlFileSectionRefs, "head")
		assert.NoError(err)
		hasControlFile, err = repo.HasControlFile(t.Context(), ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(false, hasControlFile)
	})

	t.Run("ReadControlFile should return ErrControlFileNotFound", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		_, err = sut.ReadControlFile(t.Context(), ControlFileSectionRefs, "head")
		assert.ErrorIs(err, ErrControlFileNotFound)
	})

	t.Run("DeleteControlFile should return ErrControlFileNotFound", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.DeleteControlFile(t.Context(), ControlFileSectionRefs, "head")
		assert.ErrorIs(err, ErrControlFileNotFound)
	})

	t.Run("Read and write block", func(t *testing.T) { //nolint:paralleltest
		// Write a block with the same block id for each purpose.
		repoBlock := []byte("a repo block")
		workspaceBlock := []byte("a workspace block")
		blockId := td.BlockId("1")

		ok, err := repo.WriteBlock(t.Context(), blockId, repoBlock)
		assert.NoError(err)
		assert.Equal(false, ok)

		ok, err = workspace.WriteBlock(t.Context(), blockId, workspaceBlock)
		assert.NoError(err)
		assert.Equal(false, ok)

		buf := NewBlockBuf()
		data, err := repo.ReadBlock(t.Context(), blockId, buf)
		assert.NoError(err)
		assert.Equal(repoBlock, data)

		data, err = workspace.ReadBlock(t.Context(), blockId, buf)
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
		err = sut.Init(t.Context(), nil, "")
		assert.NoError(err)

		blockId := td.BlockId("1")
		data := []byte("block 1 data")
		ok, err := sut.HasBlock(t.Context(), blockId)
		assert.NoError(err)
		assert.Equal(false, ok)

		// Write the block and verify its format on disk.
		existed, err := sut.WriteBlock(t.Context(), blockId, data)
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
		ok, err = sut.HasBlock(t.Context(), blockId)
		assert.NoError(err)
		assert.Equal(true, ok)

		// Read back the whole block with `ReadBlock`.
		buf := NewBlockBuf()
		readData, err := sut.ReadBlock(t.Context(), blockId, buf)
		assert.NoError(err)
		assert.Equal(data, readData)

		// Write the block again - it should be seen as already existing.
		existed, err = sut.WriteBlock(t.Context(), blockId, data)
		assert.NoError(err)
		assert.Equal(true, existed)
	})

	t.Run("ReadBlock not found", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(t.Context(), nil, "")
		assert.NoError(err)

		buf := NewBlockBuf()
		_, err = sut.ReadBlock(t.Context(), td.BlockId("1"), buf)
		assert.ErrorIs(err, ErrBlockNotFound)
	})

	t.Run("ReadBlockIds", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(t.Context(), nil, "")
		assert.NoError(err)

		blockId1 := td.BlockId("1")
		blockId2 := td.BlockId("2")
		_, err = sut.WriteBlock(t.Context(), blockId2, []byte("block 2"))
		assert.NoError(err)
		_, err = sut.WriteBlock(t.Context(), blockId1, []byte("block 1"))
		assert.NoError(err)

		// Simulate a crash leaving behind AtomicWriteFile's temporary file.
		f, err := sut.FS.OpenWrite(AtomicWriteTempFilename(sut.blockPath(blockId1)))
		assert.NoError(err)
		_, err = f.Write([]byte("temp"))
		assert.NoError(err)
		assert.NoError(f.Close())

		blockIds := []BlockId{}
		err = sut.ReadBlockIds(t.Context(), func(blockId BlockId) bool {
			blockIds = append(blockIds, blockId)
			return true
		})
		assert.NoError(err)
		slices.SortFunc(blockIds, func(a, b BlockId) int {
			return strings.Compare(a.String(), b.String())
		})
		assert.Equal([]BlockId{blockId1, blockId2}, blockIds)
	})

	t.Run("WriteBlock: data length must not exceed limits", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(t.Context(), nil, "")
		assert.NoError(err)
		blockId := td.BlockId("1")
		data := make([]byte, MaxBlockSize+1)
		_, err = sut.WriteBlock(t.Context(), blockId, data)
		assert.Error(err, "is too large")
		_, err = sut.FS.Stat(sut.blockPath(blockId))
		assert.ErrorIs(err, fs.ErrNotExist)
	})

	t.Run("ReadControlFile enforces the MaxControlFileSize boundary", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(t.Context(), nil, "")
		assert.NoError(err)
		// Write a file at the limit through the regular API and confirm it reads back.
		err = sut.WriteControlFile(t.Context(), ControlFileSectionRefs, "head", make([]byte, MaxControlFileSize))
		assert.NoError(err)
		data, err := sut.ReadControlFile(t.Context(), ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(MaxControlFileSize, len(data))
		// Simulate a hostile backend by writing one byte over the limit directly.
		path, err := sut.controlFilePath(ControlFileSectionRefs, "head2")
		assert.NoError(err)
		err = sut.FS.MkdirAll(filepath.Dir(path))
		assert.NoError(err)
		err = WriteFile(sut.FS, path, make([]byte, MaxControlFileSize+1))
		assert.NoError(err)
		_, err = sut.ReadControlFile(t.Context(), ControlFileSectionRefs, "head2")
		assert.Error(err, "exceeds maximum control file size")
	})

	t.Run("WriteControlFile enforces the MaxControlFileSize boundary", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
		assert.NoError(err)
		err = sut.Init(t.Context(), nil, "")
		assert.NoError(err)
		// Exactly MaxControlFileSize must be accepted.
		err = sut.WriteControlFile(t.Context(), ControlFileSectionRefs, "head", make([]byte, MaxControlFileSize))
		assert.NoError(err)
		// One byte over must be rejected, and must not leave a half-written file.
		err = sut.WriteControlFile(t.Context(), ControlFileSectionRefs, "head2", make([]byte, MaxControlFileSize+1))
		assert.Error(err, "is too large")
		has, err := sut.HasControlFile(t.Context(), ControlFileSectionRefs, "head2")
		assert.NoError(err)
		assert.Equal(false, has)
	})
}

func TestFileStorageConcurrency(t *testing.T) {
	t.Parallel()
	assert := NewAssert(t)
	sut, err := NewFileStorage(td.NewFS(t), StoragePurposeRepository)
	assert.NoError(err)
	assert.NoError(sut.Init(t.Context(), Toml{"encryption": {"version": "1"}}, ""))

	// Fresh blocks per round so writes never dedup. Pool < workers for overlap.
	const (
		poolSize = 4
		rounds   = 20
	)
	poolIDs := make([][]BlockId, rounds)
	poolData := make([][][]byte, rounds)
	for round := range rounds {
		poolIDs[round] = make([]BlockId, poolSize)
		poolData[round] = make([][]byte, poolSize)
		for j := range poolSize {
			key := strconv.Itoa(round) + "-" + strconv.Itoa(j)
			poolIDs[round][j] = td.BlockId("pool-" + key)
			poolData[round][j] = []byte("pool " + key)
		}
	}

	// Each goroutine also runs the other methods on its own keys.
	const workers = 16
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = func() error {
				id := td.BlockId(strconv.Itoa(i))
				name := "cf" + strconv.Itoa(i)
				lockName := "lock" + strconv.Itoa(i)
				want := []byte("data " + strconv.Itoa(i))
				buf := NewBlockBuf()
				if _, err := sut.WriteBlock(t.Context(), id, want); err != nil {
					return err
				}
				if _, err := sut.HasBlock(t.Context(), id); err != nil {
					return err
				}
				got, err := sut.ReadBlock(t.Context(), id, buf)
				if err != nil {
					return err
				}
				if string(got) != string(want) {
					return Errorf("block %d: read %q, want %q", i, got, want)
				}
				if err := sut.ReadBlockIds(t.Context(), func(BlockId) bool { return true }); err != nil {
					return err
				}
				if _, err := sut.Open(t.Context()); err != nil {
					return err
				}
				if err := sut.WriteControlFile(t.Context(), ControlFileSectionRefs, name, want); err != nil {
					return err
				}
				if _, err := sut.HasControlFile(t.Context(), ControlFileSectionRefs, name); err != nil {
					return err
				}
				cf, err := sut.ReadControlFile(t.Context(), ControlFileSectionRefs, name)
				if err != nil {
					return err
				}
				if string(cf) != string(want) {
					return Errorf("control file %s: read %q, want %q", name, cf, want)
				}
				if err := sut.DeleteControlFile(t.Context(), ControlFileSectionRefs, name); err != nil {
					return err
				}

				// Stress the write path. The atomic-write race shows up here.
				for round := range rounds {
					j := mrand.IntN(poolSize)
					if _, err := sut.WriteBlock(t.Context(), poolIDs[round][j], poolData[round][j]); err != nil {
						return err
					}
					got, err := sut.ReadBlock(t.Context(), poolIDs[round][j], buf)
					if err != nil {
						return err
					}
					if string(got) != string(poolData[round][j]) {
						return Errorf("pool %d-%d: read %q, want %q", round, j, got, poolData[round][j])
					}
				}

				// Acquire, then drop via ForceUnlock so both run.
				if _, err := sut.Lock(context.Background(), lockName); err != nil {
					return err
				}
				return sut.ForceUnlock(t.Context(), lockName)
			}()
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		assert.NoError(err)
	}
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

	t.Run("enforces the MaxBlockSize boundary", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		buf := NewBlockBuf()
		// Exactly MaxBlockSize must be accepted.
		data, err := buf.Read(bytes.NewReader(make([]byte, MaxBlockSize)))
		assert.NoError(err)
		assert.Equal(MaxBlockSize, len(data))
		// One byte over must be rejected.
		_, err = buf.Read(bytes.NewReader(make([]byte, MaxBlockSize+1)))
		assert.Error(err, "exceeds maximum block size")
	})
}
