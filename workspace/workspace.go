package workspace

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

type RemoteRepository string

type Workspace struct {
	RemoteRepository RemoteRepository
	WorkspacePath    string
	storage          lib.Storage
	tmpDir           string
}

// Load the configuration from `./.cling/workspace.txt`.
func OpenWorkspace(path string) (*Workspace, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get absolute path for %s", path)
	}
	storage, err := lib.NewFileStorage(path, lib.StoragePurposeWorkspace)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create storage for workspace at %s", path)
	}
	toml, err := storage.Open()
	if err != nil {
		if errors.Is(err, lib.ErrStorageNotFound) {
			return nil, lib.ErrStorageNotFound
		}
		return nil, lib.WrapErrorf(err, "failed to open workspace")
	}
	remoteRepository, ok := toml.GetValue("remote", "repository")
	if !ok {
		return nil, lib.Errorf("invalid repository config, key `remote.repository` not found")
	}
	tmpDir, err := os.MkdirTemp(os.TempDir(), "cling-sync-workspace")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary directory")
	}
	return &Workspace{RemoteRepository(remoteRepository), path, storage, tmpDir}, nil
}

// Create a new workspace. Workspaces can be nested, i.e. a workspace can be inside another workspace.
func NewWorkspace(path string, remoteRepository RemoteRepository) (*Workspace, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get absolute path for %s", path)
	}
	// For now, remote repository is always a file path on the local machine.
	remoteRepositoryAbs, err := filepath.Abs(string(remoteRepository))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get absolute path for %s", remoteRepository)
	}
	remoteRepository = RemoteRepository(remoteRepositoryAbs)
	toml := lib.Toml{
		"remote": {
			"repository": string(remoteRepository),
		},
	}
	headerComment := strings.Trim(`
DO NOT DELETE OR CHANGE THIS FILE.

This file contains the configuration of your cling workspace.
`, "\n ")
	storage, err := lib.NewFileStorage(path, lib.StoragePurposeWorkspace)
	if err != nil {
		if errors.Is(err, lib.ErrStorageAlreadyExists) {
			return nil, lib.ErrStorageAlreadyExists
		}
		return nil, lib.WrapErrorf(err, "failed to create storage for workspace at %s", path)
	}
	if err := storage.Init(toml, headerComment); err != nil {
		return nil, lib.WrapErrorf(err, "failed to create workspace")
	}
	tmpDir, err := os.MkdirTemp(os.TempDir(), "cling-sync-workspace")
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create temporary directory")
	}
	return &Workspace{remoteRepository, path, storage, tmpDir}, nil
}

func (w *Workspace) NewTmpDir(name string) (string, error) {
	tmpDir, err := os.MkdirTemp(w.tmpDir, name)
	if err != nil {
		return "", lib.WrapErrorf(err, "failed to create temporary directory")
	}
	return tmpDir, nil
}

func (w *Workspace) Close() error {
	if err := os.RemoveAll(w.tmpDir); err != nil {
		return lib.WrapErrorf(err, "failed to remove temporary directory %s", w.tmpDir)
	}
	return nil
}
