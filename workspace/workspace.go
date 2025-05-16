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
}

var ErrNoWorkspace = lib.Errorf("no workspace found")

// Load the configuration from `./.cling/workspace.txt`.
func OpenWorkspace(path string) (*Workspace, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to get absolute path for %s", path)
	}
	configPath := filepath.Join(path, ".cling", "workspace.txt")
	r, err := os.Open(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNoWorkspace
		}
		return nil, lib.WrapErrorf(err, "failed to open workspace config file %s", configPath)
	}
	defer r.Close() //nolint:errcheck
	toml, err := lib.ReadToml(r)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read workspace config file %s", configPath)
	}
	remoteRepository, ok := toml.GetValue("remote", "repository")
	if !ok {
		return nil, lib.Errorf("invalid repository config, key `remote.repository` not found")
	}
	return &Workspace{RemoteRepository: RemoteRepository(remoteRepository), WorkspacePath: path}, nil
}

// Create a new workspace. Tests whether we are already inside a workspace or repository
// (i.e. if a `.cling` directory exists up the path).
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
	p := path
	lastP := ""
	for p != lastP {
		if _, err := os.Stat(filepath.Join(p, ".cling")); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				lastP = p
				p = filepath.Dir(p)
				continue
			}
			return nil, lib.WrapErrorf(err, "failed to check for existing workspace at %s", p)
		}
		return nil, lib.Errorf("a workspace already exists at %s", p)
	}
	toml := lib.Toml{
		"remote": {
			"repository": string(remoteRepository),
		},
	}
	configPath := filepath.Join(path, ".cling", "workspace.txt")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		return nil, lib.WrapErrorf(err, "failed to create workspace config directory %s", filepath.Dir(configPath))
	}
	w, err := os.OpenFile(configPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open workspace config file %s", configPath)
	}
	defer w.Close() //nolint:errcheck
	headerComment := strings.Trim(`
DO NOT DELETE OR CHANGE THIS FILE.

This file contains the configuration of your cling workspace.
`, "\n ")
	if err := lib.WriteToml(w, headerComment, toml); err != nil {
		return nil, lib.WrapErrorf(err, "failed to write workspace config file %s", configPath)
	}
	if err := w.Close(); err != nil {
		return nil, lib.WrapErrorf(err, "failed to close workspace config file %s", configPath)
	}
	return &Workspace{RemoteRepository: remoteRepository, WorkspacePath: path}, nil
}
