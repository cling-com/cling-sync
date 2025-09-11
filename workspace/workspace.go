package workspace

import (
	"bytes"
	cryptoCipher "crypto/cipher"
	"errors"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

type RemoteRepository string

type Workspace struct {
	RemoteRepository RemoteRepository
	PathPrefix       lib.Path
	Storage          lib.Storage
	FS               lib.FS
	TempFS           lib.FS
}

// Load the configuration from `<fs>/.cling/workspace.txt`.
func OpenWorkspace(fs lib.FS, tempFS lib.FS) (*Workspace, error) {
	storage, err := lib.NewFileStorage(fs, lib.StoragePurposeWorkspace)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create storage for workspace at %s", fs)
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
	var pathPrefix lib.Path
	if pathPrefixStr, ok := toml.GetValue("remote", "path-prefix"); ok {
		pathPrefix, err = ValidatePathPrefix(pathPrefixStr)
		if err != nil {
			return nil, lib.WrapErrorf(err, "invalid path prefix %q", pathPrefix)
		}
	}
	return &Workspace{RemoteRepository(remoteRepository), pathPrefix, storage, fs, tempFS}, nil
}

// Create a new workspace. Workspaces can be nested, i.e. a workspace can be inside another workspace.
func NewWorkspace(
	fs lib.FS,
	tempFS lib.FS,
	remoteRepository RemoteRepository,
	pathPrefix lib.Path,
) (*Workspace, error) {
	toml := lib.Toml{
		"remote": {
			"repository": string(remoteRepository),
		},
	}
	if !pathPrefix.IsEmpty() {
		toml["remote"]["path-prefix"] = pathPrefix.String() + "/"
	}
	headerComment := strings.Trim(`
DO NOT DELETE OR CHANGE THIS FILE.

This file contains the configuration of your cling workspace.
`, "\n ")
	storage, err := lib.NewFileStorage(fs, lib.StoragePurposeWorkspace)
	if err != nil {
		if errors.Is(err, lib.ErrStorageAlreadyExists) {
			return nil, lib.ErrStorageAlreadyExists
		}
		return nil, lib.WrapErrorf(err, "failed to create storage for workspace at %s", fs)
	}
	if err := storage.Init(toml, headerComment); err != nil {
		return nil, lib.WrapErrorf(err, "failed to create workspace")
	}
	if err := lib.WriteRef(storage, "head", lib.RevisionId{}); err != nil {
		return nil, lib.WrapErrorf(err, "failed to write workspace head reference")
	}
	return &Workspace{remoteRepository, pathPrefix, storage, fs, tempFS}, nil
}

// Remove `w.TempFS`.
func (w *Workspace) Close() error {
	if err := w.TempFS.RemoveAll("."); err != nil {
		return lib.WrapErrorf(err, "failed to remove temporary fs %s", w.TempFS)
	}
	return nil
}

func (w *Workspace) Head() (lib.RevisionId, error) {
	ref, err := lib.ReadRef(w.Storage, "head")
	if err != nil {
		return lib.RevisionId{}, lib.WrapErrorf(err, "failed to read head reference")
	}
	return ref, nil
}

var ErrRepositoryKeysNotFound = lib.Errorf("repository keys not found")

const repositoryKeysFileName = "keys.enc"

func (w *Workspace) WriteRepositoryKeys(keys *lib.RepositoryKeys, cipher cryptoCipher.AEAD) error {
	marshalBuf := bytes.NewBuffer(nil)
	if err := lib.MarshalRepositoryKeys(keys, marshalBuf); err != nil {
		return lib.WrapErrorf(err, "failed to serialize repository keys")
	}
	encBuf := make([]byte, lib.TotalCipherOverhead+marshalBuf.Len())
	encrypted, err := lib.Encrypt(marshalBuf.Bytes(), cipher, []byte(repositoryKeysFileName), encBuf)
	if err != nil {
		return lib.WrapErrorf(err, "failed to encrypt repository keys")
	}
	if err := w.Storage.WriteControlFile(lib.ControlFileSectionSecurity, repositoryKeysFileName, encrypted); err != nil {
		return lib.WrapErrorf(err, "failed to copy repository keys to local storage")
	}
	return nil
}

func (w *Workspace) HasRepositoryKeys() bool {
	ok, err := w.Storage.HasControlFile(lib.ControlFileSectionSecurity, repositoryKeysFileName)
	if err != nil {
		return false
	}
	return ok
}

func (w *Workspace) ReadRepositoryKeys(cipher cryptoCipher.AEAD) (*lib.RepositoryKeys, error) {
	ok, err := w.Storage.HasControlFile(lib.ControlFileSectionSecurity, repositoryKeysFileName)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to check if repository keys exist")
	}
	if !ok {
		return nil, ErrRepositoryKeysNotFound
	}
	encrypted, err := w.Storage.ReadControlFile(lib.ControlFileSectionSecurity, repositoryKeysFileName)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read repository keys")
	}
	decrypted, err := lib.Decrypt(encrypted, cipher, []byte(repositoryKeysFileName), make([]byte, len(encrypted)))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to decrypt repository keys")
	}
	keys, err := lib.UnmarshalRepositoryKeys(bytes.NewReader(decrypted))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to parse repository keys")
	}
	return keys, nil
}

func (w *Workspace) DeleteRepositoryKeys() error {
	if err := w.Storage.DeleteControlFile(lib.ControlFileSectionSecurity, repositoryKeysFileName); err != nil {
		return lib.WrapErrorf(err, "failed to delete local repository keys")
	}
	return nil
}

func ValidatePathPrefix(pathPrefix string) (lib.Path, error) {
	if pathPrefix == "" {
		return lib.Path{}, nil
	}
	if strings.HasPrefix(pathPrefix, "/") {
		return lib.Path{}, lib.Errorf("invalid path prefix %q, must not start with `/`", pathPrefix)
	}
	if !strings.HasSuffix(pathPrefix, "/") {
		return lib.Path{}, lib.Errorf("invalid path prefix %q, must end with `/`", pathPrefix)
	}
	return lib.NewPath(pathPrefix[:len(pathPrefix)-1]) //nolint:wrapcheck
}
