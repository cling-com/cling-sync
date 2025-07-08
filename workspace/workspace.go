package workspace

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

type RemoteRepository string

type Workspace struct {
	RemoteRepository RemoteRepository
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
	return &Workspace{RemoteRepository(remoteRepository), storage, fs, tempFS}, nil
}

// Create a new workspace. Workspaces can be nested, i.e. a workspace can be inside another workspace.
func NewWorkspace(fs lib.FS, tempFS lib.FS, remoteRepository RemoteRepository) (*Workspace, error) {
	toml := lib.Toml{
		"remote": {
			"repository": string(remoteRepository),
		},
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
	return &Workspace{remoteRepository, storage, fs, tempFS}, nil
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

func (w *Workspace) WriteRepositoryKeys(keys *lib.RepositoryKeys) error {
	toml := lib.Toml{
		"encryption": {
			"version":       fmt.Sprintf("%d", lib.EncryptionVersion),
			"kek":           lib.FormatRecoveryCode(keys.KEK[:]),
			"block-id-hmac": lib.FormatRecoveryCode(keys.BlockIdHmacKey[:]),
		},
	}
	headerComment := strings.Trim(`
This file contains the encryption keys used to encrypt/decrypt the repository.

WARNING: These are the raw keys, so anyone with access to this file can decrypt
the whole repository or make changes to it.

IT IS SAVE TO DELETE THIS FILE. 
If you do so, you either must provide your passphrase to every command or save the keys again.
`, "\n ")
	var sb bytes.Buffer
	if err := lib.WriteToml(&sb, headerComment, toml); err != nil {
		return lib.WrapErrorf(err, "failed to serialize repository keys to local storage")
	}
	if err := w.Storage.WriteControlFile(lib.ControlFileSectionSecurity, "keys.toml", sb.Bytes()); err != nil {
		return lib.WrapErrorf(err, "failed to copy repository keys to local storage")
	}
	return nil
}

func (w *Workspace) ReadRepositoryKeys() (*lib.RepositoryKeys, bool, error) {
	ok, err := w.Storage.HasControlFile(lib.ControlFileSectionSecurity, "keys.toml")
	if err != nil {
		return nil, false, lib.WrapErrorf(err, "failed to check if repository keys exist")
	}
	if !ok {
		return nil, false, nil
	}
	rawToml, err := w.Storage.ReadControlFile(lib.ControlFileSectionSecurity, "keys.toml")
	if err != nil {
		return nil, false, lib.WrapErrorf(err, "failed to read repository keys")
	}
	toml, err := lib.ReadToml(bytes.NewReader(rawToml))
	if err != nil {
		return nil, false, lib.WrapErrorf(err, "failed to parse repository keys")
	}
	i, ok := toml.GetIntValue("encryption", "version")
	if !ok {
		return nil, false, lib.Errorf("missing or invalid key `encryption.version` in repository keys")
	}
	if i != int(lib.EncryptionVersion) {
		return nil, false, lib.Errorf(
			"unsupported repository keys version %d, want %d",
			i,
			lib.EncryptionVersion,
		)
	}
	value, ok := toml.GetValue("encryption", "kek")
	if !ok {
		return nil, false, lib.Errorf("missing key `encryption.kek` in repository keys")
	}
	kek, err := lib.ParseRecoveryCode(value)
	if err != nil {
		return nil, false, lib.WrapErrorf(err, "invalid key `encryption.kek` in repository keys")
	}
	value, ok = toml.GetValue("encryption", "block-id-hmac")
	if !ok {
		return nil, false, lib.Errorf("missing key `encryption.block-id-hmac` in repository keys")
	}
	blockIdHmacKey, err := lib.ParseRecoveryCode(value)
	if err != nil {
		return nil, false, lib.WrapErrorf(
			err,
			"invalid key `encryption.block-id-hmac` in repository keys",
		)
	}
	return &lib.RepositoryKeys{
		KEK:            lib.RawKey(kek),
		BlockIdHmacKey: lib.RawKey(blockIdHmacKey),
	}, true, nil
}

func (w *Workspace) DeleteRepositoryKeys() error {
	if err := w.Storage.DeleteControlFile(lib.ControlFileSectionSecurity, "keys.toml"); err != nil {
		return lib.WrapErrorf(err, "failed to delete local repository keys")
	}
	return nil
}
