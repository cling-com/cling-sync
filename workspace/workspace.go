package workspace

import (
	cryptoCipher "crypto/cipher"
	"errors"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

const workspaceDir = ".cling/workspace"

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
DO NOT DELETE OR MODIFY THIS FILE.

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

var ErrSavedPassphraseNotFound = lib.Errorf("saved passphrase not found")

const savedPassphraseFileName = "passphrase.enc"

// WriteSavedPassphrase AEAD-encrypts `passphrase` with `cipher` and stores
// the ciphertext as a workspace control file. The encryption key (which
// `cipher` was built from) is meant to live in the system keychain; the
// two-layer scheme means neither alone unlocks the repository.
func (w *Workspace) WriteSavedPassphrase(passphrase []byte, cipher cryptoCipher.AEAD) error {
	encrypted := make([]byte, len(passphrase)+lib.TotalCipherOverhead)
	if _, err := lib.Encrypt(passphrase, cipher, []byte(savedPassphraseFileName), encrypted); err != nil {
		return lib.WrapErrorf(err, "failed to encrypt saved passphrase")
	}
	if err := w.Storage.WriteControlFile(
		lib.ControlFileSectionSecurity,
		savedPassphraseFileName,
		encrypted,
	); err != nil {
		return lib.WrapErrorf(err, "failed to write saved passphrase")
	}
	return nil
}

func (w *Workspace) HasSavedPassphrase() bool {
	ok, err := w.Storage.HasControlFile(lib.ControlFileSectionSecurity, savedPassphraseFileName)
	if err != nil {
		return false
	}
	return ok
}

func (w *Workspace) ReadSavedPassphrase(cipher cryptoCipher.AEAD) ([]byte, error) {
	ok, err := w.Storage.HasControlFile(lib.ControlFileSectionSecurity, savedPassphraseFileName)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to check for saved passphrase")
	}
	if !ok {
		return nil, ErrSavedPassphraseNotFound
	}
	encrypted, err := w.Storage.ReadControlFile(lib.ControlFileSectionSecurity, savedPassphraseFileName)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read saved passphrase")
	}
	plaintext, err := lib.Decrypt(encrypted, cipher, []byte(savedPassphraseFileName), make([]byte, len(encrypted)))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to decrypt saved passphrase")
	}
	return plaintext, nil
}

func (w *Workspace) DeleteSavedPassphrase() error {
	if err := w.Storage.DeleteControlFile(lib.ControlFileSectionSecurity, savedPassphraseFileName); err != nil {
		if errors.Is(err, lib.ErrControlFileNotFound) {
			return nil
		}
		return lib.WrapErrorf(err, "failed to delete saved passphrase")
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
