//go:build !wasm

package workspace

import (
	clingHTTP "github.com/flunderpero/cling-sync/http"
	"github.com/flunderpero/cling-sync/lib"
)

// OpenStorage opens a repository storage by URI. `s3+<http-url>` URIs need
// the repository passphrase to decrypt the embedded credentials. Local paths
// ignore the passphrase.
func OpenStorage(uri string, passphrase []byte) (lib.Storage, error) {
	if clingHTTP.IsS3StorageURI(uri) {
		if passphrase == nil {
			return nil, lib.Errorf("S3 storage URI requires a passphrase")
		}
		cfg, _, err := clingHTTP.DecodeS3URI(uri, passphrase)
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to decode S3 URI")
		}
		return clingHTTP.NewS3StorageClient(cfg, clingHTTP.NewDefaultHTTPClient(nil)), nil
	}
	storage, err := lib.NewFileStorage(lib.NewRealFS(uri), lib.StoragePurposeRepository)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open storage")
	}
	return storage, nil
}
