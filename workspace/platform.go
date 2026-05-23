//go:build !wasm

package workspace

import (
	"net/http"

	clingHTTP "github.com/flunderpero/cling-sync/http"
	"github.com/flunderpero/cling-sync/lib"
)

// OpenStorage opens a repository storage by URI. `uri` is either an HTTP(S)
// URL pointing at a `cling-sync serve` instance or a local filesystem path.
func OpenStorage(uri string) (lib.Storage, error) {
	if clingHTTP.IsHTTPStorageUIR(uri) {
		return clingHTTP.NewHTTPStorageClient(
			uri,
			clingHTTP.NewDefaultHTTPClient(http.DefaultClient),
		), nil
	}
	storage, err := lib.NewFileStorage(lib.NewRealFS(uri), lib.StoragePurposeRepository)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open storage")
	}
	return storage, nil
}
