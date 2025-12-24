//go:build !wasm

package main

import (
	"net/http"
	"testing"

	clingHTTP "github.com/flunderpero/cling-sync/http"
	"github.com/flunderpero/cling-sync/lib"
)

var td = lib.TestData{} //nolint:gochecknoglobals

func TestWasm(t *testing.T) {
	t.Parallel()

	// Create a test repository and serve it over HTTP.
	fs := td.NewRealFS(t)
	r := td.NewTestRepository(t, fs)
	storageServer := clingHTTP.NewHTTPStorageServer(r.Storage, "127.0.0.1:9123")
	mux := http.NewServeMux()
	storageServer.RegisterRoutes(mux)
	server := &http.Server{Addr: "127.0.0.1:9123", Handler: mux} //nolint:gosec,exhaustruct
	defer server.Close()                                         //nolint:errcheck
	go server.ListenAndServe()                                   //nolint:errcheck

	RunWasmTests(t, "./repository.go", "./repository_check.go", "./js.go")
}
