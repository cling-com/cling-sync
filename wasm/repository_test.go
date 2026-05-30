//go:build !wasm

// Native entry point for the repository checks (see `testgo.go`).

package main

import (
	"net/http"
	"testing"

	clingHTTP "github.com/flunderpero/cling-sync/http"
	"github.com/flunderpero/cling-sync/lib"
)

var td = lib.TestData{} //nolint:gochecknoglobals

const (
	wasmTestAccessKey = "test-access-key"
	wasmTestSecret    = "test-secret-key"
	wasmTestRegion    = "us-east-1"
	wasmTestAddress   = "127.0.0.1:9123"
)

func TestWasm(t *testing.T) {
	t.Parallel()
	fs := td.NewRealFS(t)
	r := td.NewTestRepository(t, fs)

	mux := http.NewServeMux()
	clingHTTP.NewS3StorageServer(r.Storage, wasmTestRegion, wasmTestAccessKey, wasmTestSecret).
		RegisterRoutes(mux)
	server := &http.Server{Addr: wasmTestAddress, Handler: mux} //nolint:exhaustruct
	t.Cleanup(func() { _ = server.Close() })                    // outlives the parallel compiler subtests
	go server.ListenAndServe()                                  //nolint:errcheck

	// `wasm/testdata.go` sets the passphrase the test repository uses. The
	// wasm side decodes the encrypted URI with the same passphrase.
	encryptedURI, err := clingHTTP.EncodeS3URI(
		"s3+http://"+wasmTestAddress,
		clingHTTP.S3Credentials{AccessKeyID: wasmTestAccessKey, SecretAccessKey: []byte(wasmTestSecret)},
		[]byte("testpassphrase"),
	)
	if err != nil {
		t.Fatal(err)
	}
	RunWasmTests(t, "checkrepo", "WASM_S3_URL="+encryptedURI)
}
