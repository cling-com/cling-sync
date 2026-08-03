//go:build !wasm

// Native entry point for the repository checks (see `driver.go`).

package wasmtest

import (
	"net/http"
	"testing"

	clingHTTP "github.com/cling-com/cling-sync/http"
	"github.com/cling-com/cling-sync/lib"
	"github.com/cling-com/cling-sync/workspace"
)

var (
	td   = lib.TestData{}                //nolint:gochecknoglobals
	wstd = workspace.WorkspaceTestData{} //nolint:gochecknoglobals
)

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

	// Seed a revision so the wasm side has something to list.
	w := wstd.NewTestWorkspace(t, r.Repository)
	w.Write("a.txt", "a")
	w.Write("skip.log", "log")
	head, err := workspace.Merge(t.Context(), w.Workspace, r.Repository, wstd.MergeOptions())
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	clingHTTP.NewS3StorageServer(r.Storage, wasmTestRegion, wasmTestAccessKey, wasmTestSecret).
		RegisterRoutes(mux)
	server := &http.Server{Addr: wasmTestAddress, Handler: mux} //nolint:exhaustruct
	t.Cleanup(func() { _ = server.Close() })                    // outlives the parallel compiler subtests
	go server.ListenAndServe()                                  //nolint:errcheck

	// `workspace/testdata.go` sets the passphrase the test repository uses. The
	// wasm side decodes the encrypted URI with the same passphrase.
	encryptedURI, err := clingHTTP.EncodeS3URI(
		"s3+http://"+wasmTestAddress,
		clingHTTP.S3Credentials{AccessKeyID: wasmTestAccessKey, SecretAccessKey: []byte(wasmTestSecret)},
		[]byte("testpassphrase"),
	)
	if err != nil {
		t.Fatal(err)
	}
	RunWasmTests(t, "../checkrepo", "WASM_S3_URL="+encryptedURI, "WASM_HEAD_REVISION="+head.String())
}
