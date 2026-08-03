//go:build wasm

// Checks for the repository API. See `wasm/internal/wasmtest/driver.go` for the
// wasm test system.

package main

import (
	"fmt"
	"strings"
	"syscall/js"

	"github.com/cling-com/cling-sync/wasm"
	"github.com/cling-com/cling-sync/wasm/internal/wasmtest"
)

func main() {
	// Keep the program running, because the Wasm module is unloaded when `main` returns.
	select {}
}

//go:wasmexport registerTests
func registerTests() { //nolint:unused // Called from JS once the module is instantiated.
	wasmtest.ExportRunTests()
}

func init() { //nolint:gochecknoinits
	wasmtest.RegisterTest("Happy path", TestHappyPath)
	wasmtest.RegisterTest("Ls excludes", TestLsExcludes)
	wasmtest.RegisterTest("Close", TestClose)
}

// Open the repository seeded by `wasmtest/repository_test.go`.
func openTestRepository(t *wasmtest.WasmT) js.Value {
	api := wasm.BuildRepositoryAPI()
	url := js.Global().Get("process").Get("env").Get("WASM_S3_URL").String()
	if url == "" {
		t.Fatal("WASM_S3_URL env var not set")
	}
	// Note: passphrase is set in `testdata.go`.
	repository, err := wasm.Await(api.Call("open", url, "testpassphrase"))
	if err != nil {
		t.Fatal(err)
	}
	return repository
}

func TestHappyPath(t *wasmtest.WasmT) {
	api := wasm.BuildRepositoryAPI()
	repository := openTestRepository(t)
	head, err := wasm.Await(api.Call("head", repository))
	if err != nil {
		t.Fatal(err)
	}
	want := js.Global().Get("process").Get("env").Get("WASM_HEAD_REVISION").String()
	if want == "" {
		t.Fatal("WASM_HEAD_REVISION env var not set")
	}
	if head.String() != want {
		t.Fatal(fmt.Sprintf("head revision should be %s but is: %s", want, head))
	}
}

func TestLsExcludes(t *wasmtest.WasmT) {
	api := wasm.BuildRepositoryAPI()
	repository := openTestRepository(t)
	all, err := wasm.Await(api.Call("ls", repository, ""))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(all.String(), "a.txt") || !strings.Contains(all.String(), "skip.log") {
		t.Fatal(fmt.Sprintf("expected both paths without excludes, got: %s", all))
	}
	filtered, err := wasm.Await(api.Call("ls", repository, "*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(filtered.String(), "skip.log") {
		t.Fatal(fmt.Sprintf("`*.log` should have been excluded, got: %s", filtered))
	}
	if !strings.Contains(filtered.String(), "a.txt") {
		t.Fatal(fmt.Sprintf("only `*.log` should have been excluded, got: %s", filtered))
	}
}

func TestClose(t *wasmtest.WasmT) {
	api := wasm.BuildRepositoryAPI()
	repository := openTestRepository(t)
	if _, err := wasm.Await(api.Call("close", repository)); err != nil {
		t.Fatal(err)
	}
	// The handle is gone, so using it again must fail.
	if _, err := wasm.Await(api.Call("head", repository)); err == nil {
		t.Fatal("expected head on a closed repository to fail")
	}
}
