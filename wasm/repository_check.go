//go:build wasm && test && checkrepo

// Checks for the repository API. The `checkrepo` tag selects this file into the
// repository test binary. See `testgo.go` for the wasm test system.

package main

import (
	"fmt"
	"syscall/js"
)

func init() {
	RegisterTest("Happy path", TestHappyPath)
	RegisterTest("Close", TestClose)
}

func TestHappyPath(t *WasmT) {
	api := BuildRepositoryAPI()
	url := js.Global().Get("process").Get("env").Get("WASM_S3_URL").String()
	if url == "" {
		t.Fatal("WASM_S3_URL env var not set")
	}
	// Note: passphrase is set in `testdata.go`.
	repository, err := Await(api.Call("open", url, "testpassphrase"))
	if err != nil {
		t.Fatal(err)
	}
	head, err := Await(api.Call("head", repository))
	if err != nil {
		t.Fatal(err)
	}
	if head.String() != "0000000000000000000000000000000000000000000000000000000000000000" {
		t.Fatal(fmt.Sprintf("head revision should be root but is: %s", head))
	}
}

func TestClose(t *WasmT) {
	api := BuildRepositoryAPI()
	url := js.Global().Get("process").Get("env").Get("WASM_S3_URL").String()
	if url == "" {
		t.Fatal("WASM_S3_URL env var not set")
	}
	repository, err := Await(api.Call("open", url, "testpassphrase"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Await(api.Call("close", repository)); err != nil {
		t.Fatal(err)
	}
	// The handle is gone, so using it again must fail.
	if _, err := Await(api.Call("head", repository)); err == nil {
		t.Fatal("expected head on a closed repository to fail")
	}
}
