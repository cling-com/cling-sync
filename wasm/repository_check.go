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
