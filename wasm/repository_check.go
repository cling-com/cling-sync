//go:build wasm && test

package main

import (
	"fmt"
)

func init() {
	RegisterTest("Happy path", TestHappyPath)
}

func TestHappyPath(t *WasmT) {
	api := BuildRepositoryAPI()
	// Note: The passphrase is set in `testdata.go`.
	repository, err := Await(api.Call("open", "http://localhost:9123", "testpassphrase"))
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
