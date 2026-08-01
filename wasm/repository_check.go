//go:build wasm && test && checkrepo

// Checks for the repository API. The `checkrepo` tag selects this file into the
// repository test binary. See `testgo.go` for the wasm test system.

package main

import (
	"fmt"
	"strings"
	"syscall/js"
)

func init() {
	RegisterTest("Happy path", TestHappyPath)
	RegisterTest("Ls excludes", TestLsExcludes)
	RegisterTest("Close", TestClose)
}

// Open the repository seeded by `repository_test.go`.
func openTestRepository(t *WasmT) js.Value {
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
	return repository
}

func TestHappyPath(t *WasmT) {
	api := BuildRepositoryAPI()
	repository := openTestRepository(t)
	head, err := Await(api.Call("head", repository))
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

func TestLsExcludes(t *WasmT) {
	api := BuildRepositoryAPI()
	repository := openTestRepository(t)
	all, err := Await(api.Call("ls", repository, ""))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(all.String(), "a.txt") || !strings.Contains(all.String(), "skip.log") {
		t.Fatal(fmt.Sprintf("expected both paths without excludes, got: %s", all))
	}
	filtered, err := Await(api.Call("ls", repository, "*.log"))
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

func TestClose(t *WasmT) {
	api := BuildRepositoryAPI()
	repository := openTestRepository(t)
	if _, err := Await(api.Call("close", repository)); err != nil {
		t.Fatal(err)
	}
	// The handle is gone, so using it again must fail.
	if _, err := Await(api.Call("head", repository)); err == nil {
		t.Fatal("expected head on a closed repository to fail")
	}
}
