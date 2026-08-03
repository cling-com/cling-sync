//go:build wasm

// Checks for the `WasmHTTPClient`. See `wasm/internal/wasmtest/driver.go` for
// the wasm test system.

package main

import (
	"context"
	"errors"
	"time"

	"github.com/cling-com/cling-sync/wasm"
	"github.com/cling-com/cling-sync/wasm/internal/wasmtest"
)

const jsTestServerURL = "http://127.0.0.1:9124"

func main() {
	// Keep the program running, because the Wasm module is unloaded when `main` returns.
	select {}
}

//go:wasmexport registerTests
func registerTests() { //nolint:unused // Called from JS once the module is instantiated.
	wasmtest.ExportRunTests()
}

func init() { //nolint:gochecknoinits
	wasmtest.RegisterTest("WasmHTTPClient buffered request", TestWasmHTTPClientBufferedRequest)
	wasmtest.RegisterTest("WasmHTTPClient request with headers", TestWasmHTTPClientHeaders)
	wasmtest.RegisterTest("WasmHTTPClient request context", TestWasmHTTPClientRequestContext)
}

func TestWasmHTTPClientBufferedRequest(t *wasmtest.WasmT) {
	client := &wasm.WasmHTTPClient{}
	status, body, err := client.Request(
		context.Background(), "POST", jsTestServerURL+"/regular", nil, []byte("regular request"), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if status != 200 {
		t.Fatalf("status code = %d, want 200", status)
	}
	if string(body) != "regular response" {
		t.Fatalf("body = %q, want %q", string(body), "regular response")
	}
}

func TestWasmHTTPClientHeaders(t *wasmtest.WasmT) {
	client := &wasm.WasmHTTPClient{}
	status, body, err := client.Request(
		context.Background(), "GET", jsTestServerURL+"/echo-header",
		map[string]string{"X-Echo": "hello"}, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if status != 200 {
		t.Fatalf("status code = %d, want 200", status)
	}
	if string(body) != "hello" {
		t.Fatalf("body = %q, want %q", string(body), "hello")
	}
}

func TestWasmHTTPClientRequestContext(t *wasmtest.WasmT) {
	client := &wasm.WasmHTTPClient{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, _, err := client.Request(ctx, "GET", jsTestServerURL+"/slow", nil, nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want %v", err, context.DeadlineExceeded)
	}
}
