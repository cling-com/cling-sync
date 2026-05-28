//go:build wasm && test

package main

import (
	"context"
	"errors"
	"time"
)

const jsTestServerURL = "http://127.0.0.1:9124"

func init() {
	RegisterTest("WasmHTTPClient buffered request", TestWasmHTTPClientBufferedRequest)
	RegisterTest("WasmHTTPClient request with headers", TestWasmHTTPClientHeaders)
	RegisterTest("WasmHTTPClient request context", TestWasmHTTPClientRequestContext)
}

func TestWasmHTTPClientBufferedRequest(t *WasmT) {
	client := &WasmHTTPClient{}
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

func TestWasmHTTPClientHeaders(t *WasmT) {
	client := &WasmHTTPClient{}
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

func TestWasmHTTPClientRequestContext(t *WasmT) {
	client := &WasmHTTPClient{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, _, err := client.Request(ctx, "GET", jsTestServerURL+"/slow", nil, nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want %v", err, context.DeadlineExceeded)
	}
}
