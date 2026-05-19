//go:build wasm && test

package main

import (
	"context"
	"errors"
	"io"
	"syscall/js"
	"time"
)

const jsTestServerURL = "http://127.0.0.1:9124"

func init() {
	RegisterTest("WasmHTTPClient buffered request", TestWasmHTTPClientBufferedRequest)
	RegisterTest("WasmHTTPClient streaming request", TestWasmHTTPClientStreamingRequest)
	RegisterTest("WasmHTTPClient request context", TestWasmHTTPClientRequestContext)
	RegisterTest("WasmHTTPClient streaming releases reader", TestWasmHTTPClientStreamingReleasesReader)
	RegisterTest("WasmHTTPClient streaming close cancels reader", TestWasmHTTPClientStreamingCloseCancelsReader)
}

func TestWasmHTTPClientBufferedRequest(t *WasmT) {
	client := &WasmHTTPClient{}
	resp, err := client.Request(
		context.Background(),
		"POST",
		jsTestServerURL+"/regular",
		[]byte("regular request"),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status code = %d, want 200", resp.StatusCode)
	}
	if string(resp.Body) != "regular response" {
		t.Fatalf("body = %q, want %q", string(resp.Body), "regular response")
	}
}

func TestWasmHTTPClientStreamingRequest(t *WasmT) {
	client := &WasmHTTPClient{}
	resp, err := client.RequestStreaming(context.Background(), "GET", jsTestServerURL+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != 200 {
		t.Fatalf("status code = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "stream response body" {
		t.Fatalf("body = %q, want %q", string(body), "stream response body")
	}
}

func TestWasmHTTPClientRequestContext(t *WasmT) {
	client := &WasmHTTPClient{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := client.Request(ctx, "GET", jsTestServerURL+"/slow", nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestWasmHTTPClientStreamingReleasesReader(t *WasmT) {
	// Mock fetch with a JS ReadableStream so the test can observe the stream's
	// `locked` flag after the Go reader reaches EOF.
	stream, releaseStream := newTestReadableStream([]string{"stream ", "body"}, true, nil)
	defer releaseStream()
	restoreFetch := mockFetchStream(stream)
	defer restoreFetch()

	client := &WasmHTTPClient{}
	resp, err := client.RequestStreaming(context.Background(), "GET", "http://test.invalid/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "stream body" {
		t.Fatalf("body = %q, want %q", string(body), "stream body")
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	waitForUnlocked(t, stream)
}

func TestWasmHTTPClientStreamingCloseCancelsReader(t *WasmT) {
	// Keep the JS stream open and close the Go body early. That should cancel
	// the JS reader and release its lock instead of leaving fetch resources open.
	cancelled := make(chan struct{}, 1)
	stream, releaseStream := newTestReadableStream([]string{"stream body"}, false, func() {
		cancelled <- struct{}{}
	})
	defer releaseStream()
	restoreFetch := mockFetchStream(stream)
	defer restoreFetch()

	client := &WasmHTTPClient{}
	resp, err := client.RequestStreaming(context.Background(), "GET", "http://test.invalid/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !stream.Get("locked").Bool() {
		t.Fatal("stream is not locked")
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("stream cancel was not called")
	}
	waitForUnlocked(t, stream)
}

// newTestReadableStream builds a JS ReadableStream backed by fixed byte chunks.
// If `closeStream` is false, the stream remains pending until the Go side closes
// it, which makes the cancellation path observable.
func newTestReadableStream(chunks []string, closeStream bool, cancel func()) (js.Value, func()) {
	source := js.Global().Get("Object").New()
	start := js.FuncOf(func(this js.Value, args []js.Value) any {
		controller := args[0]
		for _, chunk := range chunks {
			controller.Call("enqueue", jsUint8Array(chunk))
		}
		if closeStream {
			controller.Call("close")
		}
		return nil
	})
	source.Set("start", start)
	var cancelFunc js.Func
	if cancel != nil {
		cancelFunc = js.FuncOf(func(this js.Value, args []js.Value) any {
			cancel()
			return nil
		})
		source.Set("cancel", cancelFunc)
	}
	stream := js.Global().Get("ReadableStream").New(source)
	return stream, func() {
		start.Release()
		if cancel != nil {
			cancelFunc.Release()
		}
	}
}

// mockFetchStream replaces global fetch with a function returning a Response
// backed by `stream`. The returned cleanup restores fetch and releases the JS
// callback.
func mockFetchStream(stream js.Value) func() {
	originalFetch := js.Global().Get("fetch")
	fetchFunc := js.FuncOf(func(this js.Value, args []js.Value) any {
		opts := js.Global().Get("Object").New()
		opts.Set("status", 200)
		resp := js.Global().Get("Response").New(stream, opts)
		return js.Global().Get("Promise").Call("resolve", resp)
	})
	js.Global().Set("fetch", fetchFunc)
	return func() {
		js.Global().Set("fetch", originalFetch)
		fetchFunc.Release()
	}
}

func jsUint8Array(s string) js.Value {
	data := js.Global().Get("Uint8Array").New(len(s))
	js.CopyBytesToJS(data, []byte(s))
	return data
}

// A ReadableStream stays locked while a reader is active. Waiting for it to
// unlock verifies that copyReadableStream called releaseLock.
func waitForUnlocked(t *WasmT, stream js.Value) {
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for stream.Get("locked").Bool() {
		select {
		case <-deadline:
			t.Fatal("stream reader lock was not released")
		case <-ticker.C:
		}
	}
}
