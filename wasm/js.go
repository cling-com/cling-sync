//go:build wasm

// Basic utilities to interact with JavaScript.
package main

import (
	"context"
	"io"
	"strconv"
	"sync"
	"syscall/js"
	"time"

	clingHTTP "github.com/flunderpero/cling-sync/http"
)

// Wrap a function in a JS Promise.
func Async(fn func(resolve func(js.Value), reject func(js.Value))) js.Value {
	var handler js.Func
	handler = js.FuncOf(func(this js.Value, args []js.Value) any {
		defer handler.Release()
		go func() {
			resolve := func(value js.Value) {
				args[0].Invoke(value)
			}
			reject := func(value js.Value) {
				args[1].Invoke(value)
			}
			fn(resolve, reject)
		}()
		return nil
	})
	return js.Global().Get("Promise").New(handler)
}

type PromiseError struct {
	Message string
}

func (e PromiseError) Error() string {
	return e.Message
}

func Await(promise js.Value) (js.Value, error) {
	resultChan := make(chan js.Value, 1)
	errorChan := make(chan error, 1)

	var thenFunc js.Func
	var catchFunc js.Func
	release := func() {
		thenFunc.Release()
		catchFunc.Release()
	}
	thenFunc = js.FuncOf(func(this js.Value, args []js.Value) any {
		release()
		resultChan <- args[0]
		return nil
	})

	catchFunc = js.FuncOf(func(this js.Value, args []js.Value) any {
		release()
		errorChan <- PromiseError{"rejected: " + args[0].String()}
		return nil
	})

	// Attach handlers.
	promise.Call("then", thenFunc).Call("catch", catchFunc)

	// Wait for result or error.
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errorChan:
		return js.Null(), err
	case <-timeout.C:
		return js.Null(), PromiseError{"promise timeout after 30 seconds" + promise.String()}
	}
}

// A HTTP client that uses the browser's built-in fetch API.
type WasmHTTPClient struct{}

type FetchError struct {
	Message string
}

func (e FetchError) Error() string {
	return e.Message
}

type wasmFetch struct {
	controller js.Value
	ctxErr     func() error
	done       chan struct{}
	closeOnce  sync.Once
}

func newWasmFetch(ctx context.Context, opts js.Value) *wasmFetch {
	controller := js.Global().Get("AbortController").New()
	opts.Set("signal", controller.Get("signal"))
	fetch := &wasmFetch{
		controller: controller,
		ctxErr:     ctx.Err,
		done:       make(chan struct{}),
		closeOnce:  sync.Once{},
	}
	go func() {
		select {
		case <-ctx.Done():
			controller.Call("abort")
		case <-fetch.done:
		}
	}()
	return fetch
}

func (f *wasmFetch) Close() {
	f.closeOnce.Do(func() {
		close(f.done)
		f.controller.Call("abort")
	})
}

func (f *wasmFetch) Error(err error) error {
	if ctxErr := f.ctxErr(); ctxErr != nil {
		return ctxErr
	}
	return err
}

type wasmHTTPStreamBody struct {
	*io.PipeReader
	reader    js.Value
	done      <-chan struct{}
	fetch     *wasmFetch
	closeOnce sync.Once
	closeErr  error
}

func (b *wasmHTTPStreamBody) Close() error {
	b.closeOnce.Do(func() {
		select {
		case <-b.done:
		default:
			if !b.reader.IsUndefined() && !b.reader.IsNull() {
				b.reader.Call("cancel")
			}
		}
		b.fetch.Close()
		b.closeErr = b.PipeReader.Close()
	})
	return b.closeErr
}

func (c *WasmHTTPClient) Request(
	ctx context.Context,
	method, url string,
	body, dst []byte,
) (*clingHTTP.HTTPResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck
	}
	opts := newFetchOptions(method, body)
	fetch := newWasmFetch(ctx, opts)
	defer fetch.Close()

	resp, err := Await(js.Global().Call("fetch", url, opts))
	if err != nil {
		return nil, fetch.Error(err)
	}
	arrayBuffer, err := Await(resp.Call("arrayBuffer"))
	if err != nil {
		return nil, fetch.Error(err)
	}
	uint8Array := js.Global().Get("Uint8Array").New(arrayBuffer)
	n := uint8Array.Length()
	var bodyBytes []byte
	if dst != nil {
		if n > len(dst) {
			return nil, FetchError{
				"response body of " + strconv.Itoa(n) + " bytes exceeds buffer of " + strconv.Itoa(len(dst)),
			}
		}
		bodyBytes = dst[:n]
	} else {
		bodyBytes = make([]byte, n)
	}
	js.CopyBytesToGo(bodyBytes, uint8Array)

	return &clingHTTP.HTTPResponse{Body: bodyBytes, StatusCode: resp.Get("status").Int()}, nil
}

func (c *WasmHTTPClient) RequestStreaming(
	ctx context.Context,
	method, url string,
	body []byte,
) (*clingHTTP.HTTPStreamingResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck
	}
	opts := newFetchOptions(method, body)
	fetch := newWasmFetch(ctx, opts)
	resp, err := Await(js.Global().Call("fetch", url, opts))
	if err != nil {
		fetch.Close()
		return nil, fetch.Error(err)
	}
	statusCode := resp.Get("status").Int()
	bodyStream := resp.Get("body")
	if bodyStream.IsUndefined() || bodyStream.IsNull() {
		fetch.Close()
		reader, writer := io.Pipe()
		_ = writer.Close()
		return &clingHTTP.HTTPStreamingResponse{StatusCode: statusCode, Body: reader}, nil
	}

	streamReader := bodyStream.Call("getReader")
	// Bridge the JS ReadableStream to Go's io.Reader interface. io.Pipe gives
	// us backpressure: writes in copyReadableStream block until the caller reads
	// from Body, so the response is not buffered fully in Go.
	pipeReader, pipeWriter := io.Pipe()
	done := make(chan struct{})
	go pipeReadableStream(fetch, streamReader, pipeWriter, done)
	return &clingHTTP.HTTPStreamingResponse{
		Body: &wasmHTTPStreamBody{
			PipeReader: pipeReader,
			reader:     streamReader,
			done:       done,
			fetch:      fetch,
			closeOnce:  sync.Once{},
			closeErr:   nil,
		},
		StatusCode: statusCode,
	}, nil
}

func newFetchOptions(method string, body []byte) js.Value {
	var bodyJS js.Value
	if len(body) > 0 {
		bodyJS = js.Global().Get("Uint8Array").New(len(body))
		js.CopyBytesToJS(bodyJS, body)
	} else {
		bodyJS = js.Undefined()
	}

	opts := js.Global().Get("Object").New()
	opts.Set("method", method)
	if !bodyJS.IsUndefined() {
		opts.Set("body", bodyJS)
	}
	return opts
}

// pipeReadableStream copies chunks from a JS ReadableStream reader into an
// io.PipeWriter. Pipe writes block until the HTTP caller reads from the paired
// PipeReader, so this stays a true streaming bridge instead of accumulating the
// whole response body in memory.
func pipeReadableStream(fetch *wasmFetch, reader js.Value, writer *io.PipeWriter, done chan<- struct{}) {
	defer close(done)
	defer fetch.Close()
	defer reader.Call("releaseLock")
	for {
		result, err := Await(reader.Call("read"))
		if err != nil {
			_ = writer.CloseWithError(fetch.Error(err))
			return
		}
		if result.Get("done").Bool() {
			_ = writer.Close()
			return
		}

		value := result.Get("value")
		if value.IsUndefined() || value.IsNull() {
			continue
		}
		chunk := make([]byte, value.Length())
		js.CopyBytesToGo(chunk, value)
		if _, err := writer.Write(chunk); err != nil {
			reader.Call("cancel")
			_ = writer.CloseWithError(err)
			return
		}
	}
}
