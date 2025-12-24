//go:build wasm

// Basic utilities to interact with JavaScript.
package main

import (
	"time"

	clingHTTP "github.com/flunderpero/cling-sync/http"
	"syscall/js"
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
	thenFunc = js.FuncOf(func(this js.Value, args []js.Value) any {
		defer thenFunc.Release()
		resultChan <- args[0]
		return nil
	})

	var catchFunc js.Func
	catchFunc = js.FuncOf(func(this js.Value, args []js.Value) any {
		defer catchFunc.Release()
		errorChan <- PromiseError{"rejected: " + args[0].String()}
		return nil
	})

	// Attach handlers
	promise.Call("then", thenFunc).Call("catch", catchFunc)

	// Wait for result or error.
	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errorChan:
		return js.Null(), err
	case <-time.After(30 * time.Second):
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

func (c *WasmHTTPClient) Request( //nolint:funlen
	method string,
	url string,
	body []byte,
) (*clingHTTP.HTTPResponse, error) {
	// Convert body to JS.
	var bodyJS js.Value
	if len(body) > 0 {
		bodyJS = js.Global().Get("Uint8Array").New(len(body))
		js.CopyBytesToJS(bodyJS, body)
	} else {
		bodyJS = js.Undefined()
	}

	// Create fetch options.
	opts := js.Global().Get("Object").New()
	opts.Set("method", method)
	if !bodyJS.IsUndefined() {
		opts.Set("body", bodyJS)
	}

	// Channels for communication.
	respChan := make(chan *clingHTTP.HTTPResponse, 1)
	errChan := make(chan error, 1)

	// Start the fetch in a goroutine to manage callbacks.
	go func() {
		var fetchThen js.Func
		fetchThen = js.FuncOf(func(this js.Value, args []js.Value) any {
			defer fetchThen.Release()
			resp := args[0]
			statusCode := resp.Get("status").Int()
			var arrayBufferHandler js.Func

			arrayBufferHandler = js.FuncOf(func(this js.Value, args []js.Value) any {
				defer arrayBufferHandler.Release()
				arrayBuffer := args[0]
				uint8Array := js.Global().Get("Uint8Array").New(arrayBuffer)
				bodyBytes := make([]byte, uint8Array.Length())
				js.CopyBytesToGo(bodyBytes, uint8Array)

				respChan <- &clingHTTP.HTTPResponse{
					Body:       bodyBytes,
					StatusCode: statusCode,
				}
				return nil
			})
			resp.Call("arrayBuffer").Call("then", arrayBufferHandler)
			return nil
		})
		var fetchCatch js.Func
		fetchCatch = js.FuncOf(func(this js.Value, args []js.Value) any {
			defer fetchCatch.Release()
			jsErr := args[0]
			if jsErr.Get("name").String() == "TypeError" {
				errChan <- FetchError{"network error: " + jsErr.Get("message").String()}
			} else {
				errChan <- FetchError{"fetch error: " + jsErr.Call("toString").String()}
			}
			return nil
		})
		js.Global().Call("fetch", url, opts).
			Call("then", fetchThen).
			Call("catch", fetchCatch)
	}()

	// Wait for response or error.
	select {
	case resp := <-respChan:
		return resp, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(30 * time.Second):
		return nil, FetchError{"request timeout"}
	}
}
