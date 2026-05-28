//go:build wasm

// Basic utilities to interact with JavaScript.
package main

import (
	"context"
	"strconv"
	"syscall/js"
	"time"
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

// describeJSError pulls a useful string out of a JS rejection value. JS Error
// objects have name+message; for everything else we try toString() before
// falling back to a type tag.
func describeJSError(v js.Value) string {
	if v.IsUndefined() {
		return "undefined"
	}
	if v.IsNull() {
		return "null"
	}
	if v.Type() == js.TypeString {
		return v.String()
	}
	name, msg := "", ""
	if n := v.Get("name"); n.Type() == js.TypeString {
		name = n.String()
	}
	if m := v.Get("message"); m.Type() == js.TypeString {
		msg = m.String()
	}
	switch {
	case name != "" && msg != "":
		return name + ": " + msg
	case msg != "":
		return msg
	case name != "":
		return name
	}
	if ts := v.Get("toString"); ts.Type() == js.TypeFunction {
		return v.Call("toString").String()
	}
	return "<" + v.Type().String() + ">"
}

// Await blocks until the JS Promise resolves or rejects.
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
		errorChan <- PromiseError{"rejected: " + describeJSError(args[0])}
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
		return js.Null(), PromiseError{"promise timeout after 30 seconds " + promise.String()}
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

//nolint:funlen
func (c *WasmHTTPClient) Request(
	ctx context.Context,
	method, url string,
	headers map[string]string,
	body, dst []byte,
) (int, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err //nolint:wrapcheck
	}
	opts := js.Global().Get("Object").New()
	opts.Set("method", method)
	if len(body) > 0 {
		bodyJS := js.Global().Get("Uint8Array").New(len(body))
		js.CopyBytesToJS(bodyJS, body)
		opts.Set("body", bodyJS)
	}
	if len(headers) > 0 {
		hdrs := js.Global().Get("Object").New()
		for k, v := range headers {
			hdrs.Set(k, v)
		}
		opts.Set("headers", hdrs)
	}

	controller := js.Global().Get("AbortController").New()
	opts.Set("signal", controller.Get("signal"))
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			controller.Call("abort")
		case <-done:
		}
	}()
	abortErr := func(err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr //nolint:wrapcheck
		}
		return err
	}

	resp, err := Await(js.Global().Call("fetch", url, opts))
	if err != nil {
		return 0, nil, abortErr(err)
	}
	arrayBuffer, err := Await(resp.Call("arrayBuffer"))
	if err != nil {
		return 0, nil, abortErr(err)
	}
	uint8Array := js.Global().Get("Uint8Array").New(arrayBuffer)
	n := uint8Array.Length()
	var respBody []byte
	if dst != nil {
		if n > len(dst) {
			return 0, nil, FetchError{
				"response body of " + strconv.Itoa(n) + " bytes exceeds buffer of " + strconv.Itoa(len(dst)),
			}
		}
		respBody = dst[:n]
	} else {
		respBody = make([]byte, n)
	}
	js.CopyBytesToGo(respBody, uint8Array)
	return resp.Get("status").Int(), respBody, nil
}
