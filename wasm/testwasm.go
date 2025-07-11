//go:build wasm && test

// The Wasm side of the test runner. This file is always compiled as part of `RunWasMTests`.
// It provides a simple API for writing tests, a limited version of Go's `testing.T`.
//
// Because tests cannot be discovered at runtime, they must be registered with `RegisterTest`.
// This should be done in an `init` function in the test file:
//
//	package my_test
//
//  func init() {
//		RegisterTest("My test", TestMyTest)
//	}
//
//  func TestMyTest(t *WasmT) {
//      t.Fatal("This test always fails")
//  }

package main

import (
	"fmt"
	"runtime"
	"strings"

	"syscall/js"
)

func main() {
	// Keep the program running, because the Wasm module is unloaded when `main` returns.
	select {}
}

type WasmT struct{}

func (w *WasmT) Fatal(args ...any) {
	panic(w.makeFailure(fmt.Sprint(args...)))
}

func (w *WasmT) Fatalf(format string, args ...any) {
	panic(w.makeFailure(fmt.Sprintf(format, args...)))
}

func (w *WasmT) Log(args ...any) {
	log(args...)
}

func (w *WasmT) makeFailure(message string) testFailure {
	_, file, line, ok := runtime.Caller(2)
	if ok {
		if idx := strings.LastIndex(file, "/"); idx >= 0 {
			file = file[idx+1:]
		}
	} else {
		file = "???"
		line = 0
	}
	return testFailure{message, file, line}
}

var tests []test

type test struct {
	Name string
	Fn   func(t *WasmT)
}

type testFailure struct {
	Message string
	File    string
	Line    int
}

func (t testFailure) Error() string {
	return fmt.Sprintf("%s:%d: %s", t.File, t.Line, t.Message)
}

func RegisterTest(name string, fn func(t *WasmT)) {
	tests = append(tests, test{Name: name, Fn: fn})
}

//go:wasmexport registerTests
func registerTests() {
	js.Global().Set("runTests", js.FuncOf(runTests))
}

func runTests(this js.Value, args []js.Value) any {
	runTest := func(test func(w *WasmT)) js.Value {
		return Async(func(resolve func(js.Value), reject func(js.Value)) {
			w := &WasmT{}
			defer func() {
				if r := recover(); r != nil {
					var errMsg string
					switch v := r.(type) {
					case string:
						errMsg = v
					case error:
						errMsg = v.Error()
					default:
						errMsg = fmt.Sprint(v)
					}
					reject(js.ValueOf(errMsg))
				}
			}()
			test(w)
			resolve(js.ValueOf("done"))
		})
	}
	return Async(func(resolve func(js.Value), reject func(js.Value)) {
		for _, test := range tests {
			log("RUN ", test.Name)
			_, err := Await(runTest(test.Fn))
			if err != nil {
				log("FAIL", test.Name)
				reject(js.ValueOf(err.Error()))
				return
			}
			log("PASS", test.Name)
		}
		resolve(js.ValueOf("done"))
	})
}

func log(args ...any) {
	js.Global().Get("console").Call("log", args...)
}
