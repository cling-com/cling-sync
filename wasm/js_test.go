//go:build !wasm

package main

import (
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

const jsTestServerAddress = "127.0.0.1:9124"

func TestWasmHTTPClient(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/regular", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if string(body) != "regular request" {
			http.Error(w, "unexpected request body", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte("regular response"))
	})
	mux.HandleFunc("/echo-header", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Header.Get("X-Echo")))
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(5 * time.Second):
			_, _ = w.Write([]byte("slow response"))
		case <-r.Context().Done():
		}
	})

	listener, err := net.Listen("tcp", jsTestServerAddress)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: mux} //nolint:exhaustruct
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-serverErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("test server failed: %v", err)
		}
	})

	RunWasmTests(t, []string{"./js.go", "./js_check.go"})
}
