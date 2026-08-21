//go:build !wasm

package http

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/cling-com/cling-sync/lib"
)

func TestNewDefaultHTTPClient(t *testing.T) {
	t.Parallel()
	t.Run("Caller-supplied client is used unchanged", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		client := &http.Client{} //nolint:exhaustruct
		assert.Equal(client, NewDefaultHTTPClient(client).Client)
	})

	t.Run("Nil client gets a transport with phase timeouts", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		transport, ok := NewDefaultHTTPClient(nil).Client.Transport.(*http.Transport)
		assert.Equal(true, ok)
		assert.Equal(10*time.Second, transport.TLSHandshakeTimeout)
		assert.Equal(60*time.Second, transport.ResponseHeaderTimeout)
		assert.Equal(time.Duration(0), NewDefaultHTTPClient(nil).Client.Timeout)
	})

	t.Run("Request against a server that never responds should fail on ResponseHeaderTimeout", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		assert.NoError(err)
		defer listener.Close() //nolint:errcheck
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close() //nolint:errcheck
			}
		}()
		transport := newDefaultTransport(time.Second, time.Second, 50*time.Millisecond)
		client := &http.Client{Transport: transport} //nolint:exhaustruct
		httpClient := NewDefaultHTTPClient(client)
		start := time.Now()
		_, _, err = httpClient.Request(t.Context(), http.MethodGet, "http://"+listener.Addr().String(), nil, nil, nil)
		assert.Error(err, "failed to execute GET")
		assert.Equal(true, time.Since(start) < 5*time.Second)
	})
}
