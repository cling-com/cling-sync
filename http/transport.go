//go:build !wasm

// The default Go HTTP Client. We put this in an extra file so that it
// gets excluded in wasm builds.

package http

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/flunderpero/cling-sync/lib"
)

type DefaultHTTPClient struct {
	Client *http.Client
}

func NewDefaultHTTPClient(client *http.Client) *DefaultHTTPClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &DefaultHTTPClient{Client: client}
}

func (c *DefaultHTTPClient) Request(
	ctx context.Context,
	method, fullURL string,
	headers map[string]string,
	body, dst []byte,
) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, fullURL, bytes.NewReader(body))
	if err != nil {
		return 0, nil, lib.WrapErrorf(err, "failed to create request")
	}
	req.ContentLength = int64(len(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	// SSRF taint is the design: `fullURL` is the user's configured bucket
	// URL. We're their S3 client.
	resp, err := c.Client.Do(req) //nolint:gosec
	if err != nil {
		return 0, nil, lib.WrapErrorf(err, "failed to execute %s %s", method, fullURL)
	}
	defer resp.Body.Close() //nolint:errcheck
	respBody, err := readCappedBody(resp.Body, dst)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}

// readCappedBody reads at most `MaxBlockSize` bytes (or `len(dst)` when
// `dst != nil`) so a misbehaving peer can't OOM us.
func readCappedBody(src io.Reader, dst []byte) ([]byte, error) {
	if dst != nil {
		limit := io.LimitReader(src, int64(len(dst))+1)
		n, err := io.ReadFull(limit, dst)
		switch {
		case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
			return dst[:n], nil
		case err != nil:
			return nil, lib.WrapErrorf(err, "failed to read response body")
		}
		var extra [1]byte
		more, _ := limit.Read(extra[:])
		if more > 0 {
			return nil, lib.Errorf("response body exceeds buffer of %d", len(dst))
		}
		return dst[:n], nil
	}
	limit := io.LimitReader(src, int64(lib.MaxBlockSize)+1)
	data, err := io.ReadAll(limit)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read response body")
	}
	if len(data) > lib.MaxBlockSize {
		return nil, lib.Errorf("response body exceeds maximum of %d", lib.MaxBlockSize)
	}
	return data, nil
}
