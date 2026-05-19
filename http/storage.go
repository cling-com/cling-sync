package http

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

const DefaultLockLeaseMin = 5 * time.Second

// We use a simplified interface to improve support for Wasm, i.e. we don't
// bring in the full HTTP client which adds a lot of dependencies.
type HTTPClient interface {
	// `dst`, when non-nil, is the destination slice for the response body. The
	// body is read into `dst`; `resp.Body` slices into `dst[:n]`. If the response
	// exceeds `len(dst)` the call returns an error. When `dst` is nil the body
	// is allocated and capped at `lib.MaxBlockSize` for safety.
	Request(ctx context.Context, method, url string, body, dst []byte) (*HTTPResponse, error)
	RequestStreaming(ctx context.Context, method, url string, body []byte) (*HTTPStreamingResponse, error)
}

type HTTPResponse struct {
	StatusCode int
	Body       []byte
}

type HTTPStreamingResponse struct {
	StatusCode int
	Body       io.ReadCloser
}

type DefaultHTTPClient struct {
	Client *http.Client
}

func NewDefaultHTTPClient(client *http.Client) *DefaultHTTPClient {
	return &DefaultHTTPClient{client}
}

func (c *DefaultHTTPClient) Request(
	ctx context.Context,
	method, url string,
	body, dst []byte,
) (*HTTPResponse, error) {
	resp, err := c.RequestStreaming(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if dst != nil {
		// SEC: A malicious server must not be allowed to overflow `dst`.
		limit := io.LimitReader(resp.Body, int64(len(dst))+1)
		n, err := io.ReadFull(limit, dst)
		switch {
		case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
			// Short body - that's fine; n is the actual size.
		case err != nil:
			return nil, lib.WrapErrorf(err, "failed to read response body")
		default:
			// Filled `dst` completely. Make sure there isn't more.
			var extraBuf [1]byte
			extra, _ := limit.Read(extraBuf[:])
			if extra > 0 {
				return nil, lib.Errorf("response body exceeds buffer of %d", len(dst))
			}
		}
		return &HTTPResponse{StatusCode: resp.StatusCode, Body: dst[:n]}, nil
	}
	// SEC: Cap at MaxBlockSize. The server might misbehave or be malicious.
	limit := io.LimitReader(resp.Body, int64(lib.MaxBlockSize)+1)
	data, err := io.ReadAll(limit)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read response body")
	}
	if len(data) > lib.MaxBlockSize {
		return nil, lib.Errorf("response body exceeds maximum of %d", lib.MaxBlockSize)
	}
	return &HTTPResponse{StatusCode: resp.StatusCode, Body: data}, nil
}

func (c *DefaultHTTPClient) RequestStreaming(
	ctx context.Context,
	method, url string,
	body []byte,
) (*HTTPStreamingResponse, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create request")
	}
	resp, err := c.Client.Do(req) //nolint:bodyclose,gosec
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to execute request")
	}
	return &HTTPStreamingResponse{StatusCode: resp.StatusCode, Body: resp.Body}, nil
}

type HTTPStorageClient struct {
	Address string
	Client  HTTPClient
	// LockLeaseMin is the period the lock-extend goroutine targets when
	// renewing a held lock.
	LockLeaseMin   time.Duration
	lockLeaseError atomic.Value
}

func IsHTTPStorageUIR(uri string) bool {
	return strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://")
}

func NewHTTPStorageClient(address string, client HTTPClient) *HTTPStorageClient {
	return &HTTPStorageClient{
		Address:        address,
		Client:         client,
		LockLeaseMin:   DefaultLockLeaseMin,
		lockLeaseError: atomic.Value{},
	}
}

func (c *HTTPStorageClient) Init(config lib.Toml, headerComment string) error {
	return lib.Errorf("HTTPStorageClient.Init is not supported")
}

func (c *HTTPStorageClient) Open() (lib.Toml, error) {
	resp, err := c.request(context.Background(), http.MethodGet, "/storage/open", nil)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open storage")
	}
	toml, err := lib.ReadToml(bytes.NewReader(resp.Body))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read storage TOML")
	}
	return toml, nil
}

func (c *HTTPStorageClient) HasBlock(blockId lib.BlockId) (bool, error) {
	resp, err := c.request(context.Background(), http.MethodHead, "/storage/block/"+blockId.String(), nil, 404)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to check if block exists")
	}
	return resp.StatusCode == http.StatusOK, nil
}

func (c *HTTPStorageClient) ReadBlockIds(yield func(lib.BlockId) error) error {
	resp, err := c.requestStreaming(context.Background(), http.MethodGet, "/storage/block-ids", nil)
	if err != nil {
		return lib.WrapErrorf(err, "failed to read block ids")
	}
	defer resp.Body.Close() //nolint:errcheck
	reader := bufio.NewReaderSize(resp.Body, lib.BlockIdSize*4096)
	for {
		var blockId lib.BlockId
		_, err := io.ReadFull(reader, blockId[:])
		switch {
		case err == nil:
			if err := yield(blockId); err != nil {
				return lib.WrapErrorf(err, "failed to handle block id %s", blockId)
			}
		case errors.Is(err, io.EOF):
			return nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			return lib.Errorf("truncated block id stream")
		default:
			return lib.WrapErrorf(err, "failed to read block id stream")
		}
	}
}

func (c *HTTPStorageClient) ReadBlock(blockId lib.BlockId, buf lib.BlockBuf) ([]byte, error) {
	resp, err := c.requestInto(
		context.Background(), http.MethodGet, "/storage/block/"+blockId.String(), nil, buf.Bytes(), 404,
	)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read block")
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, lib.ErrBlockNotFound
	}
	return resp.Body, nil
}

func (c *HTTPStorageClient) WriteBlock(blockId lib.BlockId, data []byte) (bool, error) {
	if len(data) > lib.MaxBlockSize {
		return false, lib.Errorf("block %s is too large: %d", blockId, len(data))
	}
	resp, err := c.request(context.Background(), http.MethodPut, "/storage/block/"+blockId.String(), data)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to write block")
	}
	return resp.StatusCode != http.StatusCreated, nil
}

func (c *HTTPStorageClient) HasControlFile(section lib.ControlFileSection, name string) (bool, error) {
	resp, err := c.request(
		context.Background(),
		http.MethodHead,
		"/storage/control/"+string(section)+"/"+name,
		nil,
		404,
	)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to check if control file exists")
	}
	return resp.StatusCode == http.StatusOK, nil
}

func (c *HTTPStorageClient) ReadControlFile(section lib.ControlFileSection, name string) ([]byte, error) {
	resp, err := c.request(context.Background(), http.MethodGet, "/storage/control/"+string(section)+"/"+name, nil, 404)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read control file")
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, lib.ErrControlFileNotFound
	}
	return resp.Body, nil
}

func (c *HTTPStorageClient) WriteControlFile(section lib.ControlFileSection, name string, data []byte) error {
	resp, err := c.request(context.Background(), http.MethodPut, "/storage/control/"+string(section)+"/"+name, data)
	if err != nil {
		return lib.WrapErrorf(err, "failed to write control file")
	}
	if resp.StatusCode != http.StatusOK {
		return lib.Errorf("failed to write control file: got %d (%s)", resp.StatusCode, string(data))
	}
	return nil
}

func (c *HTTPStorageClient) DeleteControlFile(section lib.ControlFileSection, name string) error {
	resp, err := c.request(
		context.Background(),
		http.MethodDelete,
		"/storage/control/"+string(section)+"/"+name,
		nil,
		404,
	)
	if err != nil {
		return lib.WrapErrorf(err, "failed to delete control file")
	}
	if resp.StatusCode == http.StatusNotFound {
		return lib.ErrControlFileNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return lib.Errorf("failed to delete control file: got %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPStorageClient) Lock(ctx context.Context, name string) (func() error, error) {
	resp, err := c.request(ctx, http.MethodPut, "/storage/lock/"+name, nil)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create lock file %s", name)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, lib.Errorf("failed to create lock file: got %d (%s)", resp.StatusCode, string(resp.Body))
	}
	token := string(resp.Body)
	refreshCtx, refreshCancel := context.WithCancel(ctx) //nolint:gosec
	go c.keepLockAlive(refreshCtx, token)
	var once sync.Once
	var unlockErr error
	return func() error {
		once.Do(func() {
			refreshCancel()
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			resp, err := c.request(ctx, http.MethodDelete, "/storage/lock/"+token, nil)
			if err != nil {
				unlockErr = lib.WrapErrorf(err, "failed to delete lock %s with token %s", name, token)
				return
			}
			if resp.StatusCode != http.StatusOK {
				unlockErr = lib.Errorf(
					"failed to delete lock %s with token %s: got %d (%s)",
					name,
					token,
					resp.StatusCode,
					string(resp.Body),
				)
			}
		})
		return unlockErr
	}, nil
}

// keepLockAlive runs in its own goroutine for the lifetime of a held lock and
// keeps extending the server-side lease until `refreshCtx` is cancelled. On
// any failure it records the error in `c.lockLeaseError` and exits.
func (c *HTTPStorageClient) keepLockAlive(refreshCtx context.Context, token string) {
	ticker := time.NewTicker(min(c.LockLeaseMin/2, time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-refreshCtx.Done():
			return
		}
		ctx, cancel := context.WithTimeout(refreshCtx, c.LockLeaseMin/2)
		resp, err := c.request(ctx, http.MethodPost, "/storage/lock/"+token, nil)
		cancel()
		if refreshCtx.Err() != nil {
			return
		}
		if err != nil {
			c.lockLeaseError.Store(lib.WrapErrorf(err, "failed to extend lock %s", token))
			return
		}
		if resp.StatusCode != http.StatusOK {
			c.lockLeaseError.Store(lib.Errorf(
				"failed to extend lock %s: got %d (%s)",
				token,
				resp.StatusCode,
				string(resp.Body),
			))
			return
		}
	}
}

func (c *HTTPStorageClient) request(
	ctx context.Context,
	method, path string,
	body []byte,
	ignoreStatusCodes ...int,
) (*HTTPResponse, error) {
	return c.requestInto(ctx, method, path, body, nil, ignoreStatusCodes...)
}

func (c *HTTPStorageClient) requestInto(
	ctx context.Context,
	method, path string,
	body, dst []byte,
	ignoreStatusCodes ...int,
) (*HTTPResponse, error) {
	if err := c.lockLeaseError.Load(); err != nil {
		err, ok := err.(error)
		if !ok {
			return nil, lib.Errorf("failed to execute request because a lock lease expired")
		}
		return nil, lib.WrapErrorf(err, "failed to execute request because a lock lease expired")
	}
	resp, err := c.Client.Request(ctx, method, c.Address+path, body, dst)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to execute request")
	}
	if resp.StatusCode >= 400 && !slices.Contains(ignoreStatusCodes, resp.StatusCode) {
		return nil, lib.Errorf("failed request %s %s: got %d (%s)", method, path, resp.StatusCode, string(resp.Body))
	}
	return resp, nil
}

func (c *HTTPStorageClient) requestStreaming(
	ctx context.Context,
	method, path string,
	body []byte,
	ignoreStatusCodes ...int,
) (*HTTPStreamingResponse, error) {
	if err := c.lockLeaseError.Load(); err != nil {
		err, ok := err.(error)
		if !ok {
			return nil, lib.Errorf("failed to execute request because a lock lease expired")
		}
		return nil, lib.WrapErrorf(err, "failed to execute request because a lock lease expired")
	}
	resp, err := c.Client.RequestStreaming(ctx, method, c.Address+path, body)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to execute streaming request")
	}
	if resp.StatusCode >= 400 && !slices.Contains(ignoreStatusCodes, resp.StatusCode) {
		defer resp.Body.Close() //nolint:errcheck
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err != nil {
			return nil, lib.WrapErrorf(err, "failed to read error response")
		}
		return nil, lib.Errorf("failed request %s %s: got %d (%s)", method, path, resp.StatusCode, string(data))
	}
	return resp, nil
}

type lockHandle struct {
	extend chan struct{}
	done   chan struct{}
}

type HTTPStorageServer struct {
	Storage      lib.Storage
	Address      string
	LockLeaseMin time.Duration
	locks        sync.Map // token -> *lockHandle
}

func NewHTTPStorageServer(storage lib.Storage, address string) *HTTPStorageServer {
	return &HTTPStorageServer{
		Storage:      storage,
		Address:      address,
		LockLeaseMin: DefaultLockLeaseMin,
		locks:        sync.Map{},
	}
}

// readBoundedBody reads at most `max` bytes from `r.Body` and returns them as
// a slice.
// If the body exceeds `max`, `http.MaxBytesReader` causes the underlying read
// to fail with `*http.MaxBytesError`, which the caller turns into a 413.
func readBoundedBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	if r.ContentLength > 0 && r.ContentLength <= limit {
		buf := bytes.NewBuffer(make([]byte, 0, r.ContentLength))
		if _, err := buf.ReadFrom(r.Body); err != nil {
			return nil, err //nolint:wrapcheck
		}
		return buf.Bytes(), nil
	}
	return io.ReadAll(r.Body) //nolint:wrapcheck
}

func (s *HTTPStorageServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /storage/open", s.Open)
	mux.HandleFunc("GET /storage/block-ids", s.ReadBlockIds)
	mux.HandleFunc("HEAD /storage/block/{id}", s.HasBlock)
	mux.HandleFunc("GET /storage/block/{id}", s.ReadBlock)
	mux.HandleFunc("PUT /storage/block/{id}", s.WriteBlock)
	mux.HandleFunc("HEAD /storage/control/{section}/{name}", s.HasControlFile)
	mux.HandleFunc("GET /storage/control/{section}/{name}", s.ReadControlFile)
	mux.HandleFunc("PUT /storage/control/{section}/{name}", s.WriteControlFile)
	mux.HandleFunc("DELETE /storage/control/{section}/{name}", s.DeleteControlFile)
	mux.HandleFunc("PUT /storage/lock/{name}", s.Lock)
	mux.HandleFunc("DELETE /storage/lock/{token}", s.Unlock)
	mux.HandleFunc("POST /storage/lock/{token}", s.ExtendLock)
}

// Return the storage TOML as a string.
func (s *HTTPStorageServer) Open(w http.ResponseWriter, r *http.Request) {
	toml, err := s.Storage.Open()
	if err != nil {
		s.emitError(lib.WrapErrorf(err, "failed to open storage"), w, http.StatusInternalServerError)
		return
	}
	if err := lib.WriteToml(w, "", toml); err != nil {
		s.emitError(lib.WrapErrorf(err, "failed to write storage TOML"), w, http.StatusInternalServerError)
		return
	}
}

func (s *HTTPStorageServer) HasBlock(w http.ResponseWriter, r *http.Request) {
	blockId, err := lib.NewBlockIdFromString(r.PathValue("id"))
	if err != nil {
		s.emitError(err, w, http.StatusBadRequest)
		return
	}
	exists, err := s.Storage.HasBlock(blockId)
	if err != nil {
		s.emitError(lib.WrapErrorf(err, "failed to check if block exists"), w, http.StatusInternalServerError)
		return
	}
	if exists {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *HTTPStorageServer) ReadBlockIds(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 0, lib.BlockIdSize*4096)
	written := false

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if _, err := w.Write(buf); err != nil {
			return lib.WrapErrorf(err, "failed to write block id stream")
		}
		written = true
		buf = buf[:0]
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}

	err := s.Storage.ReadBlockIds(func(blockId lib.BlockId) error {
		select {
		case <-r.Context().Done():
			return r.Context().Err()
		default:
		}
		if len(buf)+lib.BlockIdSize > cap(buf) {
			if err := flush(); err != nil {
				return err
			}
		}
		buf = append(buf, blockId[:]...)
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	if !written {
		s.emitError(lib.WrapErrorf(err, "failed to read block ids"), w, http.StatusInternalServerError)
		return
	}
	slog.Error("Failed to stream block ids - aborting the transport", "error", err)
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		slog.Error("Failed to abort HTTP response", "error", err)
		return
	}
	if err := conn.Close(); err != nil {
		slog.Error("Failed to close aborted HTTP response", "error", err)
	}
}

// Return the block with the given id. The response body is the opaque block bytes as stored.
func (s *HTTPStorageServer) ReadBlock(w http.ResponseWriter, r *http.Request) {
	blockId, err := lib.NewBlockIdFromString(r.PathValue("id"))
	if err != nil {
		s.emitError(err, w, http.StatusBadRequest)
		return
	}
	buf := lib.NewBlockBuf()
	data, err := s.Storage.ReadBlock(blockId, buf)
	if err != nil {
		if errors.Is(err, lib.ErrBlockNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.emitError(lib.WrapErrorf(err, "failed to read block"), w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if _, err := w.Write(data); err != nil { //nolint:gosec
		s.emitError(lib.WrapErrorf(err, "failed to write block data"), w, http.StatusInternalServerError)
		return
	}
}

// Write a block. The request body is the opaque block bytes as produced by the client.
//
// Return `200 OK` if the block already existed.
// Return `201 Created` if the block was written.
func (s *HTTPStorageServer) WriteBlock(w http.ResponseWriter, r *http.Request) {
	blockId, err := lib.NewBlockIdFromString(r.PathValue("id"))
	if err != nil {
		s.emitError(err, w, http.StatusBadRequest)
		return
	}
	data, err := readBoundedBody(w, r, lib.MaxBlockSize)
	if err != nil {
		s.emitBodyReadError(w, err, "block")
		return
	}
	exists, err := s.Storage.WriteBlock(blockId, data)
	if err != nil {
		s.emitError(lib.WrapErrorf(err, "failed to write block"), w, http.StatusInternalServerError)
		return
	}
	if exists {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
}

func (s *HTTPStorageServer) HasControlFile(w http.ResponseWriter, r *http.Request) {
	section := r.PathValue("section")
	name := r.PathValue("name")
	exists, err := s.Storage.HasControlFile(lib.ControlFileSection(section), name)
	if err != nil {
		s.emitError(lib.WrapErrorf(err, "failed to check if control file exists"), w, http.StatusInternalServerError)
		return
	}
	if exists {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *HTTPStorageServer) ReadControlFile(w http.ResponseWriter, r *http.Request) {
	section := r.PathValue("section")
	name := r.PathValue("name")
	data, err := s.Storage.ReadControlFile(lib.ControlFileSection(section), name)
	if err != nil {
		if errors.Is(err, lib.ErrControlFileNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.emitError(lib.WrapErrorf(err, "failed to read control file"), w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if _, err := w.Write(data); err != nil { //nolint:gosec
		s.emitError(lib.WrapErrorf(err, "failed to write control file data"), w, http.StatusInternalServerError)
		return
	}
}

func (s *HTTPStorageServer) WriteControlFile(w http.ResponseWriter, r *http.Request) {
	section := r.PathValue("section")
	name := r.PathValue("name")
	data, err := readBoundedBody(w, r, lib.MaxControlFileSize)
	if err != nil {
		s.emitBodyReadError(w, err, "control file")
		return
	}
	if err := s.Storage.WriteControlFile(lib.ControlFileSection(section), name, data); err != nil {
		s.emitError(lib.WrapErrorf(err, "failed to write control file"), w, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *HTTPStorageServer) DeleteControlFile(w http.ResponseWriter, r *http.Request) {
	section := r.PathValue("section")
	name := r.PathValue("name")
	if err := s.Storage.DeleteControlFile(lib.ControlFileSection(section), name); err != nil {
		if errors.Is(err, lib.ErrControlFileNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.emitError(lib.WrapErrorf(err, "failed to delete control file"), w, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *HTTPStorageServer) Lock(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	token, err := lib.RandStr(32)
	if err != nil {
		s.emitError(lib.WrapErrorf(err, "failed to generate random token"), w, http.StatusInternalServerError)
		return
	}

	unlockPure, err := s.Storage.Lock(r.Context(), name)
	if err != nil {
		s.emitError(lib.WrapErrorf(err, "failed to create lock %s", name), w, http.StatusInternalServerError)
		return
	}
	// Make sure the unlock function is called exactly once.
	unlock := sync.OnceValue(unlockPure)

	handle := &lockHandle{
		extend: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
	s.locks.Store(token, handle)

	leaseMin := s.LockLeaseMin
	go func() {
		timer := time.NewTimer(leaseMin)
		defer timer.Stop()
		defer s.locks.Delete(token)
		for {
			select {
			case <-timer.C:
				if err := unlock(); err != nil {
					slog.Error( //nolint:gosec
						"Failed to unlock expired lock",
						"error",
						err,
						"token",
						token,
						"name",
						name,
					)
				}
				return
			case <-handle.extend:
				if !timer.Stop() {
					// Timer already fired, drain it if needed
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(leaseMin)
			case <-handle.done:
				if err := unlock(); err != nil {
					slog.Error("Failed to unlock lock", "error", err, "token", token, "name", name) //nolint:gosec
				}
				return
			}
		}
	}()

	if _, err := w.Write([]byte(token)); err != nil {
		s.emitError(lib.WrapErrorf(err, "failed to write token"), w, http.StatusInternalServerError)
		return
	}
}

func (s *HTTPStorageServer) Unlock(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	value, ok := s.locks.LoadAndDelete(token)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	handle := value.(*lockHandle) //nolint:forcetypeassert
	close(handle.done)
	w.WriteHeader(http.StatusOK)
}

func (s *HTTPStorageServer) ExtendLock(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	value, ok := s.locks.Load(token)
	if !ok {
		s.emitError(lib.Errorf("lock %s does not exist", token), w, http.StatusNotFound)
		return
	}
	handle := value.(*lockHandle) //nolint:forcetypeassert
	select {
	case handle.extend <- struct{}{}:
	default:
		// There is already a message that signals to extend the lock.
	}
	w.WriteHeader(http.StatusOK)
}

// emitBodyReadError responds with 413 when the input exceeded `MaxBytesReader`'s
// limit, otherwise 400 for any other read failure.
func (s *HTTPStorageServer) emitBodyReadError(w http.ResponseWriter, err error, kind string) {
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		s.emitError(lib.WrapErrorf(err, "%s body too large", kind), w, http.StatusRequestEntityTooLarge)
		return
	}
	s.emitError(lib.WrapErrorf(err, "failed to read %s body", kind), w, http.StatusBadRequest)
}

func (s *HTTPStorageServer) emitError(err error, w http.ResponseWriter, status int) {
	slog.Error("HTTP error", "error", err)
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "text/plain")
	msg := err.Error()
	if we, ok := errors.AsType[*lib.WrappedError](err); ok {
		// Prefer the top-level message; the chain has already been logged.
		msg = we.Msg
	}
	if _, err := w.Write([]byte(msg)); err != nil { //nolint:gosec
		slog.Error("Failed to write error response", "error", err)
		return
	}
}
