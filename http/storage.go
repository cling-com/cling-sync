package http

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/flunderpero/cling-sync/lib"
)

// We use a simplified interface to improve support for Wasm, i.e. we don't
// bring in the full HTTP client which adds a lot of dependencies.
type HTTPClient interface {
	Request(method string, url string, body []byte) (*HTTPResponse, error)
}

type HTTPResponse struct {
	StatusCode int
	Body       []byte
}

type DefaultHTTPClient struct {
	Client  *http.Client
	Context context.Context //nolint:containedctx
}

func NewDefaultHTTPClient(client *http.Client, context context.Context) *DefaultHTTPClient {
	return &DefaultHTTPClient{client, context}
}

func (c *DefaultHTTPClient) Request(method string, url string, body []byte) (*HTTPResponse, error) {
	req, err := http.NewRequestWithContext(c.Context, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to create request")
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to execute request")
	}
	defer resp.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read response body")
	}
	return &HTTPResponse{resp.StatusCode, data}, nil
}

type HTTPStorageClient struct {
	Address string
	Client  HTTPClient
}

func IsHTTPStorageUIR(uri string) bool {
	return strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, "https://")
}

func NewHTTPStorageClient(address string, client HTTPClient) *HTTPStorageClient {
	return &HTTPStorageClient{address, client}
}

func (c *HTTPStorageClient) Init(config lib.Toml, headerComment string) error {
	return lib.Errorf("HTTPStorageClient.Init is not supported")
}

func (c *HTTPStorageClient) Open() (lib.Toml, error) {
	resp, err := c.request(http.MethodGet, "/storage/open", nil)
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
	resp, err := c.request(http.MethodHead, "/storage/block/"+blockId.String(), nil, 404)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to check if block exists")
	}
	return resp.StatusCode == http.StatusOK, nil
}

func (c *HTTPStorageClient) ReadBlock(blockId lib.BlockId) ([]byte, lib.BlockHeader, error) {
	resp, err := c.request(http.MethodGet, "/storage/block/"+blockId.String(), nil, 404)
	if err != nil {
		return nil, lib.BlockHeader{}, lib.WrapErrorf(err, "failed to read block")
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, lib.BlockHeader{}, lib.ErrBlockNotFound
	}
	header, err := lib.UnmarshalBlockHeader(blockId, bytes.NewReader(resp.Body))
	if err != nil {
		return nil, lib.BlockHeader{}, lib.WrapErrorf(err, "failed to unmarshal block header")
	}
	data := resp.Body[lib.BlockHeaderSize:]
	if err != nil {
		return nil, lib.BlockHeader{}, lib.WrapErrorf(err, "failed to read block data")
	}
	if len(data) != int(header.EncryptedDataSize) {
		return nil, lib.BlockHeader{}, lib.Errorf(
			"read %d bytes, expected %d",
			len(data),
			header.EncryptedDataSize,
		)
	}
	return data, header, nil
}

func (c *HTTPStorageClient) ReadBlockHeader(blockId lib.BlockId) (lib.BlockHeader, error) {
	resp, err := c.request(http.MethodGet, "/storage/block/"+blockId.String()+"/header", nil, 404)
	if err != nil {
		return lib.BlockHeader{}, lib.WrapErrorf(err, "failed to read block header")
	}
	if resp.StatusCode == http.StatusNotFound {
		return lib.BlockHeader{}, lib.ErrBlockNotFound
	}
	body := bytes.NewReader(resp.Body)
	header, err := lib.UnmarshalBlockHeader(blockId, body)
	if err != nil {
		return lib.BlockHeader{}, lib.WrapErrorf(err, "failed to unmarshal block header")
	}
	return header, nil
}

func (c *HTTPStorageClient) WriteBlock(block lib.Block) (bool, error) {
	headerBytes := bytes.NewBuffer(nil)
	if err := lib.MarshalBlockHeader(&block.Header, headerBytes); err != nil {
		return false, lib.WrapErrorf(err, "failed to marshal block header")
	}
	r := io.MultiReader(headerBytes, bytes.NewReader(block.EncryptedData))
	body, err := io.ReadAll(r)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to read block data")
	}
	resp, err := c.request(http.MethodPut, "/storage/block/"+block.Header.BlockId.String(), body)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to write block")
	}
	return resp.StatusCode != http.StatusCreated, nil
}

func (c *HTTPStorageClient) HasControlFile(section lib.ControlFileSection, name string) (bool, error) {
	resp, err := c.request(http.MethodHead, "/storage/control/"+string(section)+"/"+name, nil, 404)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to check if control file exists")
	}
	return resp.StatusCode == http.StatusOK, nil
}

func (c *HTTPStorageClient) ReadControlFile(section lib.ControlFileSection, name string) ([]byte, error) {
	resp, err := c.request(http.MethodGet, "/storage/control/"+string(section)+"/"+name, nil, 404)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read control file")
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, lib.ErrControlFileNotFound
	}
	return resp.Body, nil
}

func (c *HTTPStorageClient) WriteControlFile(section lib.ControlFileSection, name string, data []byte) error {
	resp, err := c.request(http.MethodPut, "/storage/control/"+string(section)+"/"+name, data)
	if err != nil {
		return lib.WrapErrorf(err, "failed to write control file")
	}
	if resp.StatusCode != http.StatusOK {
		return lib.Errorf("failed to write control file: got %d (%s)", resp.StatusCode, string(data))
	}
	return nil
}

func (c *HTTPStorageClient) DeleteControlFile(section lib.ControlFileSection, name string) error {
	resp, err := c.request(http.MethodDelete, "/storage/control/"+string(section)+"/"+name, nil, 404)
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

func (c *HTTPStorageClient) request(
	method, path string,
	body []byte,
	ignoreStatusCodes ...int,
) (*HTTPResponse, error) {
	resp, err := c.Client.Request(method, c.Address+path, body)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to execute request")
	}
	if resp.StatusCode >= 400 && !slices.Contains(ignoreStatusCodes, resp.StatusCode) {
		return nil, lib.Errorf("failed to open storage: got %d (%s)", resp.StatusCode, string(resp.Body))
	}
	return resp, nil
}

type HTTPStorageServer struct {
	Storage *lib.FileStorage
	Address string
}

func NewHTTPStorageServer(storage *lib.FileStorage, address string) *HTTPStorageServer {
	return &HTTPStorageServer{storage, address}
}

func (s *HTTPStorageServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /storage/open", s.Open)
	mux.HandleFunc("HEAD /storage/block/{id}", s.HasBlock)
	mux.HandleFunc("GET /storage/block/{id}", s.ReadBlock)
	mux.HandleFunc("GET /storage/block/{id}/header", s.ReadBlockHeader)
	mux.HandleFunc("PUT /storage/block/{id}", s.WriteBlock)
	mux.HandleFunc("HEAD /storage/control/{section}/{name}", s.HasControlFile)
	mux.HandleFunc("GET /storage/control/{section}/{name}", s.ReadControlFile)
	mux.HandleFunc("PUT /storage/control/{section}/{name}", s.WriteControlFile)
	mux.HandleFunc("DELETE /storage/control/{section}/{name}", s.DeleteControlFile)
}

// Return the storage TOML as a string.
func (s *HTTPStorageServer) Open(w http.ResponseWriter, r *http.Request) {
	toml, err := s.Storage.Open()
	if err != nil {
		s.error(lib.WrapErrorf(err, "failed to open storage"), w, http.StatusInternalServerError)
		return
	}
	if err := lib.WriteToml(w, "", toml); err != nil {
		s.error(lib.WrapErrorf(err, "failed to write storage TOML"), w, http.StatusInternalServerError)
		return
	}
}

func (s *HTTPStorageServer) HasBlock(w http.ResponseWriter, r *http.Request) {
	hexId := r.PathValue("id")
	id, err := hex.DecodeString(hexId)
	if err != nil {
		s.error(lib.WrapErrorf(err, "invalid block ID"), w, http.StatusBadRequest)
		return
	}
	exists, err := s.Storage.HasBlock(lib.BlockId(id))
	if err != nil {
		s.error(lib.WrapErrorf(err, "failed to check if block exists"), w, http.StatusInternalServerError)
		return
	}
	if exists {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// Return the block with the given ID.
// The response body is binary:
//   - `lib.BlockHeaderSize` bytes: the block header (see `lib.UnmarshalBlockHeader`)
//   - encrypted block data
func (s *HTTPStorageServer) ReadBlock(w http.ResponseWriter, r *http.Request) {
	hexId := r.PathValue("id")
	id, err := hex.DecodeString(hexId)
	if err != nil {
		s.error(lib.WrapErrorf(err, "invalid block ID"), w, http.StatusBadRequest)
		return
	}
	data, header, err := s.Storage.ReadBlock(lib.BlockId(id))
	if err != nil {
		if errors.Is(err, lib.ErrBlockNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.error(lib.WrapErrorf(err, "failed to read block"), w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)+lib.BlockHeaderSize))
	if err := lib.MarshalBlockHeader(&header, w); err != nil {
		s.error(lib.WrapErrorf(err, "failed to marshal block header"), w, http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(data); err != nil {
		s.error(lib.WrapErrorf(err, "failed to write block data"), w, http.StatusInternalServerError)
		return
	}
}

func (s *HTTPStorageServer) ReadBlockHeader(w http.ResponseWriter, r *http.Request) {
	hexId := r.PathValue("id")
	id, err := hex.DecodeString(hexId)
	if err != nil {
		s.error(lib.WrapErrorf(err, "invalid block ID"), w, http.StatusBadRequest)
		return
	}
	header, err := s.Storage.ReadBlockHeader(lib.BlockId(id))
	if err != nil {
		if errors.Is(err, lib.ErrBlockNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.error(lib.WrapErrorf(err, "failed to read block header"), w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(lib.BlockHeaderSize))
	if err := lib.MarshalBlockHeader(&header, w); err != nil {
		s.error(lib.WrapErrorf(err, "failed to marshal block header"), w, http.StatusInternalServerError)
		return
	}
}

// Write a block. The request body must be binary:
//   - `lib.BlockHeaderSize` bytes: the block header (see `lib.UnmarshalBlockHeader`)
//   - encrypted block data
//
// Return `200 OK` if the block already existed.
// Return `201 Created` if the block was written.
func (s *HTTPStorageServer) WriteBlock(w http.ResponseWriter, r *http.Request) {
	hexId := r.PathValue("id")
	id, err := hex.DecodeString(hexId)
	if err != nil {
		s.error(lib.WrapErrorf(err, "invalid block ID"), w, http.StatusBadRequest)
		return
	}
	header, err := lib.UnmarshalBlockHeader(lib.BlockId(id), r.Body)
	if err != nil {
		s.error(lib.WrapErrorf(err, "failed to unmarshal block header"), w, http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		s.error(lib.WrapErrorf(err, "failed to read block data"), w, http.StatusBadRequest)
		return
	}
	exists, err := s.Storage.WriteBlock(lib.Block{Header: header, EncryptedData: data})
	if err != nil {
		s.error(lib.WrapErrorf(err, "failed to write block"), w, http.StatusInternalServerError)
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
		s.error(lib.WrapErrorf(err, "failed to check if control file exists"), w, http.StatusInternalServerError)
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
		s.error(lib.WrapErrorf(err, "failed to read control file"), w, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if _, err := w.Write(data); err != nil {
		s.error(lib.WrapErrorf(err, "failed to write control file data"), w, http.StatusInternalServerError)
		return
	}
}

func (s *HTTPStorageServer) WriteControlFile(w http.ResponseWriter, r *http.Request) {
	section := r.PathValue("section")
	name := r.PathValue("name")
	data, err := io.ReadAll(r.Body)
	if err != nil {
		s.error(lib.WrapErrorf(err, "failed to read control file data"), w, http.StatusBadRequest)
		return
	}
	if err := s.Storage.WriteControlFile(lib.ControlFileSection(section), name, data); err != nil {
		s.error(lib.WrapErrorf(err, "failed to write control file"), w, http.StatusInternalServerError)
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
		s.error(lib.WrapErrorf(err, "failed to delete control file"), w, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *HTTPStorageServer) error(wrappedError *lib.WrappedError, w http.ResponseWriter, status int) {
	slog.Error("HTTP error", "error", wrappedError)
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "text/plain")
	if _, err := w.Write([]byte(wrappedError.Msg)); err != nil {
		slog.Error("Failed to write error response", "error", err)
		return
	}
}
