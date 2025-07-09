//nolint:bodyclose,noctx
package http

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

var td = lib.TestData{} //nolint:gochecknoglobals

func TestHTTPStorageClient(t *testing.T) {
	t.Parallel()

	t.Run("HTTPStorageClient implements lib.Storage", func(t *testing.T) {
		t.Parallel()
		_, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))
		var _ lib.Storage = client
	})

	t.Run("Open", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))
		toml, err := client.Open()
		assert.NoError(err)
		assert.Equal(lib.Toml{"some": {"key": "value"}}, toml)
	})

	t.Run("HasBlock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))
		blockId := td.BlockId("1")

		ok, err := client.HasBlock(blockId)
		assert.NoError(err)
		assert.Equal(false, ok)

		testWriteBlock(t, storage, blockId, []byte("abcd"))
		ok, err = client.HasBlock(blockId)
		assert.NoError(err)
		assert.Equal(true, ok)
	})

	t.Run("ReadBlock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))
		blockId := td.BlockId("1")

		_, _, err := client.ReadBlock(blockId)
		assert.ErrorIs(err, lib.ErrBlockNotFound)

		header := testWriteBlock(t, storage, blockId, []byte("abcd"))
		data, readHeader, err := client.ReadBlock(blockId)
		assert.NoError(err)
		assert.Equal(header, readHeader)
		assert.Equal([]byte("abcd"), data)
	})

	t.Run("ReadBlockHeader", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))

		blockId := td.BlockId("1")
		_, err := client.ReadBlockHeader(blockId)
		assert.ErrorIs(err, lib.ErrBlockNotFound)

		header := testWriteBlock(t, storage, blockId, []byte("abcd"))
		readHeader, err := client.ReadBlockHeader(blockId)
		assert.NoError(err)
		assert.Equal(header, readHeader)
	})

	t.Run("WriteBlock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))

		blockId := td.BlockId("1")
		block := lib.Block{
			Header: lib.BlockHeader{
				EncryptedDEK:      td.EncryptedKey("1"),
				BlockId:           blockId,
				Flags:             0,
				EncryptedDataSize: 5,
			},
			EncryptedData: []byte("abcde"),
		}
		ok, err := client.WriteBlock(block)
		assert.NoError(err)
		assert.Equal(false, ok)

		data, readHeader, err := storage.ReadBlock(blockId)
		assert.NoError(err)
		assert.Equal(block.Header, readHeader)
		assert.Equal(block.EncryptedData, data)

		// Write the same block again, it should go through but
		// return `true` (i.e. block existed before).
		ok, err = client.WriteBlock(block)
		assert.NoError(err)
		assert.Equal(true, ok)
	})

	t.Run("HasControlFile", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))

		ok, err := client.HasControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(false, ok)

		err = storage.WriteControlFile(lib.ControlFileSectionRefs, "head", []byte("1234"))
		assert.NoError(err)
		ok, err = client.HasControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(true, ok)
	})

	t.Run("ReadControlFile", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))

		_, err := client.ReadControlFile(lib.ControlFileSectionRefs, "head")
		assert.ErrorIs(err, lib.ErrControlFileNotFound)

		err = storage.WriteControlFile(lib.ControlFileSectionRefs, "head", []byte("abcd"))
		assert.NoError(err)
		data, err := client.ReadControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("abcd"), data)
	})

	t.Run("WriteControlFile", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))

		err := client.WriteControlFile(lib.ControlFileSectionRefs, "head", []byte("abcd"))
		assert.NoError(err)
		data, err := storage.ReadControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("abcd"), data)

		err = client.WriteControlFile(lib.ControlFileSectionRefs, "head", []byte("1234"))
		assert.NoError(err)
		data, err = storage.ReadControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("1234"), data)
	})

	t.Run("DeleteControlFile", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client(), t.Context()))

		err := client.DeleteControlFile(lib.ControlFileSectionRefs, "head")
		assert.ErrorIs(err, lib.ErrControlFileNotFound)

		err = storage.WriteControlFile(lib.ControlFileSectionRefs, "head", []byte("abcd"))
		assert.NoError(err)

		err = client.DeleteControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)

		ok, err := storage.HasControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(false, ok)
	})
}

func TestHTTPStorageServer(t *testing.T) {
	t.Parallel()
	t.Run("Open", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, srv := newSut(t)
		defer srv.Close()

		resp, err := http.Get(srv.URL + "/storage/open")
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
		toml, err := lib.ReadToml(resp.Body)
		assert.NoError(err)
		assert.Equal(lib.Toml{"some": {"key": "value"}}, toml)
	})

	t.Run("HasBlock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		blockId := td.BlockId("1")
		testWriteBlock(t, storage, blockId, []byte("abcd"))

		resp, err := http.Head(srv.URL + "/storage/block/" + td.BlockId("not_found").String())
		assert.NoError(err)
		assert.Equal(404, resp.StatusCode)
		resp, err = http.Head(srv.URL + "/storage/block/" + blockId.String())
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
	})

	t.Run("ReadBlock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		blockId := td.BlockId("1")
		testWriteBlock(t, storage, blockId, []byte("abcd"))

		resp, err := http.Get(srv.URL + "/storage/block/" + td.BlockId("not_found").String())
		assert.NoError(err)
		assert.Equal(404, resp.StatusCode)

		resp, err = http.Get(srv.URL + "/storage/block/" + blockId.String())
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
		assert.Equal(strconv.Itoa(lib.BlockHeaderSize+4), resp.Header.Get("Content-Length"))
		assert.Equal("application/octet-stream", resp.Header.Get("Content-Type"))
	})

	t.Run("ReadBlockHeader", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		blockId := td.BlockId("1")
		testWriteBlock(t, storage, blockId, []byte("abcd"))

		resp, err := http.Get(srv.URL + "/storage/block/" + td.BlockId("not_found").String() + "/header")
		assert.NoError(err)
		assert.Equal(404, resp.StatusCode)

		resp, err = http.Get(srv.URL + "/storage/block/" + blockId.String() + "/header")
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
		assert.Equal(strconv.Itoa(lib.BlockHeaderSize), resp.Header.Get("Content-Length"))
		assert.Equal("application/octet-stream", resp.Header.Get("Content-Type"))
	})

	t.Run("WriteBlock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()

		blockId := td.BlockId("1")
		header := lib.BlockHeader{
			EncryptedDEK:      td.EncryptedKey("1"),
			BlockId:           blockId,
			Flags:             lib.BlockFlagDeflate,
			EncryptedDataSize: 8,
		}
		data := []byte("abcdefgh")
		buf := bytes.NewBuffer(nil)
		err := lib.MarshalBlockHeader(&header, buf)
		assert.NoError(err)
		_, err = buf.Write(data)
		assert.NoError(err)
		req, err := http.NewRequest(http.MethodPut, srv.URL+"/storage/block/"+blockId.String(), buf)
		assert.NoError(err)
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(err)
		assert.Equal(201, resp.StatusCode)
		data, readHeader, err := storage.ReadBlock(blockId)
		assert.NoError(err)
		assert.Equal(header, readHeader)
		assert.Equal([]byte("abcdefgh"), data)

		// Write the same block again, it should go through but return `200 OK`.
		buf = bytes.NewBuffer(nil)
		err = lib.MarshalBlockHeader(&header, buf)
		assert.NoError(err)
		_, err = buf.Write(data)
		assert.NoError(err)
		req, err = http.NewRequest(http.MethodPut, srv.URL+"/storage/block/"+blockId.String(), buf)
		assert.NoError(err)
		resp, err = http.DefaultClient.Do(req)
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
	})

	t.Run("HasControlFile", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		resp, err := http.Head(srv.URL + "/storage/control/refs/head")
		assert.NoError(err)
		assert.Equal(404, resp.StatusCode)

		err = storage.WriteControlFile(lib.ControlFileSectionRefs, "head", []byte("1234"))
		assert.NoError(err)
		resp, err = http.Head(srv.URL + "/storage/control/refs/head")
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
	})

	t.Run("ReadControlFile", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		resp, err := http.Get(srv.URL + "/storage/control/refs/head")
		assert.NoError(err)
		assert.Equal(404, resp.StatusCode)

		err = storage.WriteControlFile(lib.ControlFileSectionRefs, "head", []byte("1234"))
		assert.NoError(err)
		resp, err = http.Get(srv.URL + "/storage/control/refs/head")
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)
		assert.Equal("application/octet-stream", resp.Header.Get("Content-Type"))
		assert.Equal("4", resp.Header.Get("Content-Length"))
		body, err := io.ReadAll(resp.Body)
		assert.NoError(err)
		assert.Equal([]byte("1234"), body)
	})

	t.Run("WriteControlFile", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()

		req, err := http.NewRequest(
			http.MethodPut,
			srv.URL+"/storage/control/refs/head",
			bytes.NewReader([]byte("1234")),
		)
		assert.NoError(err)
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)

		data, err := storage.ReadControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("1234"), data)

		// Overwrite the control file.
		req, err = http.NewRequest(
			http.MethodPut,
			srv.URL+"/storage/control/refs/head",
			bytes.NewReader([]byte("abcd")),
		)
		assert.NoError(err)
		resp, err = http.DefaultClient.Do(req)
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)

		data, err = storage.ReadControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal([]byte("abcd"), data)
	})

	t.Run("DeleteControlFile", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		req, err := http.NewRequest(http.MethodDelete, srv.URL+"/storage/control/refs/head", nil)
		assert.NoError(err)
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(err)
		assert.Equal(404, resp.StatusCode)

		err = storage.WriteControlFile(lib.ControlFileSectionRefs, "head", []byte("1234"))
		assert.NoError(err)
		resp, err = http.DefaultClient.Do(req)
		assert.NoError(err)
		assert.Equal(200, resp.StatusCode)

		ok, err := storage.HasControlFile(lib.ControlFileSectionRefs, "head")
		assert.NoError(err)
		assert.Equal(false, ok)
	})
}

func testWriteBlock(t *testing.T, storage *lib.FileStorage, blockId lib.BlockId, data []byte) lib.BlockHeader {
	t.Helper()
	assert := lib.NewAssert(t)
	header := lib.BlockHeader{
		EncryptedDEK:      td.EncryptedKey(blockId.String()),
		BlockId:           blockId,
		Flags:             0,
		EncryptedDataSize: uint32(len(data)), //nolint:gosec
	}
	_, err := storage.WriteBlock(lib.Block{Header: header, EncryptedData: data})
	assert.NoError(err)
	return header
}

func testStorage(t *testing.T) *lib.FileStorage {
	t.Helper()
	assert := lib.NewAssert(t)
	storage, err := lib.NewFileStorage(td.NewFS(t), lib.StoragePurposeRepository)
	assert.NoError(err)
	err = storage.Init(lib.Toml{"some": {"key": "value"}}, "some header comment")
	assert.NoError(err)
	return storage
}

func newSut(t *testing.T) (*lib.FileStorage, *httptest.Server) {
	t.Helper()
	storage := testStorage(t)
	sut := NewHTTPStorageServer(storage, ":9999")
	mux := http.NewServeMux()
	sut.RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	return storage, srv
}
