//nolint:bodyclose
package http

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

var td = lib.TestData{} //nolint:gochecknoglobals

func TestHTTPStorageClient(t *testing.T) {
	oldLeaseMin := LockLeaseMin
	t.Cleanup(func() { LockLeaseMin = oldLeaseMin })
	LockLeaseMin = 100 * time.Millisecond

	t.Parallel()

	t.Run("HTTPStorageClient implements lib.Storage", func(t *testing.T) {
		t.Parallel()
		_, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))
		var _ lib.Storage = client
	})

	t.Run("Open", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))
		toml, err := client.Open()
		assert.NoError(err)
		assert.Equal(lib.Toml{"some": {"key": "value"}}, toml)
	})

	t.Run("HasBlock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))
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
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))
		blockId := td.BlockId("1")

		buf := lib.NewBlockBuf()
		_, err := client.ReadBlock(blockId, buf)
		assert.ErrorIs(err, lib.ErrBlockNotFound)

		testWriteBlock(t, storage, blockId, []byte("abcd"))
		data, err := client.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal([]byte("abcd"), data)
	})

	t.Run("WriteBlock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))

		blockId := td.BlockId("1")
		data := []byte("abcde")
		ok, err := client.WriteBlock(blockId, data)
		assert.NoError(err)
		assert.Equal(false, ok)

		buf := lib.NewBlockBuf()
		readData, err := storage.ReadBlock(blockId, buf)
		assert.NoError(err)
		assert.Equal(data, readData)

		// Write the same block again, it should go through but
		// return `true` (i.e. block existed before).
		ok, err = client.WriteBlock(blockId, data)
		assert.NoError(err)
		assert.Equal(true, ok)
	})

	t.Run("WriteBlock rejects bodies larger than MaxBlockSize", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))
		_, err := client.WriteBlock(td.BlockId("1"), make([]byte, lib.MaxBlockSize+1))
		assert.Error(err, "is too large")
	})

	t.Run("WriteBlock/ReadBlock a MaxBlockSize block", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))
		blockId := td.BlockId("1")
		data := make([]byte, lib.MaxBlockSize)
		_, _ = rand.Read(data)
		testWriteBlock(t, storage, blockId, data)
		readData, err := client.ReadBlock(blockId, lib.NewBlockBuf())
		assert.NoError(err)
		assert.Equal(data, readData)
	})

	t.Run("HasControlFile", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))

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
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))

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
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))

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
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))

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

	t.Run("Lock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))

		unlock, err := client.Lock(t.Context(), "lock")
		assert.NoError(err)

		t0 := time.Now()
		ctx2, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		_, err = client.Lock(ctx2, "lock")
		assert.ErrorIs(err, context.DeadlineExceeded)
		assert.Greater(time.Since(t0), 99*time.Millisecond)

		err = unlock()
		assert.NoError(err)

		unlock2, err := client.Lock(t.Context(), "lock")
		assert.NoError(err)
		err = unlock2()
		assert.NoError(err)
	})

	t.Run("Lock continuously extends the lease", func(t *testing.T) {
		t.Parallel()

		assert := lib.NewAssert(t)
		_, srv := newSut(t)
		defer srv.Close()
		client := NewHTTPStorageClient(srv.URL, NewDefaultHTTPClient(srv.Client()))

		unlock, err := client.Lock(t.Context(), "lock")
		assert.NoError(err)
		defer unlock() //nolint:errcheck

		// Trying to lock again should fail.
		ctx2, cancel := context.WithTimeout(t.Context(), 1*time.Second)
		defer cancel()
		_, err = client.Lock(ctx2, "lock")
		assert.ErrorIs(err, context.DeadlineExceeded)
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
		assert.Equal(strconv.Itoa(4), resp.Header.Get("Content-Length"))
		assert.Equal("application/octet-stream", resp.Header.Get("Content-Type"))
	})

	t.Run("WriteBlock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, srv := newSut(t)
		defer srv.Close()

		blockId := td.BlockId("1")
		data := []byte("abcdefgh")
		req, err := http.NewRequest(http.MethodPut, srv.URL+"/storage/block/"+blockId.String(), bytes.NewReader(data))
		assert.NoError(err)
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(err)
		assert.Equal(201, resp.StatusCode)
		blockBuf := lib.NewBlockBuf()
		readData, err := storage.ReadBlock(blockId, blockBuf)
		assert.NoError(err)
		assert.Equal(data, readData)

		// Write the same block again, it should go through but return `200 OK`.
		req, err = http.NewRequest(http.MethodPut, srv.URL+"/storage/block/"+blockId.String(), bytes.NewReader(data))
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

func testWriteBlock(t *testing.T, storage *lib.FileStorage, blockId lib.BlockId, data []byte) {
	t.Helper()
	assert := lib.NewAssert(t)
	_, err := storage.WriteBlock(blockId, data)
	assert.NoError(err)
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
