//nolint:bodyclose,forbidigo
package http

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/xml"
	stderrors "errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

const (
	testAccessKey = "test-access-key"
	testSecret    = "test-secret-key"
	testRegion    = "fr-par"
)

var td = lib.TestData{} //nolint:gochecknoglobals

var s3PrefixSeq atomic.Uint64 //nolint:gochecknoglobals

// TestS3StorageLocal exercises the S3 client + server combo using a local
// httptest server backed by a fresh empty FileStorage per subtest.
func TestS3StorageLocal(t *testing.T) {
	t.Parallel()
	checkS3Storage(t, func(t *testing.T) (S3StorageConfig, HTTPClient) { //nolint:thelper
		assert := lib.NewAssert(t)
		storage, err := lib.NewFileStorage(td.NewFS(t), lib.StoragePurposeRepository)
		assert.NoError(err)
		srv := newServerForStorage(t, storage)
		return S3StorageConfig{
			BucketURL:       srv.URL,
			Region:          testRegion,
			Prefix:          "",
			AccessKeyID:     testAccessKey,
			SecretAccessKey: []byte(testSecret),
		}, NewDefaultHTTPClient(srv.Client())
	})
}

// TestS3StorageScaleway exercises the same contract against a real
// Scaleway-style S3 bucket. Skipped unless `.env` (or the process
// environment) provides TEST_S3_URL, TEST_S3_ACCESS_KEY,
// TEST_S3_SECRET_KEY.
func TestS3StorageScaleway(t *testing.T) {
	t.Parallel()
	bucketURL, ak, sk := loadS3Creds(t)
	if bucketURL == "" || ak == "" || sk == "" {
		t.Skip("Scaleway S3 not configured (set TEST_S3_URL, TEST_S3_ACCESS_KEY, TEST_S3_SECRET_KEY)")
	}
	u, err := url.Parse(bucketURL)
	if err != nil {
		t.Fatalf("invalid TEST_S3_URL: %v", err)
	}
	region := regionFromHost(u.Host)
	bucketURL = strings.TrimRight(bucketURL, "/")
	checkS3Storage(t, func(t *testing.T) (S3StorageConfig, HTTPClient) { //nolint:thelper
		prefix := fmt.Sprintf("cling-test/%s-%d",
			time.Now().UTC().Format("2006-01-02T15-04-05.000000000"), s3PrefixSeq.Add(1))
		return S3StorageConfig{
			BucketURL:       bucketURL,
			Region:          region,
			Prefix:          prefix,
			AccessKeyID:     ak,
			SecretAccessKey: []byte(sk),
		}, NewDefaultHTTPClient(nil)
	})
}

// TestS3StorageConcurrency hammers the block and control-file methods from many
// goroutines. Lock has its own test. ReadBlockIds is serialised server-side.
func TestS3StorageConcurrency(t *testing.T) {
	t.Parallel()

	// Fresh blocks per round so writes never dedup. Pool < workers for overlap.
	const (
		poolSize = 4
		rounds   = 20
	)
	poolIDs := make([][]lib.BlockId, rounds)
	poolData := make([][][]byte, rounds)
	for round := range rounds {
		poolIDs[round] = make([]lib.BlockId, poolSize)
		poolData[round] = make([][]byte, poolSize)
		for j := range poolSize {
			poolIDs[round][j] = td.BlockId(fmt.Sprintf("pool-%d-%d", round, j))
			poolData[round][j] = fmt.Appendf(nil, "pool %d-%d", round, j)
		}
	}

	setup := func(t *testing.T) (*S3StorageClient, S3StorageConfig, HTTPClient) {
		t.Helper()
		assert := lib.NewAssert(t)
		storage, err := lib.NewFileStorage(td.NewFS(t), lib.StoragePurposeRepository)
		assert.NoError(err)
		srv := newServerForStorage(t, storage)
		cfg := S3StorageConfig{
			BucketURL:       srv.URL,
			Region:          testRegion,
			Prefix:          "",
			AccessKeyID:     testAccessKey,
			SecretAccessKey: []byte(testSecret),
		}
		httpC := NewDefaultHTTPClient(srv.Client())
		c := NewS3StorageClient(cfg, httpC)
		assert.NoError(c.Init(t.Context(), lib.Toml{"some": {"key": "value"}}, ""))
		return c, cfg, httpC
	}

	work := func(ctx context.Context, c *S3StorageClient, i int) error {
		id := td.BlockId(strconv.Itoa(i))
		name := "cf" + strconv.Itoa(i)
		want := []byte("block " + strconv.Itoa(i))
		buf := lib.NewBlockBuf()
		if _, err := c.WriteBlock(ctx, id, want); err != nil {
			return err
		}
		if _, err := c.HasBlock(ctx, id); err != nil {
			return err
		}
		got, err := c.ReadBlock(ctx, id, buf)
		if err != nil {
			return err
		}
		if string(got) != string(want) {
			return fmt.Errorf("block %d: read %q, want %q", i, got, want)
		}
		if _, err := c.Open(ctx); err != nil {
			return err
		}
		if err := c.WriteControlFile(ctx, lib.ControlFileSectionRefs, name, want); err != nil {
			return err
		}
		if _, err := c.HasControlFile(ctx, lib.ControlFileSectionRefs, name); err != nil {
			return err
		}
		cf, err := c.ReadControlFile(ctx, lib.ControlFileSectionRefs, name)
		if err != nil {
			return err
		}
		if string(cf) != string(want) {
			return fmt.Errorf("control file %s: read %q, want %q", name, cf, want)
		}
		if err := c.DeleteControlFile(ctx, lib.ControlFileSectionRefs, name); err != nil {
			return err
		}

		// Stress the write path. Many goroutines write the same fresh block.
		for round := range rounds {
			j := mrand.IntN(poolSize)
			if _, err := c.WriteBlock(ctx, poolIDs[round][j], poolData[round][j]); err != nil {
				return err
			}
			got, err := c.ReadBlock(ctx, poolIDs[round][j], buf)
			if err != nil {
				return err
			}
			if string(got) != string(poolData[round][j]) {
				return fmt.Errorf("pool %d-%d: read %q, want %q", round, j, got, poolData[round][j])
			}
		}
		return nil
	}

	run := func(t *testing.T, client func() *S3StorageClient) {
		t.Helper()
		assert := lib.NewAssert(t)
		const workers = 16
		var wg sync.WaitGroup
		errs := make([]error, workers)
		for i := range workers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				errs[i] = work(t.Context(), client(), i)
			}(i)
		}
		wg.Wait()
		for _, err := range errs {
			assert.NoError(err)
		}
	}

	// One shared client stresses the client's own shared state.
	t.Run("One shared client", func(t *testing.T) {
		t.Parallel()
		c, _, _ := setup(t)
		run(t, func() *S3StorageClient { return c })
	})

	// One client per goroutine stresses the server with independent clients.
	t.Run("One client per goroutine", func(t *testing.T) {
		t.Parallel()
		_, cfg, httpC := setup(t)
		run(t, func() *S3StorageClient { return NewS3StorageClient(cfg, httpC) })
	})
}

// checkS3Storage runs the shared S3 contract tests against any backend
// produced by `newSut`. `newSut` must return a config + transport pointing
// at an empty (uninitialised) storage instance.
func checkS3Storage(t *testing.T, newSut func(*testing.T) (S3StorageConfig, HTTPClient)) {
	t.Helper()

	newClient := func(t *testing.T) *S3StorageClient { //nolint:thelper
		cfg, httpC := newSut(t)
		return NewS3StorageClient(cfg, httpC)
	}
	initClient := func(t *testing.T) *S3StorageClient { //nolint:thelper
		c := newClient(t)
		if err := c.Init(t.Context(), lib.Toml{"some": {"key": "value"}}, "test header"); err != nil {
			t.Fatal(err)
		}
		return c
	}

	t.Run("S3StorageClient implements lib.Storage", func(t *testing.T) {
		t.Parallel()
		var _ lib.Storage = newClient(t)
	})

	t.Run("Init creates the storage and Open reads the TOML back", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		c := newClient(t)
		toml := lib.Toml{"some": {"key": "value"}}
		assert.NoError(c.Init(t.Context(), toml, "header"))
		got, err := c.Open(t.Context())
		assert.NoError(err)
		assert.Equal(toml, got)
	})

	t.Run("Init twice should fail with ErrStorageAlreadyExists", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		c := initClient(t)
		assert.ErrorIs(c.Init(t.Context(), lib.Toml{"x": {"y": "z"}}, ""), lib.ErrStorageAlreadyExists)
	})

	t.Run("Open on uninitialised storage should return ErrStorageNotFound", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, err := newClient(t).Open(t.Context())
		assert.ErrorIs(err, lib.ErrStorageNotFound)
	})

	t.Run("WriteBlock + HasBlock + ReadBlock roundtrip", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		c := initClient(t)
		blockId := td.BlockId("1")
		data := []byte("abcde")

		ok, err := c.HasBlock(t.Context(), blockId)
		assert.NoError(err)
		assert.Equal(false, ok)

		existed, err := c.WriteBlock(t.Context(), blockId, data)
		assert.NoError(err)
		assert.Equal(false, existed)

		ok, err = c.HasBlock(t.Context(), blockId)
		assert.NoError(err)
		assert.Equal(true, ok)

		// Re-write reports existed=true (idempotent on content-addressed blocks).
		existed, err = c.WriteBlock(t.Context(), blockId, data)
		assert.NoError(err)
		assert.Equal(true, existed)

		got, err := c.ReadBlock(t.Context(), blockId, lib.NewBlockBuf())
		assert.NoError(err)
		assert.Equal(data, got)
	})

	t.Run("ReadBlock on missing block should return ErrBlockNotFound", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, err := initClient(t).ReadBlock(t.Context(), td.BlockId("missing"), lib.NewBlockBuf())
		assert.ErrorIs(err, lib.ErrBlockNotFound)
	})

	t.Run("WriteBlock should reject bodies larger than MaxBlockSize", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, err := initClient(t).WriteBlock(t.Context(), td.BlockId("1"), make([]byte, lib.MaxBlockSize+1))
		assert.Error(err, "is too large")
	})

	t.Run("WriteBlock/ReadBlock at MaxBlockSize", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		c := initClient(t)
		data := make([]byte, lib.MaxBlockSize)
		_, _ = rand.Read(data)
		_, err := c.WriteBlock(t.Context(), td.BlockId("max"), data)
		assert.NoError(err)
		got, err := c.ReadBlock(t.Context(), td.BlockId("max"), lib.NewBlockBuf())
		assert.NoError(err)
		assert.Equal(data, got)
	})

	t.Run("ReadBlockIds enumerates stored blocks", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		c := initClient(t)
		ids := []lib.BlockId{td.BlockId("1"), td.BlockId("2"), td.BlockId("3")}
		for _, id := range ids {
			_, err := c.WriteBlock(t.Context(), id, []byte("data"))
			assert.NoError(err)
		}
		var got []lib.BlockId
		assert.NoError(c.ReadBlockIds(t.Context(), func(id lib.BlockId) bool {
			got = append(got, id)
			return true
		}))
		slices.SortFunc(got, lib.BlockIdCompare)
		slices.SortFunc(ids, lib.BlockIdCompare)
		assert.Equal(ids, got)
	})

	t.Run("ReadBlockIds on empty storage yields nothing", func(t *testing.T) {
		t.Parallel()
		err := initClient(t).ReadBlockIds(t.Context(), func(id lib.BlockId) bool {
			t.Fatalf("unexpected id %s", id)
			return true
		})
		lib.NewAssert(t).NoError(err)
	})

	t.Run("Lock under concurrent contention: exactly one winner per name", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cfg, httpC := newSut(t)
		freshClient := NewS3StorageClient(cfg, httpC)
		assert.NoError(freshClient.Init(t.Context(), lib.Toml{}, ""))
		const concurrency = 12
		const rounds = 4
		for round := range rounds {
			name := fmt.Sprintf("race-lock-%d", round)
			unlocks := make([]func() error, concurrency)
			errs := make([]error, concurrency)
			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					// Each goroutine uses its own client so they don't share
					// the in-process lockState.
					client := NewS3StorageClient(cfg, httpC)
					unlocks[i], errs[i] = client.Lock(t.Context(), name)
				}(i)
			}
			wg.Wait()
			winners, losers := 0, 0
			for i := range concurrency {
				if errs[i] == nil {
					winners++
					_ = unlocks[i]()
				} else {
					var existsErr *lib.LockExistsError
					if stderrors.As(errs[i], &existsErr) {
						losers++
					} else {
						t.Errorf("round %d goroutine %d: unexpected error %v", round, i, errs[i])
					}
				}
			}
			assert.Equal(1, winners, fmt.Sprintf("round %d: expected exactly one Lock winner, got %d", round, winners))
			assert.Equal(concurrency-1, losers,
				fmt.Sprintf("round %d: expected %d LockExistsError losers, got %d", round, concurrency-1, losers))
		}
	})

	t.Run("ControlFile CRUD on every section", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		c := initClient(t)
		sections := []lib.ControlFileSection{
			lib.ControlFileSectionRefs,
			lib.ControlFileSectionSecurity,
			lib.ControlFileSectionConf,
		}
		for _, section := range sections {
			ok, err := c.HasControlFile(t.Context(), section, "head")
			assert.NoError(err)
			assert.Equal(false, ok)
			_, err = c.ReadControlFile(t.Context(), section, "head")
			assert.ErrorIs(err, lib.ErrControlFileNotFound)

			assert.NoError(c.WriteControlFile(t.Context(), section, "head", []byte("abcd")))
			got, err := c.ReadControlFile(t.Context(), section, "head")
			assert.NoError(err)
			assert.Equal([]byte("abcd"), got)

			assert.NoError(c.WriteControlFile(t.Context(), section, "head", []byte("1234")))
			got, err = c.ReadControlFile(t.Context(), section, "head")
			assert.NoError(err)
			assert.Equal([]byte("1234"), got)

			assert.NoError(c.DeleteControlFile(t.Context(), section, "head"))
			// Note: real S3 returns 204 (idempotent) on delete of a missing
			// key while our server-on-FileStorage returns 404. Don't assert
			// either outcome on the second delete — backends disagree by
			// design.
		}
	})

	t.Run("Lock should fail immediately when held by another client", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cfg, httpC := newSut(t)
		c1 := NewS3StorageClient(cfg, httpC)
		c2 := NewS3StorageClient(cfg, httpC)

		unlock1, err := c1.Lock(t.Context(), "head")
		assert.NoError(err)

		_, err = c2.Lock(t.Context(), "head")
		var existsErr *lib.LockExistsError
		assert.Equal(true, stderrors.As(err, &existsErr))

		// After c1 unlocks, c2 acquires.
		assert.NoError(unlock1())
		unlock2, err := c2.Lock(t.Context(), "head")
		assert.NoError(err)
		assert.NoError(unlock2())
	})

	t.Run("Verify-on-write should detect when another client steals the lock", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cfg, httpC := newSut(t)
		c1 := NewS3StorageClient(cfg, httpC)
		c2 := NewS3StorageClient(cfg, httpC)

		_, err := c1.Lock(t.Context(), "head")
		assert.NoError(err)
		// c2 force-releases out from under c1.
		assert.NoError(c2.ForceUnlock(t.Context(), "head"))
		err = c1.WriteControlFile(t.Context(), lib.ControlFileSectionRefs, "head", []byte("x"))
		assert.Error(err, "no longer exists")
	})

	t.Run("Verify-on-write should detect owner-mismatch after force-unlock + re-acquire", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cfg, httpC := newSut(t)
		c1 := NewS3StorageClient(cfg, httpC)
		c2 := NewS3StorageClient(cfg, httpC)

		_, err := c1.Lock(t.Context(), "head")
		assert.NoError(err)
		assert.NoError(c2.ForceUnlock(t.Context(), "head"))
		_, err = c2.Lock(t.Context(), "head")
		assert.NoError(err)
		err = c1.WriteControlFile(t.Context(), lib.ControlFileSectionRefs, "head", []byte("x"))
		assert.Error(err, "stolen")
	})

	t.Run("ForceUnlock on a non-existent lock should return ErrLockNotFound", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		assert.ErrorIs(newClient(t).ForceUnlock(t.Context(), "nope"), lib.ErrLockNotFound)
	})

	t.Run("LockExistsError carries the lock name", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		cfg, httpC := newSut(t)
		c1 := NewS3StorageClient(cfg, httpC)
		c2 := NewS3StorageClient(cfg, httpC)

		_, err := c1.Lock(t.Context(), "head")
		assert.NoError(err)
		_, err = c2.Lock(t.Context(), "head")
		var existsErr *lib.LockExistsError
		assert.Equal(true, stderrors.As(err, &existsErr))
		assert.Equal("head", existsErr.Name)
	})
}

// TestS3StorageInitRejectsNonConformantBackend points the client at a mock
// HTTP server that silently accepts every PUT (no conditional-PUT support)
// and asserts Init refuses to proceed.
func TestS3StorageInitRejectsNonConformantBackend(t *testing.T) {
	t.Parallel()
	assert := lib.NewAssert(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	client := NewS3StorageClient(S3StorageConfig{
		BucketURL:       srv.URL,
		Region:          testRegion,
		Prefix:          "",
		AccessKeyID:     testAccessKey,
		SecretAccessKey: []byte(testSecret),
	}, NewDefaultHTTPClient(srv.Client()))
	err := client.Init(t.Context(), lib.Toml{}, "")
	assert.Error(err, "does not support `If-None-Match: *`")
}

func TestS3StorageServer(t *testing.T) {
	t.Parallel()

	t.Run("Request missing Authorization should be rejected", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srv := newServerForStorage(t, freshStorage(t))
		resp, err := http.Get(srv.URL + "/repository.txt")
		assert.NoError(err)
		assert.Equal(http.StatusForbidden, resp.StatusCode)
	})

	t.Run("Wrong secret should produce a signature mismatch", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srv := newServerForStorage(t, freshStorage(t))
		client := NewS3StorageClient(S3StorageConfig{
			BucketURL:       srv.URL,
			Region:          testRegion,
			Prefix:          "",
			AccessKeyID:     testAccessKey,
			SecretAccessKey: []byte("wrong-secret"),
		}, NewDefaultHTTPClient(srv.Client()))
		_, err := client.Open(t.Context())
		assert.Error(err, "")
	})

	t.Run("Unknown access key should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srv := newServerForStorage(t, freshStorage(t))
		client := NewS3StorageClient(S3StorageConfig{
			BucketURL:       srv.URL,
			Region:          testRegion,
			Prefix:          "",
			AccessKeyID:     "OTHER-KEY",
			SecretAccessKey: []byte(testSecret),
		}, NewDefaultHTTPClient(srv.Client()))
		_, err := client.Open(t.Context())
		assert.Error(err, "")
	})

	pathCases := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{"Unknown top-level key should be rejected", http.MethodGet, "/random", http.StatusForbidden},
		{"Key under bad prefix should be rejected", http.MethodGet, "/blocks2/abc", http.StatusForbidden},
		{"Path traversal should be rejected", http.MethodGet, "/refs/../secret", http.StatusForbidden},
		{"Empty section name should be rejected", http.MethodGet, "/refs/", http.StatusForbidden},
		{"Invalid block id length should be rejected", http.MethodGet, "/blocks/abc", http.StatusForbidden},
		{"List without list-type=2 should be rejected", http.MethodGet, "/", http.StatusForbidden},
	}
	for _, tc := range pathCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert := lib.NewAssert(t)
			srv := newServerForStorage(t, freshStorage(t))
			resp, err := sendSignedTest(srv, tc.method, srv.URL+tc.path, nil)
			assert.NoError(err)
			assert.Equal(tc.want, resp.StatusCode)
		})
	}

	t.Run("Server should reject bodies over MaxBlockSize", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srv := newServerForStorage(t, freshStorage(t))
		body := make([]byte, lib.MaxBlockSize+1)
		resp, err := sendSignedTest(srv, http.MethodPut, srv.URL+"/blocks/"+td.BlockId("1").String(), body)
		assert.NoError(err)
		// 400 (sig hash mismatch because MaxBytesReader truncated body before
		// verify) or 413 are both valid rejections.
		assert.Equal(true,
			resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusRequestEntityTooLarge,
			fmt.Sprintf("unexpected status %d", resp.StatusCode))
	})

	t.Run("Server accepts exactly MaxControlFileSize", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srv := newServerForStorage(t, freshStorage(t))
		client := NewS3StorageClient(S3StorageConfig{
			BucketURL:       srv.URL,
			Region:          testRegion,
			Prefix:          "",
			AccessKeyID:     testAccessKey,
			SecretAccessKey: []byte(testSecret),
		}, NewDefaultHTTPClient(srv.Client()))
		assert.NoError(client.Init(t.Context(), lib.Toml{}, ""))
		assert.NoError(client.WriteControlFile(
			t.Context(), lib.ControlFileSectionRefs, "head", make([]byte, lib.MaxControlFileSize),
		))
	})

	t.Run("Server should reject bodies over MaxControlFileSize", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		srv := newServerForStorage(t, freshStorage(t))
		body := make([]byte, lib.MaxControlFileSize+1)
		resp, err := sendSignedTest(srv, http.MethodPut, srv.URL+"/refs/head", body)
		assert.NoError(err)
		assert.Equal(http.StatusRequestEntityTooLarge, resp.StatusCode)
	})

	t.Run("Client should reject oversized response bodies", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		// Mock server returns more bytes than the client allows. readCappedBody
		// must catch this without OOMing.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = io.CopyN(w, zeroReader{}, int64(lib.MaxBlockSize)+1)
		}))
		t.Cleanup(srv.Close)
		client := NewS3StorageClient(S3StorageConfig{
			BucketURL:       srv.URL,
			Region:          testRegion,
			Prefix:          "",
			AccessKeyID:     testAccessKey,
			SecretAccessKey: []byte(testSecret),
		}, NewDefaultHTTPClient(srv.Client()))
		_, err := client.ReadBlock(t.Context(), td.BlockId("1"), lib.NewBlockBuf())
		assert.Error(err, "response body exceeds buffer")
	})
}

func TestS3StorageServerListPagination(t *testing.T) {
	t.Parallel()

	const pageSize = 5

	newSut := func(t *testing.T, blockCount int) *S3StorageClient { //nolint:thelper
		assert := lib.NewAssert(t)
		storage, err := lib.NewFileStorage(td.NewFS(t), lib.StoragePurposeRepository)
		assert.NoError(err)
		server := NewS3StorageServer(storage, testRegion, testAccessKey, testSecret)
		server.ListPageSize = pageSize
		mux := http.NewServeMux()
		server.RegisterRoutes(mux)
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		client := NewS3StorageClient(S3StorageConfig{
			BucketURL:       srv.URL,
			Region:          testRegion,
			Prefix:          "",
			AccessKeyID:     testAccessKey,
			SecretAccessKey: []byte(testSecret),
		}, NewDefaultHTTPClient(srv.Client()))
		assert.NoError(client.Init(t.Context(), lib.Toml{}, ""))
		for i := range blockCount {
			_, err := client.WriteBlock(t.Context(), td.BlockId(strconv.Itoa(i)), []byte("data"))
			assert.NoError(err)
		}
		return client
	}

	listAll := func(t *testing.T, c *S3StorageClient) []lib.BlockId { //nolint:thelper
		var got []lib.BlockId
		lib.NewAssert(t).NoError(c.ReadBlockIds(t.Context(), func(id lib.BlockId) bool {
			got = append(got, id)
			return true
		}))
		slices.SortFunc(got, lib.BlockIdCompare)
		return got
	}

	cases := []struct {
		name  string
		count int
	}{
		// Boundary values: below, at, and above one page.
		{"Below page size", pageSize - 1},
		{"Exactly one page", pageSize},
		{"One past page size", pageSize + 1},
		{"Multiple pages", pageSize*3 + 2},
	}
	for _, tc := range cases {
		t.Run("Pagination with "+tc.name, func(t *testing.T) {
			t.Parallel()
			assert := lib.NewAssert(t)
			c := newSut(t, tc.count)
			got := listAll(t, c)
			assert.Equal(tc.count, len(got))
		})
	}

	t.Run("Continuation token from a stale session should be rejected", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		// Drive the server directly via signed requests so we control the
		// continuation token. A token that doesn't match the current
		// (empty) session must return 400.
		storage, err := lib.NewFileStorage(td.NewFS(t), lib.StoragePurposeRepository)
		assert.NoError(err)
		server := NewS3StorageServer(storage, testRegion, testAccessKey, testSecret)
		mux := http.NewServeMux()
		server.RegisterRoutes(mux)
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		client := NewS3StorageClient(S3StorageConfig{
			BucketURL: srv.URL, Region: testRegion, Prefix: "",
			AccessKeyID: testAccessKey, SecretAccessKey: []byte(testSecret),
		}, NewDefaultHTTPClient(srv.Client()))
		assert.NoError(client.Init(t.Context(), lib.Toml{}, ""))

		resp, err := sendSignedTest(srv, http.MethodGet,
			srv.URL+"/?list-type=2&prefix=blocks%2F&continuation-token=bogus", nil)
		assert.NoError(err)
		assert.Equal(http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("An idle listing is evicted after the inactivity timeout", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		storage, err := lib.NewFileStorage(td.NewFS(t), lib.StoragePurposeRepository)
		assert.NoError(err)
		server := NewS3StorageServer(storage, testRegion, testAccessKey, testSecret)
		server.ListPageSize = pageSize
		server.ListInactivityTimeout = 50 * time.Millisecond
		mux := http.NewServeMux()
		server.RegisterRoutes(mux)
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		client := NewS3StorageClient(S3StorageConfig{
			BucketURL: srv.URL, Region: testRegion, Prefix: "",
			AccessKeyID: testAccessKey, SecretAccessKey: []byte(testSecret),
		}, NewDefaultHTTPClient(srv.Client()))
		assert.NoError(client.Init(t.Context(), lib.Toml{}, ""))
		for i := range pageSize * 3 {
			_, err := client.WriteBlock(t.Context(), td.BlockId(strconv.Itoa(i)), []byte("data"))
			assert.NoError(err)
		}
		listURL := client.cfg.BucketURL + "/?list-type=2&prefix=blocks%2F"
		// Start a listing and leave it in flight.
		status, _, err := client.do(t.Context(), http.MethodGet, listURL, nil, nil, nil)
		assert.NoError(err)
		assert.Equal(http.StatusOK, status)
		// Once it has been idle past the timeout, a new listing evicts it and is
		// accepted rather than throttled.
		time.Sleep(150 * time.Millisecond)
		status, _, err = client.do(t.Context(), http.MethodGet, listURL, nil, nil, nil)
		assert.NoError(err)
		assert.Equal(http.StatusOK, status)
	})

	t.Run("A new listing while one is in flight should be throttled", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		c := newSut(t, pageSize*3)
		listURL := c.cfg.BucketURL + "/?list-type=2&prefix=blocks%2F"
		// Grab page 1 and a continuation token, leaving the session in flight.
		status, body, err := c.do(t.Context(), http.MethodGet, listURL, nil, nil, nil)
		assert.NoError(err)
		assert.Equal(http.StatusOK, status)
		var page struct {
			NextContinuationToken string `xml:"NextContinuationToken"`
		}
		assert.NoError(xml.Unmarshal(body, &page))
		assert.Equal(true, page.NextContinuationToken != "")

		// A second fresh listing must be refused, not allowed to stomp the first.
		status, _, err = c.do(t.Context(), http.MethodGet, listURL, nil, nil, nil)
		assert.NoError(err)
		assert.Equal(http.StatusServiceUnavailable, status)

		// Draining the in-flight session to completion frees the slot.
		for page.NextContinuationToken != "" {
			url := listURL + "&continuation-token=" + page.NextContinuationToken
			status, body, err = c.do(t.Context(), http.MethodGet, url, nil, nil, nil)
			assert.NoError(err)
			assert.Equal(http.StatusOK, status)
			page.NextContinuationToken = ""
			assert.NoError(xml.Unmarshal(body, &page))
		}
		var got []lib.BlockId
		assert.NoError(c.ReadBlockIds(t.Context(), func(id lib.BlockId) bool {
			got = append(got, id)
			return true
		}))
		assert.Equal(pageSize*3, len(got))
	})
}

func freshStorage(t *testing.T) *lib.FileStorage {
	t.Helper()
	storage, err := lib.NewFileStorage(td.NewFS(t), lib.StoragePurposeRepository)
	if err != nil {
		t.Fatal(err)
	}
	return storage
}

func newServerForStorage(t *testing.T, storage *lib.FileStorage) *httptest.Server {
	t.Helper()
	server := NewS3StorageServer(storage, testRegion, testAccessKey, testSecret)
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// sendSignedTest builds, SigV4-signs, and dispatches a raw test request.
func sendSignedTest(srv *httptest.Server, method, fullURL string, body []byte) (*http.Response, error) {
	signer := SigV4Signer{AccessKeyID: testAccessKey, SecretAccessKey: testSecret, Region: testRegion}
	headers := map[string]string{}
	if err := signer.Sign(method, fullURL, headers, body, time.Now().UTC()); err != nil {
		return nil, err
	}
	req, err := http.NewRequest(method, fullURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return srv.Client().Do(req)
}

// loadS3Creds reads TEST_S3_URL / TEST_S3_ACCESS_KEY / TEST_S3_SECRET_KEY
// from `.env` at the project root (or the process env). Returns empty
// strings if any are missing — caller skips.
func loadS3Creds(t *testing.T) (bucketURL, accessKey, secretKey string) {
	t.Helper()
	env := map[string]string{}
	_, file, _, _ := runtime.Caller(0)
	if data, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", ".env")); err == nil {
		for line := range strings.SplitSeq(string(data), "\n") {
			k, v, ok := strings.Cut(line, "=")
			if !ok {
				continue
			}
			env[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	pick := func(name string) string {
		if v := env[name]; v != "" {
			return v
		}
		return os.Getenv(name)
	}
	return pick("TEST_S3_URL"), pick("TEST_S3_ACCESS_KEY"), pick("TEST_S3_SECRET_KEY")
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
