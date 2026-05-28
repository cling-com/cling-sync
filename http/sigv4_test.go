package http

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

func TestSigV4Roundtrip(t *testing.T) {
	t.Parallel()
	signer := SigV4Signer{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		Region:          "us-east-1",
	}
	now := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)

	signAndBuildReq := func(t *testing.T, method, url string, body []byte) *http.Request {
		t.Helper()
		headers := map[string]string{}
		assert := lib.NewAssert(t)
		assert.NoError(signer.Sign(method, url, headers, body, now))
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
		assert.NoError(err)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return req
	}

	t.Run("GET with no body verifies", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		req := signAndBuildReq(t, http.MethodGet, "https://s3.example.com/bucket/key/path", nil)
		assert.NoError(VerifySigV4(req, nil, "us-east-1", signer.AccessKeyID, signer.SecretAccessKey, now))
	})

	t.Run("PUT with body verifies", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		body := []byte("hello world\nthis is some content")
		req := signAndBuildReq(t, http.MethodPut, "https://s3.example.com/bucket/key", body)
		assert.NoError(VerifySigV4(req, body, "us-east-1", signer.AccessKeyID, signer.SecretAccessKey, now))
	})

	t.Run("Tampered body should fail to verify", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		body := []byte("original")
		req := signAndBuildReq(t, http.MethodPut, "https://s3.example.com/bucket/key", body)
		err := VerifySigV4(req, []byte("tampered"), "us-east-1", signer.AccessKeyID, signer.SecretAccessKey, now)
		assert.Error(err, "body hash mismatch")
	})

	t.Run("Tampered path should fail to verify", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		req := signAndBuildReq(t, http.MethodGet, "https://s3.example.com/bucket/key", nil)
		req.URL.Path = "/bucket/other"
		err := VerifySigV4(req, nil, "us-east-1", signer.AccessKeyID, signer.SecretAccessKey, now)
		assert.Error(err, "signature mismatch")
	})

	t.Run("Query is signed verbatim", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		req := signAndBuildReq(t, http.MethodGet, "https://s3.example.com/b/?list-type=2&prefix=x/", nil)
		assert.NoError(VerifySigV4(req, nil, "us-east-1", signer.AccessKeyID, signer.SecretAccessKey, now))
	})

	t.Run("Clock skew beyond 15 minutes should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		req := signAndBuildReq(t, http.MethodGet, "https://s3.example.com/x", nil)
		err := VerifySigV4(req, nil, "us-east-1", signer.AccessKeyID, signer.SecretAccessKey, now.Add(16*time.Minute))
		assert.Error(err, "request expired")
	})

	t.Run("Clock skew within 15 minutes verifies", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		req := signAndBuildReq(t, http.MethodGet, "https://s3.example.com/x", nil)
		assert.NoError(
			VerifySigV4(req, nil, "us-east-1", signer.AccessKeyID, signer.SecretAccessKey, now.Add(14*time.Minute)),
		)
	})

	t.Run("Unknown access key should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		req := signAndBuildReq(t, http.MethodGet, "https://s3.example.com/x", nil)
		err := VerifySigV4(req, nil, "us-east-1", "other-access-key", signer.SecretAccessKey, now)
		assert.Error(err, "unknown access key")
	})

	t.Run("Wrong region should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		req := signAndBuildReq(t, http.MethodGet, "https://s3.example.com/x", nil)
		err := VerifySigV4(req, nil, "eu-west-1", signer.AccessKeyID, signer.SecretAccessKey, now)
		assert.Error(err, "credential scope")
	})

	t.Run("Missing Authorization should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		req, err := http.NewRequest(http.MethodGet, "https://s3.example.com/x", nil)
		assert.NoError(err)
		err = VerifySigV4(req, nil, "us-east-1", signer.AccessKeyID, signer.SecretAccessKey, now)
		assert.Error(err, "missing")
	})
}
