package http

import (
	"strings"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

var uriTestPassphrase = []byte("integration-test-passphrase-123") //nolint:gochecknoglobals

func TestS3URI(t *testing.T) {
	t.Parallel()

	t.Run("Roundtrip preserves bucket, region, prefix and credentials", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		creds := S3Credentials{AccessKeyID: "AKID-test-1234", SecretAccessKey: []byte("secret/with+special=chars")}
		raw := "s3+https://cling-sync-test.s3.nl-ams.scw.cloud/some/prefix"

		uri, err := EncodeS3URI(raw, creds, uriTestPassphrase)
		assert.NoError(err)
		assert.Equal(true, strings.HasPrefix(uri, "s3+https://"),
			"encoded URI does not start with s3+https://: "+uri)
		assert.Equal(true, strings.Contains(uri, "@cling-sync-test.s3.nl-ams.scw.cloud/some/prefix"),
			"encoded URI does not preserve host+path: "+uri)

		cfg, cleartextURI, err := DecodeS3URI(uri, uriTestPassphrase)
		assert.NoError(err)
		assert.Equal(creds.AccessKeyID, cfg.AccessKeyID)
		assert.Equal(creds.SecretAccessKey, cfg.SecretAccessKey)
		assert.Equal("https://cling-sync-test.s3.nl-ams.scw.cloud", cfg.BucketURL)
		assert.Equal("nl-ams", cfg.Region)
		assert.Equal("some/prefix", cfg.Prefix)
		assert.Equal("s3+https://cling-sync-test.s3.nl-ams.scw.cloud/some/prefix", cleartextURI)
	})

	t.Run("Accepts http scheme", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		uri, err := EncodeS3URI(
			"s3+http://localhost:9000",
			S3Credentials{AccessKeyID: "A", SecretAccessKey: []byte("B")},
			uriTestPassphrase,
		)
		assert.NoError(err)
		assert.Equal(true, strings.HasPrefix(uri, "s3+http://"), "expected s3+http:// prefix: "+uri)
		cfg, _, err := DecodeS3URI(uri, uriTestPassphrase)
		assert.NoError(err)
		assert.Equal("http://localhost:9000", cfg.BucketURL)
		assert.Equal("us-east-1", cfg.Region)
	})

	t.Run("Encoding a URL without `s3+` prefix should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, err := EncodeS3URI(
			"https://cling-sync-test.s3.nl-ams.scw.cloud",
			S3Credentials{AccessKeyID: "k", SecretAccessKey: []byte("s")},
			uriTestPassphrase,
		)
		assert.Error(err, "prefix")
	})

	t.Run("Decoding with the wrong passphrase should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		uri, err := EncodeS3URI(
			"s3+https://cling-sync-test.s3.nl-ams.scw.cloud",
			S3Credentials{AccessKeyID: "key", SecretAccessKey: []byte("secret")},
			uriTestPassphrase,
		)
		assert.NoError(err)
		_, _, err = DecodeS3URI(uri, []byte("a-different-passphrase-321"))
		assert.Error(err, "wrong passphrase")
	})

	t.Run("Decoding a URI with a tampered host should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		uri, err := EncodeS3URI(
			"s3+https://bucket-a.s3.nl-ams.scw.cloud",
			S3Credentials{AccessKeyID: "key", SecretAccessKey: []byte("secret")},
			uriTestPassphrase,
		)
		assert.NoError(err)
		tampered := strings.Replace(uri, "bucket-a", "bucket-b", 1)
		_, _, err = DecodeS3URI(tampered, uriTestPassphrase)
		assert.Error(err, "decrypt credentials")
	})

	t.Run("Decoding a URI with a tampered path should fail", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		uri, err := EncodeS3URI(
			"s3+https://bucket.s3.nl-ams.scw.cloud/dirA",
			S3Credentials{AccessKeyID: "key", SecretAccessKey: []byte("secret")},
			uriTestPassphrase,
		)
		assert.NoError(err)
		tampered := strings.Replace(uri, "/dirA", "/dirB", 1)
		_, _, err = DecodeS3URI(tampered, uriTestPassphrase)
		assert.Error(err, "decrypt credentials")
	})

	t.Run("Empty credentials should fail to encode", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, err := EncodeS3URI(
			"s3+https://bucket.s3.nl-ams.scw.cloud",
			S3Credentials{AccessKeyID: "", SecretAccessKey: []byte("x")},
			uriTestPassphrase,
		)
		assert.Error(err, "must not be empty")
		_, err = EncodeS3URI(
			"s3+https://bucket.s3.nl-ams.scw.cloud",
			S3Credentials{AccessKeyID: "x", SecretAccessKey: []byte("")},
			uriTestPassphrase,
		)
		assert.Error(err, "must not be empty")
	})

	t.Run("Access key containing `:` should fail to encode", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		_, err := EncodeS3URI(
			"s3+https://bucket.s3.nl-ams.scw.cloud",
			S3Credentials{AccessKeyID: "ak:bad", SecretAccessKey: []byte("ok")},
			uriTestPassphrase,
		)
		assert.Error(err, "':'")
	})
}

func TestRejectBareHTTPURI(t *testing.T) {
	t.Parallel()

	t.Run("Bare http:// URI should be rejected", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		err := RejectBareHTTPURI("http://localhost:9000")
		assert.Error(err, "s3+")
		assert.Error(err, "http://localhost:9000")
	})

	t.Run("Bare https:// URI should be rejected", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		err := RejectBareHTTPURI("https://bucket.s3.nl-ams.scw.cloud/prefix")
		assert.Error(err, "s3+")
		assert.Error(err, "https://bucket.s3.nl-ams.scw.cloud/prefix")
	})

	t.Run("s3+http(s) URI should pass", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		assert.NoError(RejectBareHTTPURI("s3+http://localhost:9000"))
		assert.NoError(RejectBareHTTPURI("s3+https://bucket.s3.nl-ams.scw.cloud/prefix"))
	})

	t.Run("Local path should pass", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		assert.NoError(RejectBareHTTPURI("/var/data/repo"))
		assert.NoError(RejectBareHTTPURI("./repo"))
		assert.NoError(RejectBareHTTPURI("repo"))
	})
}
