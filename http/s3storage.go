// S3-protocol Storage. Speaks to any S3-compatible service via HTTPClient
// so the same client works under net/http (CLI) and js/fetch (wasm).
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"maps"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

// HTTP methods and status codes the S3 client cares about. They are defined
// here so this file does not import net/http and can be compiled for wasm.
const (
	methodGet    = "GET"
	methodHead   = "HEAD"
	methodPut    = "PUT"
	methodDelete = "DELETE"

	statusOK                 = 200
	statusCreated            = 201
	statusNoContent          = 204
	statusNotFound           = 404
	statusPreconditionFailed = 412
)

type HTTPClient interface {
	// Response bytes go into `dst` when non-nil, otherwise into a fresh slice capped at `MaxBlockSize`.
	Request(
		ctx context.Context,
		method, url string,
		headers map[string]string,
		body, dst []byte,
	) (status int, respBody []byte, err error)
}

type S3StorageConfig struct {
	BucketURL       string
	Region          string
	Prefix          string
	AccessKeyID     string
	SecretAccessKey []byte
}

type S3StorageClient struct {
	cfg    S3StorageConfig
	signer SigV4Signer
	http   HTTPClient

	lockMu    sync.Mutex
	lockState *s3LockState
}

type s3LockState struct {
	Name  string
	Owner string
}

type s3LockMeta struct {
	Owner     string    `json:"owner"`
	Host      string    `json:"host"`
	Pid       int       `json:"pid"`
	CreatedAt time.Time `json:"createdAt"`
}

func NewS3StorageClient(cfg S3StorageConfig, httpClient HTTPClient) *S3StorageClient {
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")
	return &S3StorageClient{
		cfg: cfg,
		signer: SigV4Signer{
			AccessKeyID:     cfg.AccessKeyID,
			SecretAccessKey: string(cfg.SecretAccessKey),
			Region:          cfg.Region,
		},
		http:      httpClient,
		lockMu:    sync.Mutex{},
		lockState: nil,
	}
}

func (c *S3StorageClient) Init(ctx context.Context, config lib.Toml, headerComment string) error {
	var buf bytes.Buffer
	if err := lib.WriteToml(&buf, headerComment, config); err != nil {
		return lib.WrapErrorf(err, "failed to encode config TOML")
	}
	key := c.key("repository.txt")
	status, body, err := c.do(ctx, methodPut, key, ifNoneMatch, buf.Bytes(), nil)
	if err != nil {
		return lib.WrapErrorf(err, "failed to init storage")
	}
	switch status {
	case statusOK, statusCreated:
	case statusPreconditionFailed:
		return lib.ErrStorageAlreadyExists
	default:
		return lib.Errorf("init failed: %d (%s)", status, truncateErrBody(body))
	}
	// Verify the backend honors `If-None-Match: *`: a second PUT against the
	// now-existing repository.txt must be refused with 412. Cling-sync's
	// locking and write-once blocks depend on this. We piggy-back on
	// repository.txt and remove it again if the backend turns out to be
	// non-conformant, so the bucket is left in a clean uninitialized state.
	verifyStatus, _, err := c.do(ctx, methodPut, key, ifNoneMatch, buf.Bytes(), nil)
	if err != nil {
		_, _, _ = c.do(ctx, methodDelete, key, nil, nil, nil)
		return lib.WrapErrorf(err, "If-None-Match verification PUT failed")
	}
	if verifyStatus != statusPreconditionFailed {
		_, _, _ = c.do(ctx, methodDelete, key, nil, nil, nil)
		return lib.Errorf(
			"S3 backend does not support `If-None-Match: *` "+
				"(verification PUT returned %d, expected 412); "+
				"cling-sync requires this for safe locking and write-once blocks",
			verifyStatus,
		)
	}
	return nil
}

func (c *S3StorageClient) Open(ctx context.Context) (lib.Toml, error) {
	status, body, err := c.do(ctx, methodGet, c.key("repository.txt"), nil, nil, nil)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to open storage")
	}
	if status == statusNotFound {
		return nil, lib.ErrStorageNotFound
	}
	if status != statusOK {
		return nil, lib.Errorf("open failed: %d (%s)", status, truncateErrBody(body))
	}
	toml, err := lib.ReadToml(bytes.NewReader(body))
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to parse storage TOML")
	}
	return toml, nil
}

func (c *S3StorageClient) HasBlock(ctx context.Context, blockId lib.BlockId) (bool, error) {
	status, _, err := c.do(ctx, methodHead, c.key("blocks", blockId.String()), nil, nil, nil)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to check block")
	}
	switch status {
	case statusOK:
		return true, nil
	case statusNotFound:
		return false, nil
	}
	return false, lib.Errorf("unexpected status: %d", status)
}

func (c *S3StorageClient) ReadBlock(ctx context.Context, blockId lib.BlockId, buf lib.BlockBuf) ([]byte, error) {
	status, body, err := c.do(
		ctx, methodGet, c.key("blocks", blockId.String()), nil, nil, buf.Bytes(),
	)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read block")
	}
	if status == statusNotFound {
		return nil, lib.WrapErrorf(lib.ErrBlockNotFound, "block %s does not exist", blockId)
	}
	if status != statusOK {
		return nil, lib.Errorf("read block failed: %d", status)
	}
	return body, nil
}

func (c *S3StorageClient) WriteBlock(ctx context.Context, blockId lib.BlockId, data []byte) (bool, error) {
	if len(data) > lib.MaxBlockSize {
		return false, lib.Errorf("block %s is too large: %d", blockId, len(data))
	}
	if err := c.verifyLockIfHeld(ctx); err != nil {
		return false, err
	}
	status, body, err := c.do(
		ctx, methodPut, c.key("blocks", blockId.String()),
		ifNoneMatch, data, nil,
	)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to write block")
	}
	switch status {
	case statusOK, statusCreated:
		return false, nil
	case statusPreconditionFailed:
		return true, nil
	}
	return false, lib.Errorf("write block failed: %d (%s)", status, truncateErrBody(body))
}

func (c *S3StorageClient) ReadBlockIds(ctx context.Context, yield func(lib.BlockId) bool) error {
	prefix := c.key("blocks") + "/"
	continuation := ""
	for {
		query := url.Values{}
		query.Set("list-type", "2")
		query.Set("prefix", prefix)
		if continuation != "" {
			query.Set("continuation-token", continuation)
		}
		status, body, err := c.do(
			ctx, methodGet, c.cfg.BucketURL+"/?"+query.Encode(), nil, nil, nil,
		)
		if err != nil {
			return lib.WrapErrorf(err, "failed to list blocks")
		}
		if status != statusOK {
			return lib.Errorf("list failed: %d (%s)", status, truncateErrBody(body))
		}
		var result struct {
			IsTruncated           bool   `xml:"IsTruncated"`
			NextContinuationToken string `xml:"NextContinuationToken"`
			Contents              []struct {
				Key string `xml:"Key"`
			} `xml:"Contents"`
		}
		if err := xml.Unmarshal(body, &result); err != nil {
			return lib.WrapErrorf(err, "failed to parse list response")
		}
		for _, item := range result.Contents {
			key := strings.TrimPrefix(item.Key, prefix)
			blockId, err := lib.NewBlockIdFromString(key)
			if err != nil {
				return lib.WrapErrorf(err, "invalid block key %q", item.Key)
			}
			if !yield(blockId) {
				return nil
			}
		}
		if !result.IsTruncated {
			return nil
		}
		continuation = result.NextContinuationToken
		if continuation == "" {
			return lib.Errorf(
				"S3 list response set IsTruncated=true but omitted NextContinuationToken; cannot resume listing",
			)
		}
	}
}

func (c *S3StorageClient) HasControlFile(
	ctx context.Context,
	section lib.ControlFileSection,
	name string,
) (bool, error) {
	if err := lib.ValidateControlFileName(name); err != nil {
		return false, err //nolint:wrapcheck
	}
	status, _, err := c.do(ctx, methodHead, c.key(string(section), name), nil, nil, nil)
	if err != nil {
		return false, lib.WrapErrorf(err, "failed to check control file")
	}
	switch status {
	case statusOK:
		return true, nil
	case statusNotFound:
		return false, nil
	}
	return false, lib.Errorf("unexpected status: %d", status)
}

func (c *S3StorageClient) ReadControlFile(
	ctx context.Context,
	section lib.ControlFileSection,
	name string,
) ([]byte, error) {
	if err := lib.ValidateControlFileName(name); err != nil {
		return nil, err //nolint:wrapcheck
	}
	status, body, err := c.do(ctx, methodGet, c.key(string(section), name), nil, nil, nil)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to read control file")
	}
	if status == statusNotFound {
		return nil, lib.WrapErrorf(lib.ErrControlFileNotFound, "control file %s/%s does not exist", section, name)
	}
	if status != statusOK {
		return nil, lib.Errorf("read control file failed: %d", status)
	}
	if len(body) > lib.MaxControlFileSize {
		return nil, lib.Errorf("control file exceeds max size %d", lib.MaxControlFileSize)
	}
	return body, nil
}

func (c *S3StorageClient) WriteControlFile(
	ctx context.Context,
	section lib.ControlFileSection,
	name string,
	data []byte,
) error {
	if err := lib.ValidateControlFileName(name); err != nil {
		return err //nolint:wrapcheck
	}
	if len(data) > lib.MaxControlFileSize {
		return lib.Errorf("control file %s/%s is too large: %d", section, name, len(data))
	}
	if err := c.verifyLockIfHeld(ctx); err != nil {
		return err
	}
	status, body, err := c.do(
		ctx, methodPut, c.key(string(section), name), nil, data, nil,
	)
	if err != nil {
		return lib.WrapErrorf(err, "failed to write control file")
	}
	if status != statusOK && status != statusCreated {
		return lib.Errorf("write control file failed: %d (%s)", status, truncateErrBody(body))
	}
	return nil
}

func (c *S3StorageClient) DeleteControlFile(ctx context.Context, section lib.ControlFileSection, name string) error {
	if err := lib.ValidateControlFileName(name); err != nil {
		return err //nolint:wrapcheck
	}
	if err := c.verifyLockIfHeld(ctx); err != nil {
		return err
	}
	status, _, err := c.do(ctx, methodDelete, c.key(string(section), name), nil, nil, nil)
	if err != nil {
		return lib.WrapErrorf(err, "failed to delete control file")
	}
	if status == statusNotFound {
		return lib.WrapErrorf(lib.ErrControlFileNotFound, "control file %s/%s does not exist", section, name)
	}
	if status != statusOK && status != statusNoContent {
		return lib.Errorf("delete control file failed: %d", status)
	}
	return nil
}

func (c *S3StorageClient) Lock(ctx context.Context, name string) (func() error, error) {
	if err := lib.ValidateStorageLockName(name); err != nil {
		return nil, err //nolint:wrapcheck
	}
	owner, err := lib.RandStr(32)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to generate owner GUID")
	}
	host, _ := os.Hostname() //nolint:forbidigo
	body, err := json.Marshal(s3LockMeta{
		Owner: owner, Host: host, Pid: os.Getpid(), CreatedAt: time.Now().UTC(), //nolint:forbidigo
	})
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to marshal lock meta")
	}

	status, _, err := c.do(ctx, methodPut, c.key("locks", name), ifNoneMatch, body, nil)
	if err != nil {
		return nil, lib.WrapErrorf(err, "failed to acquire lock %s", name)
	}
	switch status {
	case statusOK, statusCreated:
		state := &s3LockState{Name: name, Owner: owner}
		c.lockMu.Lock()
		c.lockState = state
		c.lockMu.Unlock()
		return c.releaseLock(state), nil //nolint:contextcheck
	case statusPreconditionFailed:
		existsErr, perr := c.readLockExistsErr(ctx, name)
		if perr != nil {
			existsErr = &lib.LockExistsError{
				Name: name, Owner: "", Host: "", Pid: 0, CreatedAt: time.Time{},
			}
		}
		return nil, existsErr
	}
	return nil, lib.Errorf("unexpected status acquiring lock: %d", status)
}

func (c *S3StorageClient) ForceUnlock(ctx context.Context, name string) error {
	if err := lib.ValidateStorageLockName(name); err != nil {
		return err //nolint:wrapcheck
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	status, _, err := c.do(ctx, methodHead, c.key("locks", name), nil, nil, nil)
	if err != nil {
		return lib.WrapErrorf(err, "failed to probe lock")
	}
	if status == statusNotFound {
		return lib.WrapErrorf(lib.ErrLockNotFound, "lock %s does not exist", name)
	}
	status, _, err = c.do(ctx, methodDelete, c.key("locks", name), nil, nil, nil)
	if err != nil {
		return lib.WrapErrorf(err, "failed to force-release lock")
	}
	if status != statusOK && status != statusNoContent && status != statusNotFound {
		return lib.Errorf("force-release lock failed: %d", status)
	}
	return nil
}

func (c *S3StorageClient) verifyLockIfHeld(ctx context.Context) error {
	c.lockMu.Lock()
	state := c.lockState
	c.lockMu.Unlock()
	if state == nil {
		return nil
	}
	status, body, err := c.do(ctx, methodGet, c.key("locks", state.Name), nil, nil, nil)
	if err != nil {
		return lib.WrapErrorf(err, "failed to verify lock %s", state.Name)
	}
	if status == statusNotFound {
		return lib.Errorf("lock %s no longer exists (force-unlocked?)", state.Name)
	}
	if status != statusOK {
		return lib.Errorf("verify lock %s failed: %d", state.Name, status)
	}
	var meta s3LockMeta
	if err := json.Unmarshal(body, &meta); err != nil {
		return lib.WrapErrorf(err, "failed to parse lock meta")
	}
	if meta.Owner != state.Owner {
		return lib.Errorf("lock %s was stolen (owner %s != %s)", state.Name, meta.Owner, state.Owner)
	}
	return nil
}

func (c *S3StorageClient) readLockExistsErr(ctx context.Context, name string) (*lib.LockExistsError, error) {
	status, body, err := c.do(ctx, methodGet, c.key("locks", name), nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if status != statusOK {
		return nil, lib.Errorf("read lock holder failed: %d", status)
	}
	var meta s3LockMeta
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, lib.WrapErrorf(err, "failed to parse lock meta")
	}
	return &lib.LockExistsError{
		Name: name, Owner: meta.Owner, Host: meta.Host, Pid: meta.Pid, CreatedAt: meta.CreatedAt,
	}, nil
}

func (c *S3StorageClient) releaseLock(state *s3LockState) func() error {
	var released atomic.Bool
	return func() error {
		if !released.CompareAndSwap(false, true) {
			return nil
		}
		c.lockMu.Lock()
		if c.lockState == state {
			c.lockState = nil
		}
		c.lockMu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		status, _, err := c.do(ctx, methodDelete, c.key("locks", state.Name), nil, nil, nil)
		if err != nil {
			return lib.WrapErrorf(err, "failed to release lock %s", state.Name)
		}
		if status != statusOK && status != statusNoContent && status != statusNotFound {
			return lib.Errorf("release lock %s failed: %d", state.Name, status)
		}
		return nil
	}
}

func (c *S3StorageClient) key(parts ...string) string {
	joined := strings.Join(parts, "/")
	if c.cfg.Prefix == "" {
		return joined
	}
	return c.cfg.Prefix + "/" + joined
}

// do signs and dispatches. `keyOrURL` is treated as a bucket-relative key
// unless it already contains `://`.
func (c *S3StorageClient) do(
	ctx context.Context, method, keyOrURL string, extraHeaders map[string]string, body, dst []byte,
) (int, []byte, error) {
	fullURL := keyOrURL
	if !strings.Contains(keyOrURL, "://") {
		fullURL = c.cfg.BucketURL + "/" + keyOrURL
	}
	headers := map[string]string{}
	maps.Copy(headers, extraHeaders)
	if err := c.signer.Sign(method, fullURL, headers, body, time.Now().UTC()); err != nil {
		return 0, nil, err
	}
	status, respBody, err := c.http.Request(ctx, method, fullURL, headers, body, dst)
	if err != nil {
		return status, respBody, lib.WrapErrorf(err, "HTTP transport failed")
	}
	return status, respBody, nil
}

// ifNoneMatch refuses overwrites. 412 means the object already exists.
var ifNoneMatch = map[string]string{"If-None-Match": "*"} //nolint:gochecknoglobals

func truncateErrBody(b []byte) string {
	const limit = 200
	if len(b) <= limit {
		return string(b)
	}
	return string(b[:limit]) + "..."
}

// Compile-time assertion that S3StorageClient satisfies lib.Storage.
var _ lib.Storage = (*S3StorageClient)(nil)
