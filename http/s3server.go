// An S3 compatible server that only implements what's actually needed for cling-sync clients.
// It only ever serves one repository.
package http

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/flunderpero/cling-sync/lib"
)

const (
	storageLockTimeout           = 2 * time.Second
	defaultListPageSize          = 10000
	defaultListInactivityTimeout = 60 * time.Second
)

type S3StorageServer struct {
	Storage               lib.Storage
	Region                string
	AccessKeyID           string
	SecretAccessKey       string
	ListPageSize          int
	ListInactivityTimeout time.Duration

	locksMutex sync.Mutex
	locks      map[string]*serverLock

	// Only one block-id listing runs at a time.
	listMu      sync.Mutex
	listSession *listSession
}

type listSession struct {
	id           string
	ch           chan lib.BlockId
	cancel       context.CancelFunc
	producerErr  error
	lastActivity time.Time
}

type serverLock struct {
	body   []byte
	unlock func() error
}

func NewS3StorageServer(storage lib.Storage, region, accessKeyID, secretAccessKey string) *S3StorageServer {
	return &S3StorageServer{
		Storage: storage, Region: region,
		AccessKeyID: accessKeyID, SecretAccessKey: secretAccessKey,
		ListPageSize: defaultListPageSize, ListInactivityTimeout: defaultListInactivityTimeout,
		locksMutex: sync.Mutex{}, locks: map[string]*serverLock{},
		listMu: sync.Mutex{}, listSession: nil,
	}
}

func (s *S3StorageServer) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/", s)
}

func (s *S3StorageServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := s.readBody(w, r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	if err := VerifySigV4(r, body, s.Region, s.AccessKeyID, s.SecretAccessKey, time.Now().UTC()); err != nil {
		s.writeError(w, http.StatusForbidden, "SignatureDoesNotMatch", err.Error())
		return
	}
	s.route(w, r, body)
}

func (s *S3StorageServer) readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	if r.Body == nil || r.ContentLength == 0 {
		return nil, nil
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(lib.MaxBlockSize))
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return nil, lib.Errorf("request body exceeds %d bytes", lib.MaxBlockSize)
		}
		return nil, lib.WrapErrorf(err, "failed to read request body")
	}
	return body, nil
}

func (s *S3StorageServer) route(w http.ResponseWriter, r *http.Request, body []byte) {
	keyPart := strings.TrimPrefix(r.URL.Path, "/")
	if keyPart == "" {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			s.writeError(w, http.StatusForbidden, "AccessDenied", "only LIST V2 allowed on bucket root")
			return
		}
		s.handleList(w, r)
		return
	}
	if strings.Contains(keyPart, "..") || strings.Contains(keyPart, "//") {
		s.writeError(w, http.StatusForbidden, "AccessDenied", "invalid key")
		return
	}
	switch {
	case keyPart == "repository.txt":
		s.handleConfig(w, r, body)
	case strings.HasPrefix(keyPart, "blocks/"):
		rest := strings.TrimPrefix(keyPart, "blocks/")
		if len(rest) != 2*lib.BlockIdSize {
			s.writeError(w, http.StatusForbidden, "AccessDenied", "invalid block key")
			return
		}
		blockId, err := lib.NewBlockIdFromString(rest)
		if err != nil {
			s.writeError(w, http.StatusForbidden, "AccessDenied", err.Error())
			return
		}
		s.handleBlock(w, r, blockId, body)
	case strings.HasPrefix(keyPart, "refs/"):
		s.handleControlRoute(w, r, lib.ControlFileSectionRefs, strings.TrimPrefix(keyPart, "refs/"), body)
	case strings.HasPrefix(keyPart, "security/"):
		s.handleControlRoute(w, r, lib.ControlFileSectionSecurity, strings.TrimPrefix(keyPart, "security/"), body)
	case strings.HasPrefix(keyPart, "conf/"):
		s.handleControlRoute(w, r, lib.ControlFileSectionConf, strings.TrimPrefix(keyPart, "conf/"), body)
	case strings.HasPrefix(keyPart, "locks/"):
		rest := strings.TrimPrefix(keyPart, "locks/")
		if err := lib.ValidateStorageLockName(rest); err != nil {
			s.writeError(w, http.StatusForbidden, "AccessDenied", err.Error())
			return
		}
		s.handleLock(w, r, rest, body)
	default:
		s.writeError(w, http.StatusForbidden, "AccessDenied", "unknown key shape")
	}
}

func (s *S3StorageServer) handleControlRoute(
	w http.ResponseWriter, r *http.Request, section lib.ControlFileSection, name string, body []byte,
) {
	if err := lib.ValidateControlFileName(name); err != nil {
		s.writeError(w, http.StatusForbidden, "AccessDenied", err.Error())
		return
	}
	s.handleControl(w, r, section, name, body)
}

//nolint:funlen,contextcheck
func (s *S3StorageServer) handleList(w http.ResponseWriter, r *http.Request) {
	wantPrefix := r.URL.Query().Get("prefix")
	token := r.URL.Query().Get("continuation-token")
	// Only the blocks/ namespace is enumerable. Other prefixes return empty.
	if !strings.HasSuffix(wantPrefix, "blocks/") {
		s.writeListResult(w, wantPrefix, nil, false, "")
		return
	}
	s.listMu.Lock()
	defer s.listMu.Unlock()
	sess := s.listSession
	if token == "" {
		if sess != nil && time.Since(sess.lastActivity) < s.ListInactivityTimeout {
			s.writeError(w, http.StatusServiceUnavailable, "SlowDown", "a block listing is already in progress")
			return
		}
		if sess != nil {
			sess.cancel()
		}
		id, err := lib.RandStr(32)
		if err != nil {
			s.internalError(w, lib.WrapErrorf(err, "failed to generate listing session id"))
			return
		}
		ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec
		sess = &listSession{
			id:           id,
			ch:           make(chan lib.BlockId, s.ListPageSize*3),
			cancel:       cancel,
			producerErr:  nil,
			lastActivity: time.Now(),
		}
		s.listSession = sess
		go func() {
			defer close(sess.ch)
			err := s.Storage.ReadBlockIds(func(blockId lib.BlockId) bool {
				select {
				case sess.ch <- blockId:
					return true
				case <-ctx.Done():
					return false
				}
			})
			if err != nil {
				sess.producerErr = err
			}
		}()
	} else if sess == nil || sess.id != token {
		s.writeError(w, http.StatusBadRequest, "InvalidArgument", "no matching listing session")
		return
	}
	sess.lastActivity = time.Now()

	keys := make([]string, 0, s.ListPageSize)
	done := false
	for i := 0; i < s.ListPageSize && !done; i++ {
		select {
		case id, ok := <-sess.ch:
			if !ok {
				done = true
			} else {
				keys = append(keys, wantPrefix+id.String())
			}
		case <-r.Context().Done():
			// Client disconnected mid-page. The session stays alive for one
			// inactivity window so a reconnect with the token can resume it.
			return
		}
	}

	if done {
		s.listSession = nil
		sess.cancel()
		if sess.producerErr != nil {
			s.internalError(w, sess.producerErr)
			return
		}
		s.writeListResult(w, wantPrefix, keys, false, "")
		return
	}
	s.writeListResult(w, wantPrefix, keys, true, sess.id)
}

func (s *S3StorageServer) writeListResult(
	w http.ResponseWriter, prefix string, keys []string, isTruncated bool, nextToken string,
) {
	type entry struct {
		Key string `xml:"Key"`
	}
	type result struct {
		XMLName               xml.Name `xml:"ListBucketResult"`
		Prefix                string   `xml:"Prefix"`
		IsTruncated           bool     `xml:"IsTruncated"`
		NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
		Contents              []entry  `xml:"Contents"`
	}
	contents := make([]entry, len(keys))
	for i, k := range keys {
		contents[i] = entry{Key: k}
	}
	out, err := xml.Marshal(result{
		XMLName:               xml.Name{Space: "", Local: "ListBucketResult"},
		Prefix:                prefix,
		IsTruncated:           isTruncated,
		NextContinuationToken: nextToken,
		Contents:              contents,
	})
	if err != nil {
		s.internalError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

func (s *S3StorageServer) handleConfig(w http.ResponseWriter, r *http.Request, body []byte) {
	switch r.Method {
	case http.MethodHead:
		_, err := s.Storage.Open()
		if errors.Is(err, lib.ErrStorageNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			s.internalError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		toml, err := s.Storage.Open()
		if errors.Is(err, lib.ErrStorageNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			s.internalError(w, err)
			return
		}
		var buf bytes.Buffer
		if err := lib.WriteToml(&buf, "", toml); err != nil {
			s.internalError(w, err)
			return
		}
		writeBody(w, "application/octet-stream", buf.Bytes())
	case http.MethodPut:
		if len(body) > lib.MaxControlFileSize {
			s.writeError(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "config too large")
			return
		}
		toml, err := lib.ReadToml(bytes.NewReader(body))
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
			return
		}
		if err := s.Storage.Init(toml, ""); err != nil {
			if errors.Is(err, lib.ErrStorageAlreadyExists) {
				// Match the conditional-PUT contract: client sends
				// If-None-Match: * and reads 412 as "already exists".
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			s.internalError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

func (s *S3StorageServer) handleBlock(w http.ResponseWriter, r *http.Request, id lib.BlockId, body []byte) {
	switch r.Method {
	case http.MethodHead:
		exists, err := s.Storage.HasBlock(id)
		if err != nil {
			s.internalError(w, err)
			return
		}
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		buf := lib.NewBlockBuf()
		data, err := s.Storage.ReadBlock(id, buf)
		if errors.Is(err, lib.ErrBlockNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			s.internalError(w, err)
			return
		}
		writeBody(w, "application/octet-stream", data)
	case http.MethodPut:
		if len(body) > lib.MaxBlockSize {
			s.writeError(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "block too large")
			return
		}
		existed, err := s.Storage.WriteBlock(id, body)
		if err != nil {
			s.internalError(w, err)
			return
		}
		// Client uses If-None-Match:* and reads 412 as "already exists".
		if existed && r.Header.Get("If-None-Match") == "*" {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		if existed {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusCreated)
		}
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

func (s *S3StorageServer) handleControl(
	w http.ResponseWriter, r *http.Request, section lib.ControlFileSection, name string, body []byte,
) {
	switch r.Method {
	case http.MethodHead:
		ok, err := s.Storage.HasControlFile(section, name)
		if err != nil {
			s.internalError(w, err)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		data, err := s.Storage.ReadControlFile(section, name)
		if errors.Is(err, lib.ErrControlFileNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			s.internalError(w, err)
			return
		}
		writeBody(w, "application/octet-stream", data)
	case http.MethodPut:
		if len(body) > lib.MaxControlFileSize {
			s.writeError(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "control file too large")
			return
		}
		if err := s.Storage.WriteControlFile(section, name, body); err != nil {
			s.internalError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if err := s.Storage.DeleteControlFile(section, name); err != nil {
			if errors.Is(err, lib.ErrControlFileNotFound) {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			s.internalError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

//nolint:funlen
func (s *S3StorageServer) handleLock(w http.ResponseWriter, r *http.Request, name string, body []byte) {
	switch r.Method {
	case http.MethodHead:
		s.locksMutex.Lock()
		_, ok := s.locks[name]
		s.locksMutex.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		s.locksMutex.Lock()
		lk, ok := s.locks[name]
		s.locksMutex.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeBody(w, "application/json", lk.body)
	case http.MethodPut:
		if r.Header.Get("If-None-Match") != "*" {
			s.writeError(w, http.StatusBadRequest, "InvalidRequest", "lock PUT requires If-None-Match: *")
			return
		}
		if len(body) > lib.MaxControlFileSize {
			s.writeError(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "lock body too large")
			return
		}
		s.locksMutex.Lock()
		if _, exists := s.locks[name]; exists {
			s.locksMutex.Unlock()
			s.writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "lock held")
			return
		}
		// Reserve the slot under locksMutex before taking the (potentially blocking)
		// underlying flock so a concurrent PUT can't double-acquire.
		s.locks[name] = &serverLock{body: nil, unlock: nil}
		s.locksMutex.Unlock()

		ctx, cancel := context.WithTimeout(r.Context(), storageLockTimeout)
		defer cancel()
		unlock, err := s.Storage.Lock(ctx, name)
		if err != nil {
			s.locksMutex.Lock()
			delete(s.locks, name)
			s.locksMutex.Unlock()
			s.writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "local flock contention: "+err.Error())
			return
		}
		s.locksMutex.Lock()
		s.locks[name] = &serverLock{body: body, unlock: unlock}
		s.locksMutex.Unlock()
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		s.locksMutex.Lock()
		lk, ok := s.locks[name]
		if ok {
			delete(s.locks, name)
		}
		s.locksMutex.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if lk.unlock != nil {
			if err := lk.unlock(); err != nil {
				slog.Error("Failed to release flock on lock DELETE", "error", err, "name", name) //nolint:gosec
			}
		}
		w.WriteHeader(http.StatusOK)
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

func writeBody(w http.ResponseWriter, contentType string, data []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data) //nolint:gosec
}

func (s *S3StorageServer) internalError(w http.ResponseWriter, err error) {
	s.writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
}

func (s *S3StorageServer) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	type s3Error struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
	out, _ := xml.Marshal(s3Error{XMLName: xml.Name{Space: "", Local: "Error"}, Code: code, Message: message})
	_, _ = w.Write(out)
}
