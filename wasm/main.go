//go:build wasm

package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"html"
	"io"
	"path/filepath"
	"strings"
	"time"

	clingHTTP "github.com/flunderpero/cling-sync/http"
	"github.com/flunderpero/cling-sync/lib"
	"github.com/flunderpero/cling-sync/workspace"
	"syscall/js"
)

const MaxDownloadSize = 50 * 1024 * 1024

var repositoryHandles = make(map[int]*lib.Repository)

func main() {
	repositoryAPI := &RepositoryAPI{}
	repositoryJS := js.Global().Get("Object").New()
	repositoryJS.Set("open", js.FuncOf(repositoryAPI.Open))
	repositoryJS.Set("head", js.FuncOf(repositoryAPI.Head))
	repositoryJS.Set("ls", js.FuncOf(repositoryAPI.Ls))
	repositoryJS.Set("readFile", js.FuncOf(repositoryAPI.ReadFile))
	js.Global().Set("repositoryAPI", repositoryJS)

	// Keep the program running. This is required for the browser to keep the
	// WASM module alive.
	select {}
}

type RepositoryAPI struct{}

// Parameters:
//
//	url: string
//	passphrase: string
//
// Returns:
//
//	Promise<int> (RepositoryHandle)
func (r RepositoryAPI) Open(this js.Value, args []js.Value) any {
	url := args[0].String()
	passphrase := args[1].String()
	return async(func(resolve func(...any) js.Value, reject func(...any) js.Value) {
		httpClient := &WASMHTTPClient{}
		storage := clingHTTP.NewHTTPStorageClient(url, httpClient)
		repository, err := lib.OpenRepository(storage, []byte(passphrase))
		if err != nil {
			reject(err.Error())
			return
		}
		handle := len(repositoryHandles)
		repositoryHandles[handle] = repository
		resolve(handle)
	})
}

// Parameters:
//
//	handle: RepositoryHandle
//
// Returns:
//
//	Promise<string>
func (r RepositoryAPI) Head(this js.Value, args []js.Value) any {
	handle := args[0].Int()
	return async(func(resolve func(...any) js.Value, reject func(...any) js.Value) {
		repository, ok := repositoryHandles[handle]
		if !ok {
			reject(lib.Errorf("invalid repository handle: %d", handle))
			return
		}
		revisionId, err := repository.Head()
		if err != nil {
			reject(err.Error())
			return
		}
		resolve(revisionId.String())
	})
}

// Parameters:
//
//	handle: RepositoryHandle
//	excludes: string (comma separated)
//
// Returns:
//
//	Promise<string>: The result as <tr> elements.
func (r RepositoryAPI) Ls(this js.Value, args []js.Value) any {
	handle := args[0].Int()
	excludes := args[1].String()
	return async(func(resolve func(...any) js.Value, reject func(...any) js.Value) {
		repository, ok := repositoryHandles[handle]
		if !ok {
			reject(lib.Errorf("invalid repository handle: %d", handle))
			return
		}
		revisionId, err := repository.Head()
		if err != nil {
			reject(err.Error())
			return
		}
		tmpFS := lib.NewMemoryFS(10000000)
		var filter lib.PathFilter
		if excludes != "" {
			filter, err = lib.NewPathExclusionFilter(strings.Split(excludes, ","), nil)
		}
		if err != nil {
			reject(err.Error())
			return
		}
		opts := &workspace.LsOptions{RevisionId: revisionId, PathFilter: filter}
		files, err := workspace.Ls(repository, tmpFS, opts)
		if err != nil {
			reject(err.Error())
			return
		}
		var sb strings.Builder
		for _, file := range files {
			sb.WriteString("<tr>")
			sb.WriteString("<td>")
			sb.WriteString(html.EscapeString(file.Metadata.ModeAndPerm.ShortString()))
			sb.WriteString("</td>")
			if file.Metadata.ModeAndPerm.IsRegular() {
				sb.WriteString("<td>")
				sb.WriteString(html.EscapeString(workspace.FormatBytes(file.Metadata.Size)))
				sb.WriteString("</td>")
			} else {
				sb.WriteString("<td></td>")
			}
			if file.Metadata.ModeAndPerm.IsRegular() {
				sb.WriteString(
					"<td><a href=\"#download:" + base64.StdEncoding.EncodeToString([]byte(file.Path)) + "\">",
				)
				sb.WriteString(html.EscapeString(file.Path))
				sb.WriteString("</a></td>")
			} else {
				sb.WriteString("<td>")
				sb.WriteString(html.EscapeString(file.Path))
				sb.WriteString("/</td>")
			}
			sb.WriteString("<td>")
			sb.WriteString(html.EscapeString(file.Metadata.MTime().Format(time.RFC3339)))
			sb.WriteString("</td>")
			sb.WriteString("</tr>")
		}
		resolve(sb.String())
	})
}

// Read a file from the repository and returns it as a JS `Uint8Array`.
//
// Parameters:
//
//	handle: RepositoryHandle
//	path: string (base64 encoded)
//	revisionId: string ("" for HEAD)
//
// Returns:
//
//	Promise<[Uint8Array, string]>
//	The data and the file name.
func (r *RepositoryAPI) ReadFile(this js.Value, args []js.Value) any {
	return async(func(resolve func(...any) js.Value, reject func(...any) js.Value) {
		handle := args[0].Int()
		pathBytes, err := base64.StdEncoding.DecodeString(args[1].String())
		if err != nil {
			reject(err.Error())
			return
		}
		path := string(pathBytes)
		revisionIdArg := args[2].String()
		repository, ok := repositoryHandles[handle]
		if !ok {
			reject(lib.Errorf("invalid repository handle: %d", handle))
			return
		}
		var revisionId lib.RevisionId
		if revisionIdArg == "" {
			var err error
			revisionId, err = repository.Head()
			if err != nil {
				reject(err.Error())
				return
			}
		} else {
			revisionId = lib.RevisionId([]byte(revisionIdArg))
		}
		tmpFS := lib.NewMemoryFS(10000000)
		snapshot, err := lib.NewRevisionSnapshot(repository, revisionId, tmpFS)
		if err != nil {
			reject(err.Error())
			return
		}
		filter, err := lib.NewPathInclusionFilter([]string{path})
		if err != nil {
			reject(err.Error())
			return
		}
		r := snapshot.Reader(filter)
		file, err := r.Read()
		if errors.Is(err, io.EOF) {
			reject(lib.Errorf("file not found: %s", path))
			return
		}
		if err != nil {
			reject(err.Error())
			return
		}
		if file.Metadata.Size > MaxDownloadSize {
			reject(lib.Errorf("file too large: %s", path))
			return
		}
		if !file.Metadata.ModeAndPerm.IsRegular() {
			reject(lib.Errorf("not a regular file: %s", path))
			return
		}
		data := bytes.NewBuffer(nil)
		data.Grow(int(file.Metadata.Size))
		for _, blockId := range file.Metadata.BlockIds {
			block, _, err := repository.ReadBlock(blockId)
			if err != nil {
				reject(err.Error())
				return
			}
			data.Write(block)
		}
		resultData := js.Global().Get("Uint8Array").New(data.Len())
		js.CopyBytesToJS(resultData, data.Bytes())
		result := js.Global().Get("Array").New()
		result.Set("0", resultData)
		fileName := filepath.Base(file.Path.FSString())
		result.Set("1", js.ValueOf(fileName))
		resolve(result)
	})
}

// Wrap a function in a JS Promise.
func async(fn func(func(...any) js.Value, func(...any) js.Value)) any {
	var handler js.Func
	handler = js.FuncOf(func(this js.Value, args []js.Value) any {
		defer handler.Release()
		go fn(args[0].Invoke, args[1].Invoke)
		return nil
	})
	return js.Global().Get("Promise").New(handler)
}

// A HTTP client that uses the browser's built-in fetch API.
type WASMHTTPClient struct{}

func (c *WASMHTTPClient) Request(method string, url string, body []byte) (*clingHTTP.HTTPResponse, error) {
	// Convert body to JS.
	var bodyJS js.Value
	if len(body) > 0 {
		bodyJS = js.Global().Get("Uint8Array").New(len(body))
		js.CopyBytesToJS(bodyJS, body)
	} else {
		bodyJS = js.Undefined()
	}

	// Create fetch options.
	opts := js.Global().Get("Object").New()
	opts.Set("method", method)
	if !bodyJS.IsUndefined() {
		opts.Set("body", bodyJS)
	}

	// Channels for communication.
	respChan := make(chan *clingHTTP.HTTPResponse, 1)
	errChan := make(chan error, 1)

	// Start the fetch in a goroutine to manage callbacks.
	go func() {
		var fetchThen js.Func
		fetchThen = js.FuncOf(func(this js.Value, args []js.Value) any {
			defer fetchThen.Release()
			resp := args[0]
			statusCode := resp.Get("status").Int()
			var arrayBufferHandler js.Func

			arrayBufferHandler = js.FuncOf(func(this js.Value, args []js.Value) any {
				defer arrayBufferHandler.Release()
				arrayBuffer := args[0]
				uint8Array := js.Global().Get("Uint8Array").New(arrayBuffer)
				bodyBytes := make([]byte, uint8Array.Length())
				js.CopyBytesToGo(bodyBytes, uint8Array)

				respChan <- &clingHTTP.HTTPResponse{
					Body:       bodyBytes,
					StatusCode: statusCode,
				}
				return nil
			})
			resp.Call("arrayBuffer").Call("then", arrayBufferHandler)
			return nil
		})
		var fetchCatch js.Func
		fetchCatch = js.FuncOf(func(this js.Value, args []js.Value) any {
			defer fetchCatch.Release()
			errChan <- lib.Errorf("fetch error: %s", args[0].String())
			return nil
		})
		js.Global().Call("fetch", url, opts).
			Call("then", fetchThen).
			Call("catch", fetchCatch)
	}()

	// Wait for response or error.
	select {
	case resp := <-respChan:
		return resp, nil
	case err := <-errChan:
		return nil, err
	case <-time.After(30 * time.Second):
		return nil, lib.Errorf("request timeout")
	}
}
