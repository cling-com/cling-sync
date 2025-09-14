//go:build wasm

package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
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

const MaxDownloadSize = 500 * 1024 * 1024

var repositoryHandles = make(map[int]*lib.Repository)

func BuildRepositoryAPI() js.Value {
	repositoryAPI := &RepositoryAPI{}
	api := js.Global().Get("Object").New()
	api.Set("open", js.FuncOf(repositoryAPI.Open))
	api.Set("head", js.FuncOf(repositoryAPI.Head))
	api.Set("ls", js.FuncOf(repositoryAPI.Ls))
	api.Set("readFile", js.FuncOf(repositoryAPI.ReadFile))
	return api
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
	return Async(func(resolve func(js.Value), reject func(js.Value)) {
		httpClient := &WasmHTTPClient{}
		storage := clingHTTP.NewHTTPStorageClient(url, httpClient)
		repository, err := lib.OpenRepository(storage, []byte(passphrase))
		if err != nil {
			reject(js.ValueOf(err.Error()))
			return
		}
		handle := len(repositoryHandles)
		repositoryHandles[handle] = repository
		resolve(js.ValueOf(handle))
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
	return Async(func(resolve func(js.Value), reject func(js.Value)) {
		repository, ok := repositoryHandles[handle]
		if !ok {
			reject(js.ValueOf(fmt.Sprintf("invalid repository handle: %d", handle)))
			return
		}
		revisionId, err := repository.Head()
		if err != nil {
			reject(js.ValueOf(err.Error()))
			return
		}
		resolve(js.ValueOf(revisionId.String()))
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
	return Async(func(resolve func(js.Value), reject func(js.Value)) {
		repository, ok := repositoryHandles[handle]
		if !ok {
			reject(js.ValueOf(fmt.Sprintf("invalid repository handle: %d", handle)))
			return
		}
		revisionId, err := repository.Head()
		if err != nil {
			reject(js.ValueOf(err.Error()))
			return
		}
		// todo: is this a reasonable limit?
		tmpFS := lib.NewMemoryFS(100_000_000)
		var filter lib.PathFilter
		if excludes != "" {
			filter = lib.NewPathExclusionFilter(strings.Split(excludes, ","))
		}
		opts := &workspace.LsOptions{RevisionId: revisionId, PathFilter: filter}
		files, err := workspace.Ls(repository, tmpFS, opts)
		if err != nil {
			reject(js.ValueOf(err.Error()))
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
					"<td><a href=\"#download:" + base64.StdEncoding.EncodeToString([]byte(file.Path.String())) + "\">",
				)
				sb.WriteString(html.EscapeString(file.Path.String()))
				sb.WriteString("</a></td>")
			} else {
				sb.WriteString("<td>")
				sb.WriteString(html.EscapeString(file.Path.String()))
				sb.WriteString("/</td>")
			}
			sb.WriteString("<td>")
			sb.WriteString(html.EscapeString(file.Metadata.MTime().Format(time.RFC3339)))
			sb.WriteString("</td>")
			sb.WriteString("</tr>")
		}
		resolve(js.ValueOf(sb.String()))
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
	return Async(func(resolve func(js.Value), reject func(js.Value)) {
		handle := args[0].Int()
		pathBytes, err := base64.StdEncoding.DecodeString(args[1].String())
		if err != nil {
			reject(js.ValueOf(err.Error()))
			return
		}
		path := string(pathBytes)
		revisionIdArg := args[2].String()
		repository, ok := repositoryHandles[handle]
		if !ok {
			reject(js.ValueOf(fmt.Sprintf("invalid repository handle: %d", handle)))
			return
		}
		var revisionId lib.RevisionId
		if revisionIdArg == "" {
			var err error
			revisionId, err = repository.Head()
			if err != nil {
				reject(js.ValueOf(err.Error()))
				return
			}
		} else {
			revisionId = lib.RevisionId([]byte(revisionIdArg))
		}
		tmpFS := lib.NewMemoryFS(10000000)
		snapshot, err := lib.NewRevisionSnapshot(repository, revisionId, tmpFS)
		if err != nil {
			reject(js.ValueOf(err.Error()))
			return
		}
		filter := lib.NewPathInclusionFilter([]string{path})
		r := snapshot.Reader(lib.RevisionEntryPathFilter(filter))
		file, err := r.Read()
		if errors.Is(err, io.EOF) {
			reject(js.ValueOf(fmt.Sprintf("file not found: %s", path)))
			return
		}
		if err != nil {
			reject(js.ValueOf(err.Error()))
			return
		}
		if file.Metadata.Size > MaxDownloadSize {
			reject(js.ValueOf(fmt.Sprintf("file too large: %s", path)))
			return
		}
		if !file.Metadata.ModeAndPerm.IsRegular() {
			reject(js.ValueOf(fmt.Sprintf("not a regular file: %s", path)))
			return
		}
		data := bytes.NewBuffer(nil)
		data.Grow(int(file.Metadata.Size))
		for _, blockId := range file.Metadata.BlockIds {
			block, _, err := repository.ReadBlock(blockId)
			if err != nil {
				reject(js.ValueOf(err.Error()))
				return
			}
			data.Write(block)
		}
		resultData := js.Global().Get("Uint8Array").New(data.Len())
		js.CopyBytesToJS(resultData, data.Bytes())
		result := js.Global().Get("Array").New()
		result.Set("0", resultData)
		fileName := filepath.Base(file.Path.String())
		result.Set("1", js.ValueOf(fileName))
		resolve(result)
	})
}
