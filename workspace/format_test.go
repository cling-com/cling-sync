//nolint:paralleltest,exhaustruct,forcetypeassert
//go:generate go run ../lib/protogen.go
package workspace

import (
	"bytes"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

// TestFormatMarshall encodes a message with the generated Marshall and asks
// protoc to decode the wire bytes against workspace/format.proto.
func TestFormatMarshall(t *testing.T) {
	assert := lib.NewAssert(t)
	err := exec.Command("protoc", "--version").Run()
	assert.NoError(err)

	type marshaller interface {
		Marshall(lib.ProtobufWriter) error
		MarshallSize() int
	}
	check := func(name string, msg marshaller, unmarshall any, expected string) {
		t.Run(name, func(t *testing.T) {
			assert := lib.NewAssert(t)
			w := lib.NewProtobufWriter(make([]byte, 4096))
			assert.NoError(msg.Marshall(w))
			assert.Equal(
				dedent(expected),
				protocDecode(t, reflect.TypeOf(msg).Elem().Name(), w.Bytes()),
			)
			assert.Equal(len(w.Bytes()), msg.MarshallSize(), "MarshallSize must match the encoded length")
			out := reflect.ValueOf(unmarshall).Call(
				[]reflect.Value{reflect.ValueOf(lib.NewProtobufReader(w.Bytes()))},
			)
			if !out[1].IsNil() {
				assert.NoError(out[1].Interface().(error))
				return
			}
			assert.Equal(reflect.ValueOf(msg).Elem().Interface(), out[0].Interface())
		})
	}

	uid, gid := uint32(1000), uint32(100)
	check("StagingEntry", &StagingEntry{
		RepoPath: td.Path("foo/bar.txt"),
		Metadata: lib.PathMetadata{
			FileMode: 0o644,
			Mtime:    lib.Timestamp{Sec: 1234567890, Nsec: 500000000},
			Size:     42,
			FileHash: sha256Hash("h"),
			Uid:      &uid,
			Gid:      &gid,
		},
		Ctime: lib.Timestamp{Sec: 999, Nsec: 1},
		Size:  42,
		Inode: 123456,
	}, UnmarshallStagingEntry, `
		repo_path: "foo/bar.txt"
		metadata {
		  file_mode: 420
		  mtime {
		    sec: 1234567890
		    nsec: 500000000
		  }
		  size: 42
		  file_hash: "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh"
		  uid: 1000
		  gid: 100
		}
		ctime {
		  sec: 999
		  nsec: 1
		}
		size: 42
		inode: 123456
	`)
}

func TestFormatValidate(t *testing.T) {
	check := func(name string, msg interface{ Validate() error }, wantErr string) {
		t.Run(name, func(t *testing.T) {
			assert := lib.NewAssert(t)
			err := msg.Validate()
			if wantErr == "" {
				assert.NoError(err)
				return
			}
			assert.Error(err, wantErr)
		})
	}

	check("StagingEntry zero value", &StagingEntry{}, "")
}

// sha256Hash fills a Sha256 with the given byte (repeated).
func sha256Hash(b string) lib.Sha256 {
	var h lib.Sha256
	for i := range h {
		h[i] = b[0]
	}
	return h
}

// protocDecode pipes the given wire bytes into `protoc -I.
// --decode=workspace.MESSAGE workspace/format.proto` (from the repo root,
// so that `import "lib/format.proto"` resolves) and returns the textproto
// output.
func protocDecode(t *testing.T, message string, in []byte) string {
	t.Helper()
	cmd := exec.Command("protoc", "-I.", "--decode=workspace."+message, "workspace/format.proto")
	cmd.Dir = ".."
	cmd.Stdin = bytes.NewReader(in)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	lib.NewAssert(t).NoError(cmd.Run(), "protoc --decode failed: %s", stderr.String())
	return stdout.String()
}

// dedent strips a leading newline and the common leading whitespace from
// every line, so raw-string expectations can be indented to match the
// surrounding Go code.
func dedent(s string) string {
	s = strings.TrimPrefix(s, "\n")
	lines := strings.Split(s, "\n")
	minIndent := -1
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		n := len(line) - len(strings.TrimLeft(line, " \t"))
		if minIndent == -1 || n < minIndent {
			minIndent = n
		}
	}
	if minIndent <= 0 {
		return s
	}
	for i, line := range lines {
		if strings.TrimSpace(line) == "" {
			lines[i] = ""
			continue
		}
		lines[i] = line[minIndent:]
	}
	return strings.Join(lines, "\n")
}
