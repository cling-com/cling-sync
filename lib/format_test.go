//nolint:paralleltest,exhaustruct,unparam,forcetypeassert
package lib

import (
	"bytes"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

// TestFormatMarshall encodes a message with the generated Marshall and asks
// protoc to decode the wire bytes against format.proto.
func TestFormatMarshall(t *testing.T) {
	assert := NewAssert(t)
	err := exec.Command("protoc", "--version").Run()
	assert.NoError(err)

	check := func(name string, msg interface{ Marshall(*ProtobufWriter) error }, unmarshall any, expected string) {
		t.Run(name, func(t *testing.T) {
			assert := NewAssert(t)
			w := NewProtobufWriter(make([]byte, 4096))
			assert.NoError(msg.Marshall(w))
			assert.Equal(
				dedent(expected),
				protocDecode(t, reflect.TypeOf(msg).Elem().Name(), w.Bytes()),
			)
			out := reflect.ValueOf(unmarshall).Call(
				[]reflect.Value{reflect.ValueOf(NewProtobufReader(w.Bytes()))},
			)
			if !out[1].IsNil() {
				assert.NoError(out[1].Interface().(error))
				return
			}
			assert.Equal(reflect.ValueOf(msg).Elem().Interface(), out[0].Interface())
		})
	}

	check("Timestamp", &Timestamp{Sec: 1234567890, Nsec: 500000000}, UnmarshallTimestamp, `
		sec: 1234567890
		nsec: 500000000
	`)

	check("Block1", &Block1{
		EncryptedHeader: []byte("header data"),
		EncryptedData:   []byte("payload"),
	}, UnmarshallBlock1, `
		encrypted_header: "header data"
		encrypted_data: "payload"
	`)

	check("Revision1 minimal", &Revision1{
		Timestamp:        Timestamp{Sec: 1234567890, Nsec: 500000000},
		ParentRevisionId: revisionId("a"),
	}, UnmarshallRevision1, `
		timestamp {
		  sec: 1234567890
		  nsec: 500000000
		}
		parent_revision_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	`)

	check("BlockHeader1", &BlockHeader1{
		Version:           1,
		BlockKind:         BlockKindRevision,
		Compression:       CompressionDeflate,
		Dek:               rawKey("k"),
		EncryptedDataSize: 1024,
	}, UnmarshallBlockHeader1, `
		version: 1
		block_kind: BlockKind_revision
		compression: Compression_deflate
		dek: "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk"
		encrypted_data_size: 1024
	`)

	check("File minimal", &File{
		FileMode: 0o644,
		Mtime:    Timestamp{Sec: 1234567890, Nsec: 500000000},
		Size:     42,
		FileHash: sha256Hash("h"),
	}, UnmarshallFile, `
		file_mode: 420
		mtime {
		  sec: 1234567890
		  nsec: 500000000
		}
		size: 42
		file_hash: "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh"
	`)

	uid, gid := uint32(1000), uint32(100)
	link := "/etc/passwd"
	birthtime := Timestamp{Sec: 999, Nsec: 1}
	check("File fully set", &File{
		FileMode:      FileModeSymlink | 0o777,
		Mtime:         Timestamp{Sec: 1234567890, Nsec: 500000000},
		Size:          128,
		FileHash:      sha256Hash("h"),
		BlockIds:      []BlockId{blockId("b"), blockId("c")},
		SymLinkTarget: &link,
		Uid:           &uid,
		Gid:           &gid,
		Birthtime:     &birthtime,
	}, UnmarshallFile, `
		file_mode: 2559
		mtime {
		  sec: 1234567890
		  nsec: 500000000
		}
		size: 128
		file_hash: "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh"
		block_ids: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		block_ids: "cccccccccccccccccccccccccccccccc"
		sym_link_target: "/etc/passwd"
		uid: 1000
		gid: 100
		birthtime {
		  sec: 999
		  nsec: 1
		}
	`)

	check("RevisionEntry1", &RevisionEntry1{
		Kind: RevisionEntryKindUpdate,
		Path: td.Path("foo/bar.txt"),
		File: File{
			FileMode: 0o644,
			Mtime:    Timestamp{Sec: 1234567890, Nsec: 500000000},
			Size:     42,
			FileHash: sha256Hash("h"),
		},
	}, UnmarshallRevisionEntry1, `
		kind: RevisionEntryKind_update
		path: "foo/bar.txt"
		file {
		  file_mode: 420
		  mtime {
		    sec: 1234567890
		    nsec: 500000000
		  }
		  size: 42
		  file_hash: "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh"
		}
	`)

	check("RevisionEntryChunk", &RevisionEntryChunk{
		Entries: []RevisionEntry1{
			{
				Kind: RevisionEntryKindUpdate,
				Path: td.Path("foo/bar.txt"),
				File: File{
					FileMode: 0o644,
					Mtime:    Timestamp{Sec: 1234567890, Nsec: 500000000},
					Size:     42,
					FileHash: sha256Hash("h"),
				},
			},
		},
	}, UnmarshallRevisionEntryChunk, `
		entries {
		  kind: RevisionEntryKind_update
		  path: "foo/bar.txt"
		  file {
		    file_mode: 420
		    mtime {
		      sec: 1234567890
		      nsec: 500000000
		    }
		    size: 42
		    file_hash: "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh"
		  }
		}
	`)

	msg, author := "hello", "alice"
	check("Revision1 fully set", &Revision1{
		Timestamp:        Timestamp{Sec: 1234567890, Nsec: 500000000},
		ParentRevisionId: revisionId("a"),
		Message:          &msg,
		Author:           &author,
		BlockIds:         []BlockId{blockId("b"), blockId("c")},
	}, UnmarshallRevision1, `
		timestamp {
		  sec: 1234567890
		  nsec: 500000000
		}
		parent_revision_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		message: "hello"
		author: "alice"
		block_ids: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		block_ids: "cccccccccccccccccccccccccccccccc"
	`)
}

func TestFormatUnmarshallLength(t *testing.T) {
	t.Run("BlockHeader1 dek wrong length", func(t *testing.T) {
		assert := NewAssert(t)
		w := NewProtobufWriter(make([]byte, 4096))
		assert.NoError(w.WriteBytes(4, make([]byte, 31)))
		_, err := UnmarshallBlockHeader1(NewProtobufReader(w.Bytes()))
		assert.Error(err, "BlockHeader1.Dek must have length 32")
	})
	t.Run("File block_ids entry wrong length", func(t *testing.T) {
		assert := NewAssert(t)
		w := NewProtobufWriter(make([]byte, 4096))
		assert.NoError(w.WriteBytes(5, make([]byte, 31)))
		_, err := UnmarshallFile(NewProtobufReader(w.Bytes()))
		assert.Error(err, "every entry in File.BlockIds must have length 32")
	})
	t.Run("uint32 varint overflow", func(t *testing.T) {
		assert := NewAssert(t)
		w := NewProtobufWriter(make([]byte, 16))
		assert.NoError(w.WriteTag(1, 0))
		assert.NoError(w.WriteVarint(int64(uint64(0x1_0000_0000)))) // > max uint32
		_, err := UnmarshallBlockHeader1(NewProtobufReader(w.Bytes()))
		assert.Error(err, "uint32 varint out of range")
	})
	t.Run("nested error propagates from File via RevisionEntry1", func(t *testing.T) {
		assert := NewAssert(t)
		file := NewProtobufWriter(make([]byte, 64))
		assert.NoError(file.WriteBytes(4, make([]byte, 31))) // wrong-length file_hash
		entry := NewProtobufWriter(make([]byte, 128))
		assert.NoError(entry.WriteTag(1, 0))
		assert.NoError(entry.WriteVarint(int64(RevisionEntryKindUpdate)))
		assert.NoError(entry.WriteBytes(3, file.Bytes()))
		_, err := UnmarshallRevisionEntry1(NewProtobufReader(entry.Bytes()))
		assert.Error(err, "File.FileHash must have length 32")
	})
}

// TestFormatUnmarshallSkipUnknown encodes a Timestamp interleaved with
// fields not in the current schema and verifies Unmarshall ignores them.
func TestFormatUnmarshallSkipUnknown(t *testing.T) {
	assert := NewAssert(t)
	w := NewProtobufWriter(make([]byte, 4096))
	assert.NoError(w.WriteTag(1, 0))
	assert.NoError(w.WriteVarint(42))
	assert.NoError(w.WriteTag(3, 0)) // unknown varint field
	assert.NoError(w.WriteVarint(99))
	assert.NoError(w.WriteTag(2, 0))
	assert.NoError(w.WriteVarint(7))
	assert.NoError(w.WriteBytes(4, []byte("future bytes"))) // unknown length-delim field

	ts, err := UnmarshallTimestamp(NewProtobufReader(w.Bytes()))
	assert.NoError(err)
	assert.Equal(Timestamp{Sec: 42, Nsec: 7}, ts)
}

func TestFormatValidate(t *testing.T) {
	check := func(name string, msg interface{ Validate() error }, wantErr string) {
		t.Run(name, func(t *testing.T) {
			assert := NewAssert(t)
			err := msg.Validate()
			if wantErr == "" {
				assert.NoError(err)
				return
			}
			assert.Error(err, wantErr)
		})
	}

	check("BlockHeader1 zero value", &BlockHeader1{}, "")
	check("BlockHeader1 invalid block_kind", &BlockHeader1{BlockKind: 99},
		"BlockHeader1.BlockKind has invalid value 99")
	check("BlockHeader1 invalid compression", &BlockHeader1{Compression: 99},
		"BlockHeader1.Compression has invalid value 99")
	check("RevisionEntry1 invalid kind", &RevisionEntry1{Kind: 99},
		"RevisionEntry1.Kind has invalid value 99")

	// Block1: encrypted_header <= 512, encrypted_data <= 8388080.
	check("Block1 zero value", &Block1{}, "")
	check("Block1 encrypted_header at boundary", &Block1{
		EncryptedHeader: make([]byte, 512),
	}, "")
	check("Block1 encrypted_header oversize", &Block1{
		EncryptedHeader: make([]byte, 513),
	}, "EncryptedHeader must not be longer than 512")
	check("Block1 encrypted_data at boundary", &Block1{
		EncryptedData: make([]byte, 8388080),
	}, "")
	check("Block1 encrypted_data oversize", &Block1{
		EncryptedData: make([]byte, 8388081),
	}, "EncryptedData must not be longer than 8388080")

	// Timestamp: no validation rules.
	check("Timestamp zero value", &Timestamp{}, "")

	// File: file_hash length (unreachable), block_ids cap (impractical 2^32-1),
	//       sym_link_target required if FileMode has Symlink bit.
	link := "/etc/passwd"
	check("File zero value", &File{}, "")
	check("File non-symlink, no target", &File{
		FileMode: 0o644,
	}, "")
	check("File symlink with target", &File{
		FileMode:      FileModeSymlink,
		SymLinkTarget: &link,
	}, "")
	check("File symlink without target", &File{
		FileMode: FileModeSymlink,
	}, "SymLinkTarget must be set")

	// RevisionEntry1: no validation rules.
	check("RevisionEntry1 zero value", &RevisionEntry1{}, "")

	// RevisionEntryChunk: entries cap is 2^24-1 — too large to materialize.
	check("RevisionEntryChunk zero value", &RevisionEntryChunk{}, "")

	// Revision1: parent_revision_id length (unreachable for [32]byte),
	//            block_ids cap of 65535.
	check("Revision1 zero value", &Revision1{}, "")
	check("Revision1 block_ids at boundary", &Revision1{
		BlockIds: make([]BlockId, 65535),
	}, "")
	check("Revision1 block_ids oversize", &Revision1{
		BlockIds: make([]BlockId, 65536),
	}, "BlockIds must not be longer than 65535")
}

// revisionId fills a RevisionId with the given byte (repeated) for readable
// textproto expectations.
func revisionId(b string) RevisionId {
	var id RevisionId
	for i := range id {
		id[i] = b[0]
	}
	return id
}

// blockId fills a BlockId with the given byte (repeated) for readable
// textproto expectations.
func blockId(b string) BlockId {
	var id BlockId
	for i := range id {
		id[i] = b[0]
	}
	return id
}

// rawKey fills a RawKey with the given byte (repeated).
func rawKey(b string) RawKey {
	var k RawKey
	for i := range k {
		k[i] = b[0]
	}
	return k
}

// sha256Hash fills a Sha256 with the given byte (repeated).
func sha256Hash(b string) Sha256 {
	var h Sha256
	for i := range h {
		h[i] = b[0]
	}
	return h
}

// protocDecode pipes the given wire bytes into `protoc --decode=MESSAGE
// format.proto` and returns the textproto output.
func protocDecode(t *testing.T, message string, in []byte) string {
	t.Helper()
	cmd := exec.Command("protoc", "--decode="+message, "format.proto")
	cmd.Stdin = bytes.NewReader(in)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	NewAssert(t).NoError(cmd.Run(), "protoc --decode failed: %s", stderr.String())
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
