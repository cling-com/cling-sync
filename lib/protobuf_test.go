//nolint:forbidigo,paralleltest
package lib

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// protoTester encodes a single proto field and returns the wire bytes.
// Switch implementations with `PROTO_TEST_IMPL=protoc go test`. The protoc
// implementation is the canonical reference; lib delegates to ProtobufWriter
// in protogen.go.
type protoTester interface {
	encodeBytes(fieldNumber int, value []byte) []byte
}

func newProtoTester(tb testing.TB) protoTester {
	tb.Helper()
	if os.Getenv("PROTO_TEST_IMPL") == "protoc" {
		if err := exec.Command("protoc", "--version").Run(); err != nil {
			tb.Fatalf("PROTO_TEST_IMPL=protoc but protoc is not available: %v", err)
		}
		tb.Log("Using protoc proto tester")
		return newProtocProtoTester(tb)
	}
	tb.Log("Using lib proto tester")
	return newLibProtoTester(tb)
}

func TestProtoVarint(t *testing.T) {
	check := func(name string, expected []byte, value int64) {
		t.Run(name, func(t *testing.T) {
			assert := NewAssert(t)
			w := NewProtobufWriter(make([]byte, 4096))
			assert.NoError(w.WriteVarint(value))
			assert.Equal(expected, w.Bytes())
			got, err := NewProtobufReader(expected).ReadVarint()
			assert.NoError(err)
			assert.Equal(value, got)
		})
	}
	check("zero", []byte{0x00}, 0)
	check("one", []byte{0x01}, 1)
	check("127 (largest 1-byte varint)", []byte{0x7F}, 127)
	check("128 (smallest 2-byte varint)", []byte{0x80, 0x01}, 128)
	check("16383 (largest 2-byte varint)", []byte{0xFF, 0x7F}, 16383)
	check("16384 (smallest 3-byte varint)", []byte{0x80, 0x80, 0x01}, 16384)
	check("max int64 (9-byte varint)",
		[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F},
		math.MaxInt64)
	check("-1 (sign-extended to 10-byte varint)",
		[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		-1)
	check("min int64 (10-byte varint)",
		[]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01},
		math.MinInt64)
}

func TestProtoEncodeBytes(t *testing.T) {
	p := newProtoTester(t)
	check := func(name string, expected []byte, fieldNumber int, value []byte) {
		t.Run(name, func(t *testing.T) {
			assert := NewAssert(t)
			assert.Equal(expected, p.encodeBytes(fieldNumber, value))
			r := NewProtobufReader(expected)
			field, wireType, err := r.ReadTag()
			assert.NoError(err)
			assert.Equal(fieldNumber, field)
			assert.Equal(2, wireType)
			got, err := r.ReadBytes()
			assert.NoError(err)
			assert.Equal(value, got)
		})
	}
	check("empty", []byte{0x0A, 0x00}, 1, []byte{})
	check("single 0xFF", []byte{0x0A, 0x01, 0xFF}, 1, []byte{0xFF})
	check("0x00 0x01 0xFF", []byte{0x0A, 0x03, 0x00, 0x01, 0xFF}, 1, []byte{0x00, 0x01, 0xFF})

	b127 := bytes.Repeat([]byte{0xAB}, 127)
	check("127 bytes (largest 1-byte length)", append([]byte{0x0A, 0x7F}, b127...), 1, b127)

	b128 := bytes.Repeat([]byte{0xAB}, 128)
	check("128 bytes (smallest 2-byte length)", append([]byte{0x0A, 0x80, 0x01}, b128...), 1, b128)
}

// An embedded message is wire-format-identical to a length-delimited bytes
// field whose payload is the serialized inner message. The check round-trips
// the outer wrapping and recursively decodes the inner field.
func TestProtoEncodeMessage(t *testing.T) {
	p := newProtoTester(t)

	t.Run("Inner{v=42} at field 1", func(t *testing.T) {
		assert := NewAssert(t)
		inner := []byte{0x08, 0x2A} // tag 1, wire 0, varint 42
		expected := []byte{0x0A, 0x02, 0x08, 0x2A}
		assert.Equal(expected, p.encodeBytes(1, inner))

		r := NewProtobufReader(expected)
		field, wireType, err := r.ReadTag()
		assert.NoError(err)
		assert.Equal(1, field)
		assert.Equal(2, wireType)
		innerBytes, err := r.ReadBytes()
		assert.NoError(err)
		assert.Equal(inner, innerBytes)

		ri := NewProtobufReader(innerBytes)
		field, wireType, err = ri.ReadTag()
		assert.NoError(err)
		assert.Equal(1, field)
		assert.Equal(0, wireType)
		v, err := ri.ReadVarint()
		assert.NoError(err)
		assert.Equal(uint32(42), uint32(v))
	})

	t.Run("empty Inner at field 1", func(t *testing.T) {
		assert := NewAssert(t)
		expected := []byte{0x0A, 0x00}
		assert.Equal(expected, p.encodeBytes(1, nil))

		r := NewProtobufReader(expected)
		field, wireType, err := r.ReadTag()
		assert.NoError(err)
		assert.Equal(1, field)
		assert.Equal(2, wireType)
		got, err := r.ReadBytes()
		assert.NoError(err)
		assert.Equal([]byte{}, got)
	})
}

// WriteMessage emits a length-delimited field whose payload is produced by
// a sub-writer pointed at w.out[w.offset+5:]. After the inner Marshall it
// writes the canonical-width length varint at the original offset and
// shifts the inner bytes backward to close the gap. Tests exercise the
// shift across several length widths to confirm canonical bytes regardless
// of inner size.
func TestProtoWriteMessage(t *testing.T) {
	check := func(name string, fieldNumber int, inner []byte, expected []byte) {
		t.Run(name, func(t *testing.T) {
			assert := NewAssert(t)
			w := NewProtobufWriter(make([]byte, 4096))
			err := w.WriteMessage(fieldNumber, func(ww ProtobufWriter) error {
				return ww.WriteBytes(1, inner)
			})
			assert.NoError(err)
			assert.Equal(expected, w.Bytes())

			r := NewProtobufReader(expected)
			field, wireType, err := r.ReadTag()
			assert.NoError(err)
			assert.Equal(fieldNumber, field)
			assert.Equal(2, wireType)
			innerBytes, err := r.ReadBytes()
			assert.NoError(err)

			ri := NewProtobufReader(innerBytes)
			field, wireType, err = ri.ReadTag()
			assert.NoError(err)
			assert.Equal(1, field)
			assert.Equal(2, wireType)
			got, err := ri.ReadBytes()
			assert.NoError(err)
			assert.Equal(inner, got)
		})
	}

	// 1-byte inner-length varint → shift inner backward by 4.
	// Outer = [tag=0x0A, len=4, inner: tag=0x0A, len=2, 'h', 'i']
	check("small inner (1-byte length)", 1, []byte("hi"),
		[]byte{0x0A, 0x04, 0x0A, 0x02, 'h', 'i'})

	// 2-byte inner-length varint → shift inner backward by 3.
	// 128-byte payload + bytes-field header (3 bytes) = 131-byte inner → varint(131) = [0x83, 0x01].
	b128 := bytes.Repeat([]byte{0xAB}, 128)
	innerB128 := append([]byte{0x0A, 0x80, 0x01}, b128...)
	check("inner with 2-byte length varint", 1, b128,
		append([]byte{0x0A, 0x83, 0x01}, innerB128...))

	// Embedded message at a high tag (2-byte tag varint).
	check("small inner at field 16", 16, []byte("x"),
		[]byte{0x82, 0x01, 0x03, 0x0A, 0x01, 'x'})

	// Empty inner.
	t.Run("empty inner", func(t *testing.T) {
		assert := NewAssert(t)
		w := NewProtobufWriter(make([]byte, 4096))
		err := w.WriteMessage(1, func(_ ProtobufWriter) error { return nil })
		assert.NoError(err)
		assert.Equal([]byte{0x0A, 0x00}, w.Bytes())
	})

	// Errors from the inner Marshall propagate.
	t.Run("inner error propagates", func(t *testing.T) {
		assert := NewAssert(t)
		w := NewProtobufWriter(make([]byte, 4096))
		boom := Errorf("boom")
		err := w.WriteMessage(1, func(_ ProtobufWriter) error { return boom })
		assert.ErrorIs(err, boom)
	})
}

// ProtobufSizeWriter never writes anything; it only sums the number of
// bytes ProtobufBytesWriter would produce. The contract is: for any
// sequence of Write* calls, sw.Size() must equal len(bw.Bytes()).
func TestProtoSizeWriter(t *testing.T) {
	check := func(name string, write func(w ProtobufWriter) error) {
		t.Run(name, func(t *testing.T) {
			assert := NewAssert(t)
			bw := NewProtobufWriter(make([]byte, 4096))
			assert.NoError(write(bw))
			sw := NewProtobufSizeWriter()
			assert.NoError(write(sw))
			assert.Equal(len(bw.Bytes()), sw.Size())
		})
	}

	check("WriteVarint zero", func(w ProtobufWriter) error { return w.WriteVarint(0) })
	check("WriteVarint 1-byte boundary", func(w ProtobufWriter) error { return w.WriteVarint(127) })
	check("WriteVarint 2-byte boundary", func(w ProtobufWriter) error { return w.WriteVarint(128) })
	check("WriteVarint max int64", func(w ProtobufWriter) error { return w.WriteVarint(math.MaxInt64) })
	check("WriteVarint -1 (10-byte)", func(w ProtobufWriter) error { return w.WriteVarint(-1) })

	check("WriteTag field 1 wire 0", func(w ProtobufWriter) error { return w.WriteTag(1, 0) })
	check("WriteTag field 16 wire 2", func(w ProtobufWriter) error { return w.WriteTag(16, 2) })

	check("WriteBytes empty", func(w ProtobufWriter) error { return w.WriteBytes(1, nil) })
	check("WriteBytes short", func(w ProtobufWriter) error { return w.WriteBytes(1, []byte("hi")) })
	check("WriteBytes 128 (2-byte length)", func(w ProtobufWriter) error {
		return w.WriteBytes(1, bytes.Repeat([]byte{0xAB}, 128))
	})

	check("WriteMessage empty", func(w ProtobufWriter) error {
		return w.WriteMessage(1, func(_ ProtobufWriter) error { return nil })
	})
	check("WriteMessage one field", func(w ProtobufWriter) error {
		return w.WriteMessage(1, func(ww ProtobufWriter) error { return ww.WriteBytes(1, []byte("hi")) })
	})
	check("WriteMessage nested", func(w ProtobufWriter) error {
		return w.WriteMessage(2, func(ww ProtobufWriter) error {
			return ww.WriteMessage(1, func(www ProtobufWriter) error {
				return www.WriteBytes(1, bytes.Repeat([]byte{0xAB}, 200))
			})
		})
	})

	check("mixed calls", func(w ProtobufWriter) error {
		if err := w.WriteTag(1, 0); err != nil {
			return err
		}
		if err := w.WriteVarint(42); err != nil {
			return err
		}
		if err := w.WriteBytes(2, []byte("hello")); err != nil {
			return err
		}
		return w.WriteMessage(3, func(ww ProtobufWriter) error {
			return ww.WriteVarint(0xDEADBEEF)
		})
	})

	t.Run("inner error propagates", func(t *testing.T) {
		assert := NewAssert(t)
		sw := NewProtobufSizeWriter()
		boom := Errorf("boom")
		err := sw.WriteMessage(1, func(_ ProtobufWriter) error { return boom })
		assert.ErrorIs(err, boom)
	})
}

// A repeated bytes/string/message field is wire-format-identical to its
// non-repeated entries concatenated. There is no envelope tag and no count.
// (Repeated scalar varints are packed in proto3, but cling-sync only repeats
// length-delimited types, so packed encoding is out of scope.)
func TestProtoEncodeRepeatedBytes(t *testing.T) {
	p := newProtoTester(t)

	t.Run("two entries at field 1", func(t *testing.T) {
		assert := NewAssert(t)
		entries := [][]byte{[]byte("abc"), []byte("de")}

		actual := append(p.encodeBytes(1, entries[0]), p.encodeBytes(1, entries[1])...)
		expected := []byte{0x0A, 0x03, 'a', 'b', 'c', 0x0A, 0x02, 'd', 'e'}
		assert.Equal(expected, actual)

		r := NewProtobufReader(expected)
		got := [][]byte{} //nolint:prealloc
		for range entries {
			field, wireType, err := r.ReadTag()
			assert.NoError(err)
			assert.Equal(1, field)
			assert.Equal(2, wireType)
			b, err := r.ReadBytes()
			assert.NoError(err)
			got = append(got, b)
		}
		assert.Equal(entries, got)
	})
}

type libProtoTester struct{ tb testing.TB }

func newLibProtoTester(tb testing.TB) *libProtoTester {
	tb.Helper()
	return &libProtoTester{tb}
}

func libEncode(write func(ProtobufWriter) error) []byte {
	w := NewProtobufWriter(make([]byte, 4096))
	if err := write(w); err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (l *libProtoTester) encodeBytes(fn int, v []byte) []byte {
	return libEncode(func(w ProtobufWriter) error { return w.WriteBytes(fn, v) })
}

// protocProtoTester invokes `protoc --encode` to produce canonical wire bytes.
// A fresh proto3 schema is generated per call. The `optional` keyword (proto3,
// stable since protoc 3.15) forces serialization of scalar defaults like 0,
// "", and empty bytes; the wire format is identical to a non-optional field.
type protocProtoTester struct {
	tb  testing.TB
	dir string
}

func newProtocProtoTester(tb testing.TB) *protocProtoTester {
	tb.Helper()
	return &protocProtoTester{tb: tb, dir: tb.TempDir()}
}

func (p *protocProtoTester) encodeBytes(fn int, v []byte) []byte {
	return p.encode(fmt.Sprintf("optional bytes v = %d;", fn), "v: "+protoStringLiteral(v))
}

func (p *protocProtoTester) encode(field, text string) []byte {
	schema := fmt.Sprintf("syntax = \"proto3\";\nmessage M { %s }\n", field)
	return p.runProtoc(schema, text)
}

func (p *protocProtoTester) runProtoc(schema, text string) []byte {
	p.tb.Helper()
	assert := NewAssert(p.tb)
	schemaPath := filepath.Join(p.dir, "test.proto")
	err := os.WriteFile(schemaPath, []byte(schema), 0o600)
	assert.NoError(err, "write schema")
	cmd := exec.Command("protoc", "--encode=M", "--proto_path="+p.dir, "test.proto")
	cmd.Stdin = strings.NewReader(text)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	assert.NoError(err, "protoc --encode failed: %s", stderr.String())
	return stdout.Bytes()
}

// protoStringLiteral renders a textproto string literal with every byte
// emitted as a 3-digit octal escape so binary content survives intact.
func protoStringLiteral(b []byte) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for _, c := range b {
		fmt.Fprintf(&sb, `\%03o`, c)
	}
	sb.WriteByte('"')
	return sb.String()
}
