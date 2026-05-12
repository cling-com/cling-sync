// A minimal protobuf implementation that only supports what this project needs.
//
//go:generate go run protogen.go
package lib

type ProtobufWriter struct {
	out    []byte
	offset int
}

// `out` has to be large enough to hold the expected message. As a rule of
// thumb, the marshalled wire size plus 10 bytes per level of embedded-message
// nesting is safe (WriteMessage reserves a 10-byte scratch slot at each
// depth while it writes the inner message and patches the length varint).
//
// The Write* methods return an error when the buffer would overflow.
// Errors are not recoverable: by the time a write fails, the writer may
// have advanced past the tag (and possibly the length prefix) of the
// failing field. Callers must discard `Bytes()` on any Write* error.
func NewProtobufWriter(out []byte) *ProtobufWriter {
	return &ProtobufWriter{out, 0}
}

func (w *ProtobufWriter) Bytes() []byte {
	return w.out[0:w.offset]
}

func (w *ProtobufWriter) WriteBytes(field int, v []byte) error {
	if err := w.WriteTag(field, 2); err != nil {
		return err
	}
	if err := w.WriteVarint(int64(len(v))); err != nil {
		return err
	}
	if len(v) > len(w.out)-w.offset {
		return Errorf("buffer too small")
	}
	w.offset += copy(w.out[w.offset:], v)
	return nil
}

func (w *ProtobufWriter) WriteMessage(field int, marshall func(*ProtobufWriter) error) error {
	if err := w.WriteTag(field, 2); err != nil {
		return err
	}
	if w.offset+10 > len(w.out) {
		return Errorf("buffer too small")
	}
	ww := NewProtobufWriter(w.out[w.offset+10:])
	if err := marshall(ww); err != nil {
		return err
	}
	n := len(ww.Bytes())
	lengthStart := w.offset
	if err := w.WriteVarint(int64(n)); err != nil {
		return err
	}
	if gap := lengthStart + 10 - w.offset; gap > 0 {
		copy(w.out[w.offset:], w.out[w.offset+gap:w.offset+gap+n])
	}
	w.offset += n
	return nil
}

func (w *ProtobufWriter) WriteTag(field, wireType int) error {
	return w.WriteVarint(int64(field<<3 | wireType))
}

func (w *ProtobufWriter) WriteVarint(v_ int64) error {
	v := uint64(v_) //nolint:gosec
	for v > 0x7f {
		if w.offset >= len(w.out) {
			return Errorf("buffer too small")
		}
		w.out[w.offset] = byte(v&0x7f) | 0x80
		w.offset++
		v >>= 7
	}
	if w.offset >= len(w.out) {
		return Errorf("buffer too small")
	}
	w.out[w.offset] = byte(v)
	w.offset++
	return nil
}

type ProtobufReader struct {
	in     []byte
	offset int
}

func NewProtobufReader(in []byte) *ProtobufReader {
	return &ProtobufReader{in, 0}
}

func (r *ProtobufReader) AtEnd() bool {
	return r.offset >= len(r.in)
}

func (r *ProtobufReader) ReadVarint() (int64, error) {
	var result int64 = 0
	for i := range 10 {
		if r.offset >= len(r.in) {
			return 0, Errorf("truncated varint")
		}
		v := r.in[r.offset]
		r.offset++
		result |= int64(v&0x7f) << (7 * i)
		if v < 0x80 {
			return result, nil
		}
	}
	return 0, Errorf("varint exceeds 10 bytes")
}

func (r *ProtobufReader) ReadTag() (int, int, error) {
	t, err := r.ReadVarint()
	if err != nil {
		return 0, 0, err
	}
	return int(t >> 3), int(t & 0x07), nil
}

// Skip consumes the value of an unknown field given its wire type. Only the
// wire types format.proto uses (0 = varint, 2 = length-delimited) are
// supported; anything else returns an error.
func (r *ProtobufReader) Skip(wireType int) error {
	switch wireType {
	case 0:
		_, err := r.ReadVarint()
		return err
	case 2:
		l, err := r.ReadUint32()
		if err != nil {
			return err
		}
		if int(l) > len(r.in)-r.offset {
			return Errorf("truncated length-delimited field: want %d, have %d", l, len(r.in)-r.offset)
		}
		r.offset += int(l)
		return nil
	default:
		return Errorf("unsupported wire type %d", wireType)
	}
}

func (r *ProtobufReader) ReadUint32() (uint32, error) {
	v, err := r.ReadVarint()
	if err != nil {
		return 0, err
	}
	if v < 0 || v > 0xFFFFFFFF {
		return 0, Errorf("uint32 varint out of range: %d", v)
	}
	return uint32(v), nil
}

func (r *ProtobufReader) ReadBytes() ([]byte, error) {
	l, err := r.ReadUint32()
	if err != nil {
		return nil, err
	}
	if int(l) > len(r.in)-r.offset {
		return nil, Errorf("truncated bytes field: want %d, have %d", l, len(r.in)-r.offset)
	}
	res := make([]byte, l)
	copy(res, r.in[r.offset:r.offset+int(l)])
	r.offset += int(l)
	return res, nil
}
