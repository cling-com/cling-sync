// A minimal protobuf implementation that only supports what this project needs.
package lib

type ProtobufWriter interface {
	WriteTag(field, wireType int) error
	WriteVarint(v int64) error
	WriteUint64(field int, v uint64) error
	WriteBytes(field int, v []byte) error
	WriteMessage(field int, marshall func(ProtobufWriter) error) error
}

func VarintLen(v int64) int {
	u := uint64(v) //nolint:gosec
	n := 1
	for u > 0x7f {
		u >>= 7
		n++
	}
	return n
}

func TagLen(field, wireType int) int {
	return VarintLen(int64(field<<3 | wireType))
}

type ProtobufBytesWriter struct {
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
func NewProtobufWriter(out []byte) *ProtobufBytesWriter {
	return &ProtobufBytesWriter{out, 0}
}

func (w *ProtobufBytesWriter) Bytes() []byte {
	return w.out[0:w.offset]
}

func (w *ProtobufBytesWriter) WriteBytes(field int, v []byte) error {
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

func (w *ProtobufBytesWriter) WriteMessage(field int, marshall func(ProtobufWriter) error) error {
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

func (w *ProtobufBytesWriter) WriteTag(field, wireType int) error {
	return w.WriteVarint(int64(field<<3 | wireType))
}

func (w *ProtobufBytesWriter) WriteVarint(v_ int64) error {
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

func (w *ProtobufBytesWriter) WriteUint64(field int, v uint64) error {
	if err := w.WriteTag(field, 0); err != nil {
		return err
	}
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

type ProtobufSizeWriter struct {
	size int
}

func NewProtobufSizeWriter() *ProtobufSizeWriter {
	return &ProtobufSizeWriter{size: 0}
}

func (w *ProtobufSizeWriter) Size() int {
	return w.size
}

func (w *ProtobufSizeWriter) WriteTag(field, wireType int) error {
	w.size += TagLen(field, wireType)
	return nil
}

func (w *ProtobufSizeWriter) WriteVarint(v int64) error {
	w.size += VarintLen(v)
	return nil
}

func (w *ProtobufSizeWriter) WriteUint64(field int, v uint64) error {
	n := 1
	for v > 0x7f {
		v >>= 7
		n++
	}
	w.size += TagLen(field, 0) + n
	return nil
}

func (w *ProtobufSizeWriter) WriteBytes(field int, v []byte) error {
	w.size += TagLen(field, 2) + VarintLen(int64(len(v))) + len(v)
	return nil
}

func (w *ProtobufSizeWriter) WriteMessage(field int, marshall func(ProtobufWriter) error) error {
	inner := NewProtobufSizeWriter()
	if err := marshall(inner); err != nil {
		return err
	}
	w.size += TagLen(field, 2) + VarintLen(int64(inner.size)) + inner.size
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

func (r *ProtobufReader) ReadUint64() (uint64, error) {
	v, err := r.ReadVarint()
	if err != nil {
		return 0, err
	}
	// ReadVarint returns int64; uint64 covers the full bit pattern so any
	// value is in range. The cast just reinterprets the bits.
	return uint64(v), nil //nolint:gosec
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
