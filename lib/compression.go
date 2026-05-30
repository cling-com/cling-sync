package lib

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"
	"math"
)

const (
	compressionCheckSize = 1024
)

// Calculate the "entropy" of the data using the Shannon
// entropy formula to decide whether it should be compressed.
// (https://en.wikipedia.org/wiki/Entropy_(information_theory)
//
// We only look at the first `compressionCheckSize` bytes.
func IsCompressible(data []byte) bool {
	if len(data) < compressionCheckSize {
		return false
	}
	freq := make(map[byte]int)
	for _, b := range data[:compressionCheckSize] {
		freq[b]++
	}
	var h float64
	for _, v := range freq {
		if v == 0 {
			continue
		}
		f := float64(v) / float64(compressionCheckSize)
		h -= f * math.Log2(f)
	}
	// `h` now contains the number of bits of entropy (i.e. the number of bits that carry information).
	return h < 7
}

var errCompressOverflow = errors.New("compressed output exceeds target")

// cappedWriter writes into a fixed buffer and reports `errCompressOverflow`
// once it would write past the end.
type cappedWriter struct {
	buf []byte
	n   int
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	n := copy(w.buf[w.n:], p)
	w.n += n
	if n < len(p) {
		return n, errCompressOverflow
	}
	return n, nil
}

// Compress writes Deflate-compressed `data` into `target` and returns the
// number of bytes written. `ok` is false when the compressed output would not
// fit in `target`, which the caller treats as "not worth compressing".
func Compress(data []byte, target []byte) (n int, ok bool, err error) {
	w := &cappedWriter{buf: target, n: 0}
	z, err := zlib.NewWriterLevel(w, 6)
	if err != nil {
		return 0, false, WrapErrorf(err, "failed to create zlib writer")
	}
	if _, err := z.Write(data); err != nil {
		if errors.Is(err, errCompressOverflow) {
			return 0, false, nil
		}
		return 0, false, WrapErrorf(err, "failed to write to zlib writer")
	}
	if err := z.Close(); err != nil {
		if errors.Is(err, errCompressOverflow) {
			return 0, false, nil
		}
		return 0, false, WrapErrorf(err, "failed to close zlib writer")
	}
	return w.n, true, nil
}

func Decompress(data []byte) ([]byte, error) {
	r := bytes.NewReader(data)
	z, err := zlib.NewReader(r)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create zlib reader")
	}
	defer z.Close() //nolint:errcheck
	res, err := io.ReadAll(io.LimitReader(z, MaxBlockDataSize+1))
	if err != nil {
		return nil, WrapErrorf(err, "failed to read from zlib reader")
	}
	if len(res) > MaxBlockDataSize {
		return nil, Errorf("decompressed size exceeds maximum block size %d", MaxBlockDataSize)
	}
	return res, nil
}
