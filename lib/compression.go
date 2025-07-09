package lib

import (
	"bytes"
	"compress/zlib"
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

// Compress data using the Deflate algorithm level 6.
// todo: document the compression level.
func Compress(data []byte) ([]byte, error) {
	w := bytes.NewBuffer(nil)
	z, err := zlib.NewWriterLevel(w, 6)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create zlib writer")
	}
	_, err = z.Write(data)
	if err != nil {
		return nil, WrapErrorf(err, "failed to write to zlib writer")
	}
	err = z.Close()
	if err != nil {
		return nil, WrapErrorf(err, "failed to close zlib writer")
	}
	return w.Bytes(), nil
}

func Decompress(data []byte) ([]byte, error) {
	r := bytes.NewReader(data)
	z, err := zlib.NewReader(r)
	if err != nil {
		return nil, WrapErrorf(err, "failed to create zlib reader")
	}
	defer z.Close() //nolint:errcheck
	res, err := io.ReadAll(z)
	if err != nil {
		return nil, WrapErrorf(err, "failed to read from zlib reader")
	}
	return res, nil
}
