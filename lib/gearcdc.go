// # Content-Defined Chunking
//
// Implementation of the GearCDC algorithm to re-use blocks when the contents
// changed slightly (data added or removed somewhere in the middle).
//
// See https://joshleeb.com/posts/gear-hashing.html
package lib

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

type GearCDCTable [256]uint64

type GearCDC struct {
	table     GearCDCTable
	r         io.Reader
	buf       []byte
	bufSize   int
	bufOffset int
	mask      uint64
	minSize   int
	maxSize   int
}

const (
	defaultMinBlockSize = MaxBlockDataSize / 4
	defaultMaxBlockSize = MaxBlockDataSize
	defaultMask         = (1 << 21) - 1 // ~ 2-4MB average block size.
)

func NewGearCDCWithDefaults(r io.Reader, table GearCDCTable) *GearCDC {
	return NewGearCDC(r, defaultMask, defaultMinBlockSize, defaultMaxBlockSize, table)
}

// Initialize the GearCDC.
func NewGearCDC(r io.Reader, mask uint64, minSize, maxSize int, table GearCDCTable) *GearCDC {
	return &GearCDC{
		table: table, r: r, buf: make([]byte, 64*1024), bufSize: 0, bufOffset: 0,
		mask:    mask,
		minSize: minSize,
		maxSize: maxSize,
	}
}

// Read from the underlying reader until we reach a block boundary.
// If we reach the end of the underlying reader, return `io.EOF`.
//
// Return a slice of `dst` with the read bytes.
func (g *GearCDC) Read() ([]byte, error) {
	i := g.bufOffset
	dstIndex := 0
	var window uint64
	buf := bytes.NewBuffer(nil)
	for {
		if i == g.bufSize {
			n, err := g.r.Read(g.buf)
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, WrapErrorf(err, "failed to read from underlying reader")
			}
			if n == 0 {
				if dstIndex > 0 {
					// This is the last block, emit it.
					break
				}
				return nil, io.EOF
			}
			g.bufSize = n
			i = 0
		}
		b := g.buf[i]
		buf.WriteByte(b)
		dstIndex++
		i++
		window = (window << 1) ^ g.table[b]
		if (window&g.mask == 0 && dstIndex >= g.minSize) || dstIndex >= g.maxSize {
			break
		}
	}
	g.bufOffset = i
	return buf.Bytes(), nil
}

func NewGearCDCTable(key RawKey) (GearCDCTable, error) {
	zeros := [8 * 256]byte{}
	cipher, err := NewCipher(key)
	if err != nil {
		return GearCDCTable{}, WrapErrorf(err, "failed to created cipher for GearCDC table creation")
	}
	// We use an all-zero nonce which is ok, because we only use the given key for this single
	// piece of encryption.
	nonce := make([]byte, cipher.NonceSize())
	bytes := cipher.Seal(nil, nonce, zeros[:], nil)
	table := GearCDCTable{}
	for i := range table {
		table[i] = binary.LittleEndian.Uint64(bytes[i*8 : (i+1)*8])
	}
	return table, nil
}
