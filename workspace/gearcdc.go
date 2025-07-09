// # Content-Defined Chunking
//
// Implementation of the GearCDC algorithm to re-use blocks when the contents
// changed slightly (data added or removed somewhere in the middle).
//
// See https://joshleeb.com/posts/gear-hashing.html
package workspace

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"

	"github.com/flunderpero/cling-sync/lib"
)

type GearCDC struct {
	table     [256]uint64
	r         io.Reader
	buf       []byte
	bufSize   int
	bufOffset int
	mask      uint64
	minSize   int
	maxSize   int
}

const (
	defaultMinBlockSize = lib.MaxBlockDataSize / 4
	defaultMaxBlockSize = lib.MaxBlockDataSize
	defaultMask         = (1 << 21) - 1 // ~ 2-4MB average block size.
)

func NewGearCDCWithDefaults(r io.Reader) *GearCDC {
	return NewGearCDC(r, defaultMask, defaultMinBlockSize, defaultMaxBlockSize)
}

func NewGearCDC(r io.Reader, mask uint64, minSize, maxSize int) *GearCDC {
	// Using a fixed seed to make the hash deterministic.
	random := rand.NewPCG(123456789, 987654321)
	table := [256]uint64{}
	for i := range table {
		table[i] = random.Uint64()
	}
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
			if errors.Is(err, io.EOF) || n == 0 {
				if dstIndex > 0 {
					// This is the last block, emit it.
					break
				}
				return nil, io.EOF
			}
			if err != nil {
				return nil, lib.WrapErrorf(err, "failed to read from underlying reader")
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
