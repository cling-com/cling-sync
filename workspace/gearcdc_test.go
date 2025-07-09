package workspace

import (
	"bytes"
	cryptoRand "crypto/rand"
	"errors"
	"io"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/flunderpero/cling-sync/lib"
)

func TestGearCDCBasics(t *testing.T) {
	t.Parallel()
	original := "lore ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
	expectedOriginal := []string{
		"lore ipsum dol",
		"or sit amet, conse",
		"ctetur adipiscing",
		" elit, sed do eiusmo",
		"d tempor incididunt",
		" ut labore et dol",
		"ore magna aliqua.",
	}
	test := func(input string, minSize, maxSize int, mask uint64) []string {
		t.Helper()
		buf := bytes.NewBufferString(input)
		sut := NewGearCDC(buf, mask, minSize, maxSize)
		res := []string{}
		var block []byte
		var err error
		for {
			block, err = sut.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			res = append(res, string(block))
		}
		return res
	}
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		assert.Equal(expectedOriginal, test(original, 10, 20, (1<<3)-1))
	})
	t.Run("Change a character in the middle", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		modified := original[:len(original)/2] + "X" + original[len(original)/2+1:]
		assert.Equal([]string{
			expectedOriginal[0],
			expectedOriginal[1],
			expectedOriginal[2],
			" elit, sed dX e",
			"iusmod tempo",
			"r incididunt",
			expectedOriginal[5],
			expectedOriginal[6],
		}, test(modified, 10, 20, (1<<3)-1))
	})
	t.Run("Insert a character at the beginning", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		modified := "X" + original
		assert.Equal([]string{
			"Xlore ipsum dol",
			expectedOriginal[1],
			expectedOriginal[2],
			expectedOriginal[3],
			expectedOriginal[4],
			expectedOriginal[5],
			expectedOriginal[6],
		}, test(modified, 10, 20, (1<<3)-1))
	})
	t.Run("Insert a character in the middle", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		modified := original[:len(original)/2] + "X" + original[len(original)/2:]
		assert.Equal([]string{
			expectedOriginal[0],
			expectedOriginal[1],
			expectedOriginal[2],
			" elit, sed dXo",
			" eiusmod t",
			"empor incididunt",
			expectedOriginal[5],
			expectedOriginal[6],
		}, test(modified, 10, 20, (1<<3)-1))
	})
	t.Run("Remove a character at the beginning", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		modified := original[1:]
		assert.Equal([]string{
			"ore ipsum dol",
			expectedOriginal[1],
			expectedOriginal[2],
			expectedOriginal[3],
			expectedOriginal[4],
			expectedOriginal[5],
			expectedOriginal[6],
		}, test(modified, 10, 20, (1<<3)-1))
	})
	t.Run("Remove a character in the middle", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		modified := original[:len(original)/2] + original[len(original)/2+1:]
		assert.Equal([]string{
			expectedOriginal[0],
			expectedOriginal[1],
			expectedOriginal[2],
			" elit, sed d e",
			"iusmod tempo",
			"r incididunt",
			expectedOriginal[5],
			expectedOriginal[6],
		}, test(modified, 10, 20, (1<<3)-1))
	})
}

func TestGearCDCWithDefaults(t *testing.T) {
	// Use the same values for `minSize`, `maxSize` and `mask` as we do
	// when committing a file.
	t.Parallel()
	assert := lib.NewAssert(t)
	test := func(input []byte) []lib.BlockId {
		t.Helper()
		buf := bytes.NewBuffer(input)
		sut := NewGearCDCWithDefaults(buf)

		i := 0
		var blockIds []lib.BlockId
		var lastBlockLen int
		for {
			block, err := sut.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(err)
			inputBlockSha256 := lib.CalculateSha256(input[i : i+len(block)])
			blockSha256 := lib.CalculateSha256(block)
			assert.Equal(inputBlockSha256, blockSha256)
			i += len(block)
			blockIds = append(blockIds, lib.BlockId(blockSha256))
			lastBlockLen = len(block)
		}
		// We aim for ~ 4 MB average block size.
		// We don't count the last block, because it's way smaller.
		if len(blockIds) > 1 {
			avgBlockSize := (len(input) - lastBlockLen) / (len(blockIds) - 1)
			assert.Greater(avgBlockSize, defaultMinBlockSize)
			assert.Greater(avgBlockSize, lib.MaxBlockSize/2/100*90)
		}
		return blockIds
	}
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		original := randBytes(24_000_000)
		blockIds := test(original)
		assert.Equal(7, len(blockIds))
	})
	t.Run("Insert bytes in the middle", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		original := randBytes(24_000_000)
		modified := slices.Concat(original[:len(original)/2], randBytes(10000), original[len(original)/2:])
		originalBlockIds := test(original)
		modifiedBlockIds := test(modified)
		expectedModifiedBlockIds := make([]lib.BlockId, len(originalBlockIds))
		copy(expectedModifiedBlockIds, originalBlockIds)
		expectedModifiedBlockIds[3] = modifiedBlockIds[3]
		assert.Equal(expectedModifiedBlockIds, modifiedBlockIds)
		assert.NotEqual(originalBlockIds[3], modifiedBlockIds[3])
	})
	t.Run("Very small input", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		input := []byte{1}
		sut := NewGearCDCWithDefaults(bytes.NewBuffer(input))

		block, err := sut.Read()
		assert.NoError(err)
		assert.Equal(input, block)
		_, err = sut.Read()
		assert.ErrorIs(err, io.EOF)
	})
	t.Run("Input at minSize", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		input := randBytes(defaultMinBlockSize)
		sut := NewGearCDCWithDefaults(bytes.NewBuffer(input))

		block, err := sut.Read()
		assert.NoError(err)
		assert.Equal(input, block)
		_, err = sut.Read()
		assert.ErrorIs(err, io.EOF)
	})
	t.Run("Highly repetitive input", func(t *testing.T) {
		t.Parallel()
		assert := lib.NewAssert(t)
		original := rollingBytes(24_000_000)
		originalBlockIds := test(original)
		// Repetitive data yields in lots of maxSize blocks.
		assert.Equal(3, len(originalBlockIds))

		// Make some changes in the first block
		modified := make([]byte, len(original))
		copy(modified, original)
		modified[0] += 1
		modifiedBlockIds := test(modified)
		assert.Equal([]lib.BlockId{
			modifiedBlockIds[0], // This block changed.
			originalBlockIds[1],
			originalBlockIds[2],
		}, modifiedBlockIds)
		assert.NotEqual(originalBlockIds[0], modifiedBlockIds[0])

		// Remove the first byte, all blocks change.
		modified = make([]byte, len(original))
		copy(modified, original)
		modified = modified[1:]
		modifiedBlockIds = test(modified)
		assert.NotEqual(originalBlockIds[0], modifiedBlockIds[0])
		assert.NotEqual(originalBlockIds[1], modifiedBlockIds[1])
		assert.NotEqual(originalBlockIds[2], modifiedBlockIds[2])
	})
}

func BenchmarkGearCDCWithDefaults(b *testing.B) {
	b.SetBytes(100 * 1024 * 1024)
	for b.Loop() {
		b.StopTimer()
		data := make([]byte, 100*1024*1024)
		_, err := cryptoRand.Reader.Read(data)
		if err != nil {
			b.Fatal(err)
		}
		sut := NewGearCDCWithDefaults(bytes.NewBuffer(data))
		b.StartTimer()
		for {
			_, err := sut.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	r := rand.NewPCG(1, 1)
	for i := range b {
		b[i] = byte(r.Uint64())
	}
	return b
}

func rollingBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}
