package lib

import (
	"bytes"
	cryptoRand "crypto/rand"
	"errors"
	"io"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestGearCDCBasics(t *testing.T) {
	t.Parallel()
	original := "lore ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. lore ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. lore ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."
	expectedHappyPath := []string{
		"lore ipsum dolor sit amet, consectetur a",
		"dipiscing elit, sed do eiusmod tempor in",
		"cididunt ut labore et dolore mag",
		"na aliqua. lore ipsum dolor sit amet, co",
		"nsectetur adipiscing elit, sed do eiusmo",
		"d tempor incididunt ut lab",
		"ore et dolore magna aliqua. lore ip",
		"sum dolor sit amet, consectetur adipisci",
		"ng elit, sed do eiusmod tempor incididun",
		"t ut labore et dolore mag",
		"na aliqua.",
	}
	testExtended := func(t *testing.T, input string, table GearCDCTable, mask uint64, minSize, maxSize int) []string {
		t.Helper()
		buf := bytes.NewBufferString(input)
		sut := NewGearCDC(buf, mask, minSize, maxSize, table)
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
	test := func(t *testing.T, input string) []string {
		t.Helper()
		table, _ := NewGearCDCTable(RawKey{})
		return testExtended(t, input, table, (1<<4)-1, 20, 40)
	}
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		assert.Equal(expectedHappyPath, test(t, original))
	})
	t.Run("Change a character in the middle", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		modified := original[:len(original)/2] + "X" + original[len(original)/2+1:]
		assert.Equal([]string{
			expectedHappyPath[0],
			expectedHappyPath[1],
			expectedHappyPath[2],
			expectedHappyPath[3],
			"nsectetur adipiscing elit, sed dX eiusmo",
			expectedHappyPath[5],
			expectedHappyPath[6],
			expectedHappyPath[7],
			expectedHappyPath[8],
			expectedHappyPath[9],
			expectedHappyPath[10],
		}, test(t, modified))
	})
	t.Run("Insert a character at the beginning", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		modified := "X" + original
		assert.Equal([]string{
			"Xlore ipsum dolor sit amet, consectetur ",
			"adipiscing elit, sed do eiusmod tempor i",
			"ncididunt ut labore et dolore mag",
			"na aliqua. lore ipsum dolor sit amet, co",
			expectedHappyPath[4],
			expectedHappyPath[5],
			expectedHappyPath[6],
			expectedHappyPath[7],
			expectedHappyPath[8],
			expectedHappyPath[9],
			expectedHappyPath[10],
		}, test(t, modified))
	})
	t.Run("Insert a character in the middle", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		modified := original[:len(original)/2] + "X" + original[len(original)/2:]
		assert.Equal([]string{
			expectedHappyPath[0],
			expectedHappyPath[1],
			expectedHappyPath[2],
			expectedHappyPath[3],
			"nsectetur adipiscing elit, sed dXo eiusm",
			"od tempor incididunt ut lab",
			expectedHappyPath[6],
			expectedHappyPath[7],
			expectedHappyPath[8],
			expectedHappyPath[9],
			expectedHappyPath[10],
		}, test(t, modified))
	})
	t.Run("Remove a character at the beginning", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		modified := original[1:]
		assert.Equal([]string{
			"ore ipsum dolor sit amet, consectetur ad",
			"ipiscing elit, sed do eiusmod tempor inc",
			"ididunt ut labore et dolore mag",
			expectedHappyPath[3],
			expectedHappyPath[4],
			expectedHappyPath[5],
			expectedHappyPath[6],
			expectedHappyPath[7],
			expectedHappyPath[8],
			expectedHappyPath[9],
			expectedHappyPath[10],
		}, test(t, modified))
	})
	t.Run("Remove a character in the middle", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		modified := original[:len(original)/2] + original[len(original)/2+1:]
		assert.Equal([]string{
			expectedHappyPath[0],
			expectedHappyPath[1],
			expectedHappyPath[2],
			expectedHappyPath[3],
			"nsectetur adipiscing elit, sed d eiusmod",
			" tempor incididunt ut lab",
			expectedHappyPath[6],
			expectedHappyPath[7],
			expectedHappyPath[8],
			expectedHappyPath[9],
			expectedHappyPath[10],
		}, test(t, modified))
	})
}

func TestGearCDCWithDefaults(t *testing.T) {
	// Use the same values for `minSize`, `maxSize` and `mask` as we do
	// when committing a file.
	t.Parallel()
	test := func(t *testing.T, input []byte) []BlockId {
		t.Helper()
		assert := NewAssert(t)
		buf := bytes.NewBuffer(input)
		table, err := NewGearCDCTable(RawKey{})
		assert.NoError(err)
		sut := NewGearCDCWithDefaults(buf, table)

		i := 0
		var blockIds []BlockId
		var lastBlockLen int
		for {
			block, err := sut.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(err)
			inputBlockSha256 := CalculateSha256(input[i : i+len(block)])
			blockSha256 := CalculateSha256(block)
			assert.Equal(inputBlockSha256, blockSha256)
			i += len(block)
			blockIds = append(blockIds, BlockId(blockSha256))
			lastBlockLen = len(block)
		}
		// We aim for ~ 4 MB average block size.
		// We don't count the last block, because it's way smaller.
		if len(blockIds) > 1 {
			avgBlockSize := (len(input) - lastBlockLen) / (len(blockIds) - 1)
			assert.Greater(avgBlockSize, defaultMinBlockSize)
			assert.Greater(avgBlockSize, MaxBlockSize/2/100*75)
		}
		return blockIds
	}
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		original := randBytes(24_000_000)
		blockIds := test(t, original)
		assert.Equal(8, len(blockIds))
	})
	t.Run("Insert bytes in the middle", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		original := randBytes(24_000_000)
		modified := slices.Concat(original[:len(original)/2], randBytes(10000), original[len(original)/2:])
		originalBlockIds := test(t, original)
		modifiedBlockIds := test(t, modified)
		assert.Equal(len(originalBlockIds), len(modifiedBlockIds))
		changed := 0
		for i, original := range originalBlockIds {
			if original != modifiedBlockIds[i] {
				changed += 1
			}
		}
		assert.Equal(1, changed)
	})
	t.Run("Very small input", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		input := []byte{1}
		table, _ := NewGearCDCTable(RawKey{})
		sut := NewGearCDCWithDefaults(bytes.NewBuffer(input), table)

		block, err := sut.Read()
		assert.NoError(err)
		assert.Equal(input, block)
		_, err = sut.Read()
		assert.ErrorIs(err, io.EOF)
	})
	t.Run("Input at minSize", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		input := randBytes(defaultMinBlockSize)
		table, _ := NewGearCDCTable(RawKey{})
		sut := NewGearCDCWithDefaults(bytes.NewBuffer(input), table)

		block, err := sut.Read()
		assert.NoError(err)
		assert.Equal(input, block)
		_, err = sut.Read()
		assert.ErrorIs(err, io.EOF)
	})
	t.Run("Highly repetitive input", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		original := rollingBytes(24_000_000)
		originalBlockIds := test(t, original)
		// Repetitive data yields in lots of maxSize blocks.
		assert.Equal(3, len(originalBlockIds))

		// Make some changes in the first block
		modified := make([]byte, len(original))
		copy(modified, original)
		modified[0] += 1
		modifiedBlockIds := test(t, modified)
		assert.Equal([]BlockId{
			modifiedBlockIds[0], // This block changed.
			originalBlockIds[1],
			originalBlockIds[2],
		}, modifiedBlockIds)
		assert.NotEqual(originalBlockIds[0], modifiedBlockIds[0])

		// Remove the first byte, all blocks change.
		modified = make([]byte, len(original))
		copy(modified, original)
		modified = modified[1:]
		modifiedBlockIds = test(t, modified)
		assert.NotEqual(originalBlockIds[0], modifiedBlockIds[0])
		assert.NotEqual(originalBlockIds[1], modifiedBlockIds[1])
		assert.NotEqual(originalBlockIds[2], modifiedBlockIds[2])
	})
}

func TestGearCDCReaderReturnsDataWithEOF(t *testing.T) {
	// io.Reader is allowed to return (n>0, io.EOF) on the final read.
	// GearCDC must not drop those n bytes.
	t.Parallel()
	assert := NewAssert(t)
	input := []byte("hello world, this is a test of the gear cdc chunking algorithm")
	r := &eofDataReader{data: input, done: false}
	table, _ := NewGearCDCTable(RawKey{})
	sut := NewGearCDC(r, (1<<3)-1, 10, 20, table)
	var result []byte
	for {
		block, err := sut.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		assert.NoError(err)
		result = append(result, block...)
	}
	assert.Equal(string(input), string(result))
}

// eofDataReader returns all data in a single Read call with io.EOF,
// which is valid per the io.Reader contract.
type eofDataReader struct {
	data []byte
	done bool
}

func (r *eofDataReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		r.done = true
		return n, io.EOF
	}
	return n, nil
}

func BenchmarkGearCDCWithDefaults(b *testing.B) {
	b.SetBytes(100 * 1024 * 1024)
	table, _ := NewGearCDCTable(RawKey{})
	for b.Loop() {
		b.StopTimer()
		data := make([]byte, 100*1024*1024)
		_, err := cryptoRand.Reader.Read(data)
		if err != nil {
			b.Fatal(err)
		}
		sut := NewGearCDCWithDefaults(bytes.NewBuffer(data), table)
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
