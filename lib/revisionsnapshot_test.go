package lib

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"testing"
)

func TestRevisionNWayMergeSort(t *testing.T) {
	t.Parallel()
	type Item struct {
		Value int
	}
	marshal := func(i Item, w io.Writer) error {
		return binary.Write(w, binary.LittleEndian, int64(i.Value))
	}
	unmarshal := func(r io.Reader) (Item, error) {
		var v int64
		err := binary.Read(r, binary.LittleEndian, &v)
		return Item{int(v)}, err
	}
	compare := func(a, b Item) (int, error) {
		return a.Value - b.Value, nil
	}
	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		chunks := [][]int{
			{1, 4, 7},
			{2, 5, 8},
			{3, 6, 9},
		}
		var readers []io.Reader
		for _, chunk := range chunks {
			buf := &bytes.Buffer{}
			for _, val := range chunk {
				_ = marshal(Item{val}, buf)
			}
			readers = append(readers, buf)
		}
		out := &bytes.Buffer{}
		err := nWayMergeSort[Item](readers, out, unmarshal, marshal, compare)
		assert.NoError(err)
		var values []int
		for {
			it, err := unmarshal(out)
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(err)
			values = append(values, it.Value)
		}
		assert.Equal([]int{1, 2, 3, 4, 5, 6, 7, 8, 9}, values)
	})
	t.Run("Fuzzing", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		chunks := make([]bytes.Buffer, 5)
		numValues := len(chunks) * 100
		for i := range numValues {
			// Bias towards lower indexes. This creates an uneven distribution
			// of entries across chunks.
			chunkIndex := int(rand.ExpFloat64()) % len(chunks)
			err := marshal(Item{Value: i}, &chunks[chunkIndex])
			assert.NoError(err)
		}
		readers := make([]io.Reader, len(chunks))
		for i, c := range chunks {
			readers[i] = &c
		}
		out := &bytes.Buffer{}
		err := nWayMergeSort(readers, out, unmarshal, marshal, compare)
		assert.NoError(err)
		for i := range numValues {
			it, err := unmarshal(out)
			assert.NoError(err)
			assert.Equal(i, it.Value)
		}
		_, err = unmarshal(out)
		assert.ErrorIs(err, io.EOF)
	})
	t.Run("Compare function error is propagated", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		buf1 := &bytes.Buffer{}
		buf2 := &bytes.Buffer{}
		_ = marshal(Item{1}, buf1)
		_ = marshal(Item{2}, buf2)
		badCompare := func(a, b Item) (int, error) {
			return 0, Errorf("Boom")
		}
		err := nWayMergeSort([]io.Reader{buf1, buf2}, &bytes.Buffer{}, unmarshal, marshal, badCompare)
		assert.Error(err, "Boom")
	})
}
