package lib

import (
	"cmp"
	"math/rand"
	"slices"
	"testing"
)

func TestHeap(t *testing.T) {
	t.Parallel()

	t.Run("Happy path", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 4)
		for _, v := range []int{5, 3, 9, 1, 7, 2, 8, 4, 6, 0} {
			h.Push(v)
		}
		assert.Equal(10, h.Len())
		assert.Equal(seq(10), drain(h))
	})

	t.Run("Empty heap", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 0)
		assert.Equal(0, h.Len())
		assert.Equal([]int{}, drain(h))
	})

	t.Run("Single element", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 4)
		h.Push(7)
		assert.Equal(7, h.Peek())
		assert.Equal(7, h.Pop())
		assert.Equal(0, h.Len())
	})

	t.Run("Peek", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 4)
		h.Push(3)
		h.Push(1)
		assert.Equal(1, h.Peek())
		assert.Equal(1, h.Peek())
		assert.Equal(2, h.Len())
	})

	t.Run("Ordered input", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 0)
		for v := range 32 {
			h.Push(v)
		}
		assert.Equal(seq(32), drain(h))
	})

	t.Run("Reversed input", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 0)
		for v := 31; v >= 0; v-- {
			h.Push(v)
		}
		assert.Equal(seq(32), drain(h))
	})

	t.Run("Duplicates", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 4)
		for _, v := range []int{2, 1, 2, 1, 2} {
			h.Push(v)
		}
		assert.Equal([]int{1, 1, 2, 2, 2}, drain(h))
	})

	t.Run("Equal elements", func(t *testing.T) {
		// Every comparison answers 0, so nothing tells the elements apart. The
		// sift loops must still terminate and lose nothing.
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(func(a, b int) int { return 0 }, 0)
		for v := range 64 {
			h.Push(v)
		}
		assert.Equal(64, h.Len())
		got := drain(h)
		slices.Sort(got)
		assert.Equal(seq(64), got)
	})

	t.Run("Tree shapes", func(t *testing.T) {
		// A size of 2^k-1 fills the last level, an even size leaves the last
		// parent with only a left child. Walk every shape up to 40.
		t.Parallel()
		assert := NewAssert(t)
		for size := range 41 {
			h := NewHeap(cmp.Compare[int], 0)
			for v := size - 1; v >= 0; v-- {
				h.Push(v)
				assertHeapOrder(t, h)
			}
			out := make([]int, 0, size)
			for h.Len() > 0 {
				out = append(out, h.Pop())
				assertHeapOrder(t, h)
			}
			assert.Equal(seq(size), out, "size %d", size)
		}
	})

	t.Run("Interleaved push and pop", func(t *testing.T) {
		// Keep a sorted reference alongside, so `Peek` is checked against the
		// true minimum after every step rather than only at the end.
		t.Parallel()
		assert := NewAssert(t)
		rnd := rand.New(rand.NewSource(7))
		h := NewHeap(cmp.Compare[int], 0)
		want := []int{}
		for step := range 2000 {
			if len(want) > 0 && rnd.Intn(2) == 0 {
				assert.Equal(want[0], h.Peek(), "step %d", step)
				assert.Equal(want[0], h.Pop(), "step %d", step)
				want = want[1:]
			} else {
				v := rnd.Intn(30)
				h.Push(v)
				want = append(want, v)
				slices.Sort(want)
			}
			assertHeapOrder(t, h)
			assert.Equal(len(want), h.Len(), "step %d", step)
		}
		assert.Equal(want, drain(h))
	})

	t.Run("Reuse after draining", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 4)
		for _, v := range []int{3, 1, 2} {
			h.Push(v)
		}
		assert.Equal([]int{1, 2, 3}, drain(h))
		for _, v := range []int{9, 5, 7} {
			h.Push(v)
		}
		assert.Equal([]int{5, 7, 9}, drain(h))
	})

	t.Run("Growth past capacity", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 1)
		for v := 99; v >= 0; v-- {
			h.Push(v)
		}
		assert.Equal(100, h.Len())
		assert.Equal(seq(100), drain(h))
	})

	t.Run("Random sequences", func(t *testing.T) {
		t.Parallel()
		assert := NewAssert(t)
		rnd := rand.New(rand.NewSource(1))
		for _, size := range []int{7, 63, 64, 65, 1000, 5000} {
			want := make([]int, 0, size)
			h := NewHeap(cmp.Compare[int], 0)
			for range size {
				v := rnd.Intn(size/4 + 1) // A small range, so ties are common.
				want = append(want, v)
				h.Push(v)
			}
			slices.Sort(want)
			assert.Equal(want, drain(h), "size %d", size)
		}
	})

	t.Run("Popped elements are released", func(t *testing.T) {
		// A heap that keeps popped elements reachable pins whatever they refer
		// to for as long as the heap lives.
		t.Parallel()
		assert := NewAssert(t)
		h := NewHeap(func(a, b *int) int { return cmp.Compare(*a, *b) }, 8)
		for v := range 8 {
			h.Push(&v)
		}
		drain(h)
		for i, item := range h.items[:cap(h.items)] {
			assert.Nil(item, "slot %d still holds a popped element", i)
		}
	})
}

// Drive the heap with a fuzzed operation sequence and check it against a sorted
// reference after every step.
func FuzzHeap(f *testing.F) {
	f.Add([]byte{3, 1, 4, 1, 5, 9, 2, 6})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{255, 254, 1, 0, 128})
	f.Fuzz(func(t *testing.T, ops []byte) {
		assert := NewAssert(t)
		h := NewHeap(cmp.Compare[int], 0)
		want := []int{}
		for _, op := range ops {
			// An even byte pops when there is something to pop, otherwise the
			// remaining bits are the value to push.
			if op%2 == 0 && len(want) > 0 {
				assert.Equal(want[0], h.Peek())
				assert.Equal(want[0], h.Pop())
				want = want[1:]
			} else {
				v := int(op >> 1)
				h.Push(v)
				want = append(want, v)
				slices.Sort(want)
			}
			assert.Equal(len(want), h.Len())
			assertHeapOrder(t, h)
		}
		assert.Equal(want, drain(h))
	})
}

// The integers `[0, n)`, for comparing against a drained heap.
func seq(n int) []int {
	out := make([]int, 0, n)
	for i := range n {
		out = append(out, i)
	}
	return out
}

func drain[T any](h *Heap[T]) []T {
	out := make([]T, 0, h.Len())
	for h.Len() > 0 {
		out = append(out, h.Pop())
	}
	return out
}

// No node may be smaller than its parent. Drained order alone does not prove
// this, so check the array itself.
func assertHeapOrder[T any](t *testing.T, h *Heap[T]) {
	t.Helper()
	assert := NewAssert(t)
	for i := 1; i < len(h.items); i++ {
		parent := (i - 1) / 2
		assert.Equal(true, h.compare(h.items[parent], h.items[i]) <= 0,
			"node %d is smaller than its parent %d", i, parent)
	}
}
