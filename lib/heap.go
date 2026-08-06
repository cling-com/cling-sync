package lib

// A binary min-heap ordered by `compare`, which returns a negative number when
// `a` comes before `b`, like `slices.SortFunc`.
//
// Go's `container/heap` would box every element into `any` and needs a five-method
// interface implementation. It also sifts down with two comparisons per level,
// which is the wrong trade when comparing is expensive.
type Heap[T any] struct {
	items   []T
	compare func(a, b T) int
}

func NewHeap[T any](compare func(a, b T) int, capacity int) *Heap[T] {
	return &Heap[T]{items: make([]T, 0, capacity), compare: compare}
}

func (h *Heap[T]) Len() int {
	return len(h.items)
}

// The smallest element. The heap must not be empty.
func (h *Heap[T]) Peek() T {
	return h.items[0]
}

func (h *Heap[T]) Push(v T) {
	h.items = append(h.items, v)
	h.up(len(h.items)-1, v)
}

// Remove and return the smallest element. The heap must not be empty.
//
// The root is refilled by moving the gap all the way down to a leaf and only
// then bubbling the last element back up (Floyd's bounce). That costs one
// comparison per level instead of two, and the bubble-up nearly always stops at
// once because the element came from the bottom to begin with. It pays for
// itself whenever comparing costs more than moving.
func (h *Heap[T]) Pop() T {
	smallest := h.items[0]
	last := len(h.items) - 1
	v := h.items[last]
	var zero T
	h.items[last] = zero // Do not keep the popped element reachable.
	h.items = h.items[:last]
	if last == 0 {
		return smallest
	}
	// Move the gap down, always following the smaller child.
	i := 0
	for {
		child := 2*i + 1
		if child >= len(h.items) {
			break
		}
		if right := child + 1; right < len(h.items) && h.compare(h.items[right], h.items[child]) < 0 {
			child = right
		}
		h.items[i] = h.items[child]
		i = child
	}
	h.up(i, v)
	return smallest
}

// Move `v` up from `i` until its parent is no longer greater. Parents shift
// down into the gap so `v` is written exactly once.
func (h *Heap[T]) up(i int, v T) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.compare(v, h.items[parent]) >= 0 {
			break
		}
		h.items[i] = h.items[parent]
		i = parent
	}
	h.items[i] = v
}
