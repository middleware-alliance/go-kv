package index

import (
	"bytes"
	"github.com/google/btree"
	"go-kv/data"
	"sort"
	"sync"
)

// BTree is a wrapper around the google/btree package.
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree creates a new BTree instance.
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// NewBTreeWithDegree creates a new BTree instance.
func NewBTreeWithDegree(degree int) *BTree {
	return &BTree{
		tree: btree.New(degree),
		lock: new(sync.RWMutex),
	}
}

// Put inserts a new key-value pair into the BTree.
// If the key already exists, it will be overwritten.
// Returns true if the key was inserted or overwritten, false otherwise.
func (B *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{
		Key: key,
		pos: pos,
	}
	B.lock.Lock()
	defer B.lock.Unlock()
	_ = B.tree.ReplaceOrInsert(it)
	return true
}

// Get retrieves the value associated with the given key.
// Returns nil if the key does not exist.
// Note that the returned value is a pointer to a LogRecordPos struct.
func (B *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{
		Key: key,
	}
	item := B.tree.Get(it)
	if item == nil {
		return nil
	}
	return item.(*Item).pos
}

// Delete removes the key-value pair associated with the given key.
// Returns true if the key was deleted, false otherwise.
func (B *BTree) Delete(key []byte) bool {
	it := &Item{
		Key: key,
	}
	B.lock.Lock()
	defer B.lock.Unlock()
	item := B.tree.Delete(it)
	return item != nil
}

// Size returns the number of key-value pairs in the BTree.
func (B *BTree) Size() int {
	return B.tree.Len()
}

// Iterator returns a new iterator for the BTree.
func (B *BTree) Iterator(reverse bool) Iterator {
	if B.tree == nil {
		return nil
	}
	B.lock.RLock()
	defer B.lock.RUnlock()
	return newBtreeIterator(B.tree, reverse)
}

// btreeIterator is an iterator for the BTree.
type btreeIterator struct {
	currIndex int     // current index in the BTree
	reverse   bool    // whether to iterate in reverse order
	values    []*Item // slice of values in the BTree
}

// newBtreeIterator creates a new iterator for the BTree.
func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// save all the values in the BTree into the values slice
	save := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(save)
	} else {
		tree.Ascend(save)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind resets the iterator to the beginning of the BTree.
func (b *btreeIterator) Rewind() {
	b.currIndex = 0
}

// Seek moves the iterator to the position of the first key greater than or equal to the given key.
func (b *btreeIterator) Seek(key []byte) {
	if b.reverse {
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].Key, key) <= 0
		})
	} else {
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].Key, key) >= 0
		})
	}
}

// Next moves the iterator to the next position.
func (b *btreeIterator) Next() {
	b.currIndex += 1
}

// Valid returns whether the iterator is currently pointing to a valid position.
func (b *btreeIterator) Valid() bool {
	return b.currIndex < len(b.values)
}

// Key returns the current key.
func (b *btreeIterator) Key() []byte {
	return b.values[b.currIndex].Key
}

// Value returns the current value.
func (b *btreeIterator) Value() *data.LogRecordPos {
	return b.values[b.currIndex].pos
}

// Close releases any resources associated with the iterator.
func (b *btreeIterator) Close() {
	b.values = nil
}
