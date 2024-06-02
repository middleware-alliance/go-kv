package index

import (
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"go-kv/data"
	"sort"
	"sync"
)

// AdaptiveRadixTree is a wrapper around go-adaptive-radix-tree
// to provide a more Go-like interface.
// https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART creates a new AdaptiveRadixTree instance.
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: &sync.RWMutex{},
	}
}

// Put inserts a key-value pair into the ART.
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	art.tree.Insert(key, pos)
	return true
}

// Get retrieves the value associated with a key from the ART.
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// Delete removes a key-value pair from the ART.
func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, deleted := art.tree.Delete(key)
	return deleted
}

// Size returns the number of key-value pairs in the ART.
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.tree.Size()
}

// Iterator returns an Iterator over the key-value pairs in the ART.
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}

// Close releases any resources associated with the BTree.
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// artIterator is an iterator for the AdaptiveRadixTree.
type artIterator struct {
	currIndex int     // current index in the BTree
	reverse   bool    // whether to iterate in reverse order
	values    []*Item // slice of values in the BTree
}

// newArtIterator creates a new iterator for the AdaptiveRadixTree.
func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			Key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind resets the iterator to the beginning of the BTree.
func (artIter *artIterator) Rewind() {
	artIter.currIndex = 0
}

// Seek moves the iterator to the position of the first key greater than or equal to the given key.
func (artIter *artIterator) Seek(key []byte) {
	if artIter.reverse {
		artIter.currIndex = sort.Search(len(artIter.values), func(i int) bool {
			return bytes.Compare(artIter.values[i].Key, key) <= 0
		})
	} else {
		artIter.currIndex = sort.Search(len(artIter.values), func(i int) bool {
			return bytes.Compare(artIter.values[i].Key, key) >= 0
		})
	}
}

// Next moves the iterator to the next position.
func (artIter *artIterator) Next() {
	artIter.currIndex += 1
}

// Valid returns whether the iterator is currently pointing to a valid position.
func (artIter *artIterator) Valid() bool {
	return artIter.currIndex < len(artIter.values)
}

// Key returns the current key.
func (artIter *artIterator) Key() []byte {
	return artIter.values[artIter.currIndex].Key
}

// Value returns the current value.
func (artIter *artIterator) Value() *data.LogRecordPos {
	return artIter.values[artIter.currIndex].pos
}

// Close releases any resources associated with the iterator.
func (artIter *artIterator) Close() {
	artIter.values = nil
}
