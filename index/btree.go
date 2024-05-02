package index

import (
	"github.com/google/btree"
	"go-kv/data"
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
func (B BTree) Put(key []byte, pos *data.LogRecordPos) bool {
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
func (B BTree) Get(key []byte) *data.LogRecordPos {
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
func (B BTree) Delete(key []byte) bool {
	it := &Item{
		Key: key,
	}
	B.lock.Lock()
	defer B.lock.Unlock()
	item := B.tree.Delete(it)
	return item != nil
}
