package index

import (
	"bytes"
	"github.com/google/btree"
	"go-kv/data"
)

// 抽象索引器接口，后续可以扩展为支持多种索引器

// Indexer is the interface for indexing data in the log.
type Indexer interface {
	// Put inserts a key-value pair into the index.
	// If the key already exists, it returns false and the value is not updated.
	// Otherwise, it returns true and the value is updated.
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get retrieves the value of a key from the index.
	// If the key does not exist, it returns nil.
	Get(key []byte) *data.LogRecordPos
	// Delete removes a key-value pair from the index.
	// If the key does not exist, it returns false.
	Delete(key []byte) bool
	// Size returns the number of key-value pairs in the index.
	Size() int
	// Iterator creates a new iterator for the index.
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// Btree is a B-tree based index.
	Btree IndexType = iota + 1
	// ART is an Adaptive Radix Tree based index.
	ART
)

// NewIndexer creates a new Indexer.
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		//todo: implement ART index
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	Key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.Key, bi.(*Item).Key) < 0
}

// Iterator is the interface for iterating over the index.
type Iterator interface {
	// Rewind resets the iterator to the beginning of the index.
	Rewind()

	// Seek moves the iterator to the position of the first key greater than or equal to the given key.
	Seek(key []byte)

	// Next moves the iterator to the next position.
	Next()

	// Valid returns whether the iterator is valid.
	Valid() bool

	// Key returns the current key.
	Key() []byte

	// Value returns the current value.
	Value() *data.LogRecordPos

	// Close closes the iterator.
	Close()
}
