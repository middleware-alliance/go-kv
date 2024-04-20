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
}

type Item struct {
	Key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.Key, bi.(*Item).Key) < 0
}
