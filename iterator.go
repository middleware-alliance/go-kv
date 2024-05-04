package go_kv

import (
	"bytes"
	"go-kv/index"
)

// Iterator represents an iterator over a KV store.
type Iterator struct {
	indexIter index.Iterator // iterator over the index
	db        *DB
	options   IteratorOptions
}

// NewIterator creates a new iterator over the KV store.
func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(options.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   options,
	}
}

// Rewind resets the iterator to the beginning of the index.
func (i *Iterator) Rewind() {
	i.indexIter.Rewind()
	i.skipToNext()
}

// Seek moves the iterator to the position just after the given key.
func (i *Iterator) Seek(key []byte) {
	i.indexIter.Seek(key)
	i.skipToNext()
}

// Next moves the iterator to the next position.
func (i *Iterator) Next() {
	i.indexIter.Next()
	i.skipToNext()
}

// Valid returns true if the iterator is pointing to a valid position.
func (i *Iterator) Valid() bool {
	return i.indexIter.Valid()
}

// Key returns the current key.
func (i *Iterator) Key() []byte {
	return i.indexIter.Key()
}

// Value returns the current value.
func (i *Iterator) Value() ([]byte, error) {
	logRecordPos := i.indexIter.Value()
	i.db.mut.RLock()
	defer i.db.mut.RUnlock()
	return i.db.getValueByPosition(logRecordPos)
}

// Close releases the resources used by the iterator.
func (i *Iterator) Close() {
	i.indexIter.Close()
}

func (i *Iterator) skipToNext() {
	prefixLen := len(i.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; i.indexIter.Valid(); i.indexIter.Next() {
		key := i.indexIter.Key()
		//if prefixLen <= len(key) && bytes.Compare(i.options.Prefix, key[:prefixLen]) == 0 {
		if bytes.Equal(i.options.Prefix, key[:prefixLen]) {
			break
		}
	}
}
