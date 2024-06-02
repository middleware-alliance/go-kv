package index

import (
	"go-kv/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree is a B+ tree implementation.
// It is used to store and retrieve data in a key-value store.
// https://github.com/etcd-io/bbolt
type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree creates a new B+ tree.
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = syncWrites
	// open b+ tree index
	tree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open b+ tree index")
	}

	// create index bucket if it doesn't exist
	if err = tree.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create b+ tree index bucket")
	}

	return &BPlusTree{tree: tree}
}

// Put inserts a key-value pair into the index.
// If the key already exists, it returns false and the value is not updated.
// Otherwise, it returns true and the value is updated.
func (B *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := B.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in b+ tree index")
	}
	return true
}

// Get retrieves the value of a key from the index.
// If the key does not exist, it returns nil.
func (B *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := B.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if v := bucket.Get(key); v != nil && len(v) != 0 {
			pos = data.DecodeLogRecordPos(v)
		}
		return nil
	}); err != nil {
		panic("failed to get value from b+ tree index")
	}

	return pos
}

// Delete removes a key-value pair from the index.
// If the key does not exist, it returns false.
func (B *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := B.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); value != nil && len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value from b+ tree index")
	}

	return ok
}

// Size returns the number of key-value pairs in the index.
func (B *BPlusTree) Size() int {
	var size int
	if err := B.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size of b+ tree index")
	}

	return size
}

// Iterator creates a new iterator for the index.
func (B *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(B.tree, reverse)
}

// Close releases any resources associated with the BTree.
func (B *BPlusTree) Close() error {
	return B.tree.Close()
}

// bptreeIterator is an iterator for the B+ tree index.
type bptreeIterator struct {
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor
	reverse bool
	currKey []byte
	currVal []byte
}

// newBptreeIterator creates a new iterator for the B+ tree index.
func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to create b+ tree iterator")
	}
	b := &bptreeIterator{tx: tx, cursor: tx.Bucket(indexBucketName).Cursor(), reverse: reverse}
	b.Rewind()
	return b
}

func (b *bptreeIterator) Rewind() {
	if b.reverse {
		b.currKey, b.currVal = b.cursor.Last()
	} else {
		b.currKey, b.currVal = b.cursor.First()
	}
}

func (b *bptreeIterator) Seek(key []byte) {
	b.currKey, b.currVal = b.cursor.Seek(key)
}

func (b *bptreeIterator) Next() {
	if b.reverse {
		b.currKey, b.currVal = b.cursor.Prev()
	} else {
		b.currKey, b.currVal = b.cursor.Next()
	}
}

func (b *bptreeIterator) Valid() bool {
	return b.currKey != nil && len(b.currKey) != 0
}

func (b *bptreeIterator) Key() []byte {
	return b.currKey
}

func (b *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(b.currVal)
}

func (b *bptreeIterator) Close() {
	_ = b.tx.Rollback()
}
