package go_kv

import "os"

type Options struct {
	DirPath      string    // directory path to store the data
	DataFileSize int64     // size of each data file in bytes
	SyncWrites   bool      // whether to sync writes to disk or not
	IndexType    IndexType // type of index to use for lookups
}

// IteratorOptions is a struct for options to be used while iterating over the data.
type IteratorOptions struct {
	Prefix  []byte // prefix to filter keys by, default is nil to return all keys
	Reverse bool   // whether to iterate in reverse order or not, default is false
}

// WriteBatchOptions is a struct for options to be used while writing a batch of data.
type WriteBatchOptions struct {
	MaxBatchNum uint // maximum number of writes to buffer before flushing to disk, default is 1000
	SyncWrites  bool // whether to sync writes to disk or not, default is false
}

type IndexType = int8

const (
	// Btree is a B-tree based index.
	Btree IndexType = iota + 1
	// ART is an Adaptive Radix Tree based index.
	ART
	// BPlusTree is a B+tree based index.
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BPlusTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 1000,
	SyncWrites:  false,
}
