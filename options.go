package go_kv

import "os"

type Options struct {
	DirPath      string    // directory path to store the data
	DataFileSize int64     // size of each data file in bytes
	SyncWrites   bool      // whether to sync writes to disk or not
	IndexType    IndexType // type of index to use for lookups
}

type IndexType = int8

const (
	// Btree is a B-tree based index.
	Btree IndexType = iota + 1
	// ART is an Adaptive Radix Tree based index.
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    Btree,
}
