package go_kv

type Options struct {
	DirPath      string // directory path to store the data
	DataFileSize int64  // size of each data file in bytes
	SyncWrites   bool   // whether to sync writes to disk or not
}
