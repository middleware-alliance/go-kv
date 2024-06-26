package go_kv

import "errors"

var (
	ErrKeyNotFound            = errors.New("key not found")
	ErrKeyExists              = errors.New("key already exists")
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("index update failed")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("data directory is corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed max batch number")
	ErrMergeIsProgress        = errors.New("merge is in progress, try again later")
)
