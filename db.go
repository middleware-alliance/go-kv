package go_kv

import (
	"go-kv/data"
	"go-kv/index"
	"sync"
)

// DB represents a key-value store.
type DB struct {
	options    Options
	mut        *sync.RWMutex
	activeFile *data.DataFile            // active data file
	olderFiles map[uint32]*data.DataFile // older data files, only read
	index      index.Indexer             // memory index
}

// Put inserts a key-value pair into the database.
// It returns an error if the key is empty.
// It returns an error if the index update failed.
// It returns an error if there is an error writing to disk.
func (db *DB) Put(key, value []byte) error {
	// key and value validation
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// new log record
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// append log record to active data file
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// update memory index
	if ok := db.index.Put(key, pos); ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mut.Lock()
	defer db.mut.Unlock()

	// if active file is full, create a new one
	// if active file is not full, append log record to active file
	if db.activeFile == nil {
		// create new data file
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	// encode log record
	encRecord, size := data.EncodeLogRecord(logRecord)
	// if active file is full, create a new one
	// if active file is not full, append log record to active file
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// persistence logic, sync current memory buffer to disk
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// current active file becomes older file
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// create new data file
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	// write log record to active file
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// if you need immediately flush to disk, call Sync()
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// construct log record position with memory index, return it
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

// setActiveFile sets the active data file to the latest one.
// It returns an error if there is no data file to set as active.
// Access this method needs db.mut is required.
func (db *DB) setActiveFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// open new data dataFile
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

// Get retrieves the value of a key from the database.
// It returns an error if the key is empty.
// It returns an error if the index lookup failed.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mut.RLock()
	defer db.mut.RUnlock()

	// key validation
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// lookup log record position from memory index
	logRecordPos := db.index.Get(key)
	// if not found key, return error
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// lookup log record from data file identified by logRecordPos
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	// if data file not found, return error
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// read log record from data file offset by logRecordPos
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// log record validation, if log record is deleted, return error
	if logRecord.Type == data.LogRecordTypeDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}
